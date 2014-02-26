/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.pregelix.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.pregelix.dataflow.std.group.ClusteredGroupWriter;
import edu.uci.ics.pregelix.dataflow.std.group.IClusteredAggregatorDescriptorFactory;

/**
 * @author pouria This class defines the logic for merging the run, generated
 *         during the first phase of external sort (for both sorting without
 *         replacement selection and with it). For the case with replacement
 *         selection, this code also takes the limit on the output into account
 *         (if specified). If number of input runs is less than the available
 *         memory frames, then merging can be done in one pass, by allocating
 *         one buffer per run, and one buffer as the output buffer. A
 *         priorityQueue is used to find the top tuple at each iteration, among
 *         all the runs' heads in memory (check RunMergingFrameReader for more
 *         details). Otherwise, assuming that we have R runs and M memory
 *         buffers, where (R > M), we first merge first (M-1) runs and create a
 *         new sorted run, out of them. Discarding the first (M-1) runs, now
 *         merging procedure gets applied recursively on the (R-M+2) remaining
 *         runs using the M memory buffers. For the case of replacement
 *         selection, if outputLimit is specified, once the final pass is done
 *         on the runs (which is the pass that generates the final sorted
 *         output), as soon as the output size hits the output limit, the
 *         process stops, closes, and returns.
 */

public class ExternalSortRunMerger {

    private final IHyracksTaskContext ctx;
    private final List<IFrameReader> runs;
    private final int[] sortFields;
    private final RecordDescriptor inRecordDesc;
    private final RecordDescriptor outRecordDesc;
    private final int framesLimit;
    private final IFrameWriter writer;
    private List<ByteBuffer> inFrames;
    private ByteBuffer outFrame;
    private FrameTupleAppender outFrameAppender;

    private IFrameSorter frameSorter; // Used in External sort, no replacement
                                      // selection

    private int[] groupFields;
    private IBinaryComparator[] comparators;
    private IClusteredAggregatorDescriptorFactory aggregatorFactory;
    private IClusteredAggregatorDescriptorFactory partialAggregatorFactory;

    // Constructor for external sort, no replacement selection
    public ExternalSortRunMerger(IHyracksTaskContext ctx, IFrameSorter frameSorter, List<IFrameReader> runs,
            int[] sortFields, RecordDescriptor inRecordDesc, RecordDescriptor outRecordDesc, int framesLimit,
            IFrameWriter writer, int[] groupFields, IBinaryComparator[] comparators,
            IClusteredAggregatorDescriptorFactory aggregatorFactory,
            IClusteredAggregatorDescriptorFactory partialAggregatorFactory) {
        this.ctx = ctx;
        this.frameSorter = frameSorter;
        this.runs = new LinkedList<IFrameReader>(runs);
        this.sortFields = sortFields;
        this.inRecordDesc = inRecordDesc;
        this.outRecordDesc = outRecordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;

        this.groupFields = groupFields;
        this.comparators = comparators;
        this.aggregatorFactory = aggregatorFactory;
        this.partialAggregatorFactory = partialAggregatorFactory;
    }

    public void process() throws HyracksDataException {
        ClusteredGroupWriter pgw = new ClusteredGroupWriter(ctx, groupFields, comparators, aggregatorFactory,
                inRecordDesc, outRecordDesc, writer);
        try {
            if (runs.size() <= 0) {
                pgw.open();
                if (frameSorter != null && frameSorter.getFrameCount() > 0) {
                    frameSorter.flushFrames(pgw);
                }
                /** recycle sort buffer */
                frameSorter.close();
            } else {
                /** recycle sort buffer */
                frameSorter.close();

                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.allocateFrame();
                outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
                outFrameAppender.reset(outFrame, true);
                for (int i = 0; i < framesLimit - 1; ++i) {
                    inFrames.add(ctx.allocateFrame());
                }
                int maxMergeWidth = framesLimit - 1;
                while (runs.size() > maxMergeWidth) {
                    int generationSeparator = 0;
                    while (generationSeparator < runs.size() && runs.size() > maxMergeWidth) {
                        int mergeWidth = Math.min(Math.min(runs.size() - generationSeparator, maxMergeWidth),
                                runs.size() - maxMergeWidth + 1);
                        FileReference newRun = ctx.createManagedWorkspaceFile(ExternalSortRunMerger.class
                                .getSimpleName());
                        IFrameWriter mergeResultWriter = new RunFileWriter(newRun, ctx.getIOManager());
                        pgw = new ClusteredGroupWriter(ctx, groupFields, comparators, partialAggregatorFactory,
                                inRecordDesc, inRecordDesc, mergeResultWriter);
                        pgw.open();
                        IFrameReader[] runCursors = new RunFileReader[mergeWidth];
                        for (int i = 0; i < mergeWidth; i++) {
                            runCursors[i] = runs.get(generationSeparator + i);
                        }
                        merge(pgw, runCursors);
                        runs.subList(generationSeparator, mergeWidth + generationSeparator).clear();
                        runs.add(generationSeparator++, ((RunFileWriter) mergeResultWriter).createReader());
                    }
                }
                if (!runs.isEmpty()) {
                    pgw = new ClusteredGroupWriter(ctx, groupFields, comparators, partialAggregatorFactory,
                            inRecordDesc, inRecordDesc, writer);
                    pgw.open();
                    IFrameReader[] runCursors = new RunFileReader[runs.size()];
                    for (int i = 0; i < runCursors.length; i++) {
                        runCursors[i] = runs.get(i);
                    }
                    merge(pgw, runCursors);
                }
            }
        } catch (Exception e) {
            pgw.fail();
            throw new HyracksDataException(e);
        } finally {
            pgw.close();
        }
    }

    private void merge(IFrameWriter mergeResultWriter, IFrameReader[] runCursors) throws HyracksDataException {
        RunMergingFrameReader merger = new RunMergingFrameReader(ctx, runCursors, inFrames, sortFields, inRecordDesc);
        merger.open();
        try {
            while (merger.nextFrame(outFrame)) {
                FrameUtils.flushFrame(outFrame, mergeResultWriter);
            }
        } finally {
            merger.close();
        }
    }
}
