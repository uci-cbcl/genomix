/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.hashsort;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class HybridHashSortRunMerger {

    private final IHyracksTaskContext ctx;
    private final List<RunFileReader> runs;
    private final int[] keyFields;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDesc;
    private final int framesLimit;
    private final int tableSize;
    private final IFrameWriter writer;
    private final IAggregatorDescriptor grouper;
    private final ITuplePartitionComputer tpc;
    private ByteBuffer outFrame;
    private FrameTupleAppender outFrameAppender;
    private final boolean isLoadBuffered;

    public HybridHashSortRunMerger(IHyracksTaskContext ctx, LinkedList<RunFileReader> runs, int[] keyFields,
            IBinaryComparator[] comparators, RecordDescriptor recordDesc, ITuplePartitionComputer tpc,
            IAggregatorDescriptor grouper, int framesLimit, int tableSize, IFrameWriter writer, boolean isLoadBuffered) {
        this.ctx = ctx;
        this.runs = runs;
        this.keyFields = keyFields;
        this.comparators = comparators;
        this.recordDesc = recordDesc;
        this.framesLimit = framesLimit;
        this.writer = writer;
        this.isLoadBuffered = isLoadBuffered;
        this.tableSize = tableSize;
        this.tpc = tpc;
        this.grouper = grouper;
    }

    public void process() throws HyracksDataException {
        
        // FIXME
        int mergeLevels = 0, mergeRunCount = 0;
        try {

            outFrame = ctx.allocateFrame();
            outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
            outFrameAppender.reset(outFrame, true);

            int maxMergeWidth = framesLimit - 1;
            while (runs.size() > maxMergeWidth) {
                int generationSeparator = 0;
                // FIXME
                int mergeRounds = 0;
                while (generationSeparator < runs.size() && runs.size() > maxMergeWidth) {
                    int mergeWidth = Math.min(Math.min(runs.size() - generationSeparator, maxMergeWidth), runs.size()
                            - maxMergeWidth + 1);
                    FileReference newRun = null;
                    IFrameWriter mergeResultWriter = this.writer;
                    newRun = ctx.createManagedWorkspaceFile(HybridHashSortRunMerger.class.getSimpleName());
                    mergeResultWriter = new RunFileWriter(newRun, ctx.getIOManager());
                    mergeResultWriter.open();
                    IFrameReader[] runCursors = new RunFileReader[mergeWidth];
                    for (int i = 0; i < mergeWidth; i++) {
                        runCursors[i] = runs.get(generationSeparator + i);
                    }
                    merge(mergeResultWriter, runCursors, false);
                    runs.subList(generationSeparator, generationSeparator + mergeWidth).clear();
                    runs.add(generationSeparator++, ((RunFileWriter) mergeResultWriter).createReader());
                    mergeRounds++;
                }
                mergeLevels++;
                mergeRunCount += mergeRounds;
            }
            if (!runs.isEmpty()) {
                IFrameReader[] runCursors = new RunFileReader[runs.size()];
                for (int i = 0; i < runCursors.length; i++) {
                    runCursors[i] = runs.get(i);
                }
                merge(writer, runCursors, true);
            }
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {

            ctx.getCounterContext()
                    .getCounter("optional." + HybridHashSortRunMerger.class.getSimpleName() + ".merge.runs.count", true)
                    .set(mergeRunCount);

            ctx.getCounterContext()
                    .getCounter("optional." + HybridHashSortRunMerger.class.getSimpleName() + ".merge.levels", true)
                    .set(mergeLevels);
        }
    }

    private void merge(IFrameWriter mergeResultWriter, IFrameReader[] runCursors, boolean isFinal)
            throws HyracksDataException {
        // FIXME
        long methodTimer = System.nanoTime();

        IFrameReader merger = new GroupRunMergingFrameReader(ctx, runCursors, framesLimit, tableSize, keyFields, tpc,
                comparators, grouper, recordDesc, isFinal, isLoadBuffered);
        merger.open();
        try {
            while (merger.nextFrame(outFrame)) {
                FrameUtils.flushFrame(outFrame, mergeResultWriter);
            }
        } finally {
            merger.close();
        }
        ctx.getCounterContext()
                .getCounter("optional." + HybridHashSortRunMerger.class.getSimpleName() + ".merge.time", true)
                .update(System.nanoTime() - methodTimer);
    }
}
