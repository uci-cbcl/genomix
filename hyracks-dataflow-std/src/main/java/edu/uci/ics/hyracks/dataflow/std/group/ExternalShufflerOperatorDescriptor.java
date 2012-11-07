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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class ExternalShufflerOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private final int framesLimit;

    public ExternalShufflerOperatorDescriptor(JobSpecification spec, int framesLimit, RecordDescriptor outRecDesc) {
        super(spec, 1, 1);

        this.framesLimit = framesLimit;

        recordDescriptors[0] = outRecDesc;
    }

    private static final long serialVersionUID = 1L;

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private FrameShuffler frameShuffler;

            private LinkedList<RunFileReader> runReaders;

            private final String LOCAL_RUN_PREFIX = ExternalShufflerOperatorDescriptor.class.getSimpleName() + "_LOCAL";
            private final String GLOBAL_RUN_PREFIX = ExternalShufflerOperatorDescriptor.class.getSimpleName()
                    + "_GLOBAL";

            @Override
            public void open() throws HyracksDataException {
                frameShuffler = new FrameShuffler(ctx, recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0));
                runReaders = new LinkedList<RunFileReader>();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                // do local shuffle on each full memory
                if (frameShuffler.getFrameCount() >= framesLimit - 1) {
                    // frame shuffler is full; needs to be flushed
                    RunFileWriter runWriter = new RunFileWriter(ctx.createManagedWorkspaceFile(LOCAL_RUN_PREFIX),
                            ctx.getIOManager());
                    runWriter.open();
                    frameShuffler.shufferFrames();
                    frameShuffler.flushFrames(runWriter);
                    runReaders.add(runWriter.createReader());
                    runWriter.close();
                    frameShuffler.reset();
                }
                frameShuffler.insertFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                writer.open();
                if (frameShuffler.getFrameCount() > 0) {
                    frameShuffler.shufferFrames();
                }
                if (runReaders.size() <= 0) {
                    // no runs are generated
                    frameShuffler.flushFrames(writer);
                } else {
                    RunFileWriter runWriter = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(ExternalShufflerOperatorDescriptor.class.getSimpleName()),
                            ctx.getIOManager());
                    runWriter.open();
                    frameShuffler.flushFrames(runWriter);
                    runReaders.add(runWriter.createReader());
                    runWriter.close();

                    // do global shuffling
                    // - create output buffer
                    ByteBuffer outFrame = ctx.allocateFrame();

                    int maxMergeWidth = framesLimit - 1;
                    while (runReaders.size() > maxMergeWidth) {
                        int generationSeparator = 0;
                        while (generationSeparator < runReaders.size() && runReaders.size() > maxMergeWidth) {
                            int mergeWidth = Math.min(Math.min(runReaders.size() - generationSeparator, maxMergeWidth),
                                    runReaders.size() - maxMergeWidth + 1);

                            // start merging
                            IFrameReader[] runCursors = new RunFileReader[mergeWidth];
                            for (int i = 0; i < mergeWidth; i++) {
                                runCursors[i] = runReaders.get(generationSeparator + i);
                            }
                            RunMergingShufflerFrameReader merger = new RunMergingShufflerFrameReader(ctx, runCursors,
                                    recordDesc);
                            // create a new run
                            RunFileWriter newMergeRun = new RunFileWriter(
                                    ctx.createManagedWorkspaceFile(GLOBAL_RUN_PREFIX), ctx.getIOManager());
                            newMergeRun.open();
                            merger.open();
                            while (merger.nextFrame(outFrame)) {
                                FrameUtils.flushFrame(outFrame, newMergeRun);
                            }
                            runReaders.subList(generationSeparator, generationSeparator + mergeWidth).clear();
                            runReaders.add(generationSeparator++, newMergeRun.createReader());
                            newMergeRun.close();
                            merger.close();
                        }
                    }
                    if (!runReaders.isEmpty()) {
                        // final merge
                        IFrameReader[] runCursors = new RunFileReader[runReaders.size()];
                        for (int i = 0; i < runReaders.size(); i++) {
                            runCursors[i] = runReaders.get(i);
                        }
                        RunMergingShufflerFrameReader merger = new RunMergingShufflerFrameReader(ctx, runCursors,
                                recordDesc);
                        merger.open();
                        while (merger.nextFrame(outFrame)) {
                            FrameUtils.flushFrame(outFrame, writer);
                        }
                    }
                }

                writer.close();
            }
        };
    }
}
