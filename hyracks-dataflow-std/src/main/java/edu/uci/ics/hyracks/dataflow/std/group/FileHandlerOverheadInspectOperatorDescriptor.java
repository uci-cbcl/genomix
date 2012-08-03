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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class FileHandlerOverheadInspectOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int framesPerRunFile;

    private final int runsWriteInParallel;

    private final boolean isRandRead;

    private static final Logger LOGGER = Logger.getLogger(FileHandlerOverheadInspectOperatorDescriptor.class
            .getSimpleName());

    public FileHandlerOverheadInspectOperatorDescriptor(JobSpecification spec, int framesPerRunFile) {
        this(spec, framesPerRunFile, false, false);
    }

    public FileHandlerOverheadInspectOperatorDescriptor(JobSpecification spec, int framesPerRunFile,
            boolean isRandRead, boolean isRandWrite) {
        super(spec, 1, 0);
        this.framesPerRunFile = framesPerRunFile;

        this.isRandRead = isRandRead;
        this.runsWriteInParallel = isRandWrite ? 2 : 1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.dataflow.IActivity#createPushRuntime(edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int, int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        return new AbstractUnaryInputSinkOperatorNodePushable() {

            List<RunFileReader> runs;
            RunFileWriter[] runWriters;
            int[] framesCount;
            int workingRunIndex;

            List<ByteBuffer> frames;

            long timer;

            @Override
            public void open() throws HyracksDataException {
                runs = new ArrayList<RunFileReader>();
                framesCount = new int[runsWriteInParallel];
                for (int i = 0; i < runsWriteInParallel; i++) {
                    framesCount[i] = 0;
                }
                runWriters = new RunFileWriter[runsWriteInParallel];
                workingRunIndex = 0;

                frames = new ArrayList<ByteBuffer>();
                for (int i = 0; i < framesPerRunFile; i++) {
                    frames.add(ctx.allocateFrame());
                }

                timer = System.currentTimeMillis();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                if (framesCount[workingRunIndex] % framesPerRunFile == 0) {
                    if (runWriters[workingRunIndex] != null) {
                        runWriters[workingRunIndex].close();
                        runs.add(runWriters[workingRunIndex].createReader());
                    }
                    runWriters[workingRunIndex] = new RunFileWriter(ctx.getJobletContext().createManagedWorkspaceFile(
                            FileHandlerOverheadInspectOperatorDescriptor.class.getSimpleName()), ctx.getIOManager());
                    runWriters[workingRunIndex].open();
                }
                FrameUtils.flushFrame(buffer, runWriters[workingRunIndex]);
                framesCount[workingRunIndex]++;
                workingRunIndex = (workingRunIndex + 1) % runsWriteInParallel;
            }

            @Override
            public void fail() throws HyracksDataException {

            }

            @Override
            public void close() throws HyracksDataException {
                if (runWriters[workingRunIndex] != null) {
                    runWriters[workingRunIndex].close();
                    runs.add(runWriters[workingRunIndex].createReader());
                }

                LOGGER.warning("LoadingDone\t" + (System.currentTimeMillis() - timer) + "\t"
                        + ctx.getIOManager().toString());

                workingRunIndex = 0;

                timer = System.currentTimeMillis();

                // load all run files

                for (int i = 0; i < runs.size(); i++) {
                    runs.get(i).open();
                }

                while (!runs.isEmpty()) {
                    if (!runs.get(workingRunIndex).nextFrame(frames.get(0))) {
                        runs.remove(workingRunIndex);
                        if (runs.isEmpty()) {
                            break;
                        }
                        workingRunIndex = workingRunIndex % runs.size();
                    } else {
                        if (isRandRead)
                            workingRunIndex = (workingRunIndex + 1) % runs.size();
                    }
                }

                frames = null;

                LOGGER.warning("ReadingDone\t" + (System.currentTimeMillis() - timer) + "\t"
                        + ctx.getIOManager().toString());
            }
        };
    }
}
