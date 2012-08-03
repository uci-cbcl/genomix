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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class ExternalSortRunMergerWithLoadOpt {
    private static final Logger LOGGER = Logger.getLogger(ExternalSortRunMergerWithLoadOpt.class.getName());

    private final int[] keyFields;
    private final IBinaryComparator[] comparators;

    private final int framesLimit;

    private final RecordDescriptor inRecDesc;

    private final IHyracksTaskContext ctx;

    private final IFrameWriter outputWriter;

    private final boolean isLoadOptimized;

    List<ByteBuffer> inFrames;
    ByteBuffer outFrame, writerFrame;
    FrameTupleAppender outAppender, writerAppender;
    List<IFrameReader> runs;
    ArrayTupleBuilder finalTupleBuilder;
    FrameTupleAccessor outFrameAccessor;
    int[] currentFrameIndexInRun, currentRunFrames, currentBucketInRun;
    int runFrameLimit = 1;

    // For instrumenting
    int mergeLevels = 0;
    long comparisonCount = 0;
    long flushingTime = 0;
    long readingTime = 0;
    long queuePopTime = 0;

    public ExternalSortRunMergerWithLoadOpt(IHyracksTaskContext ctx, int[] keyFields, int framesLimit,
            IBinaryComparator[] comparators, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.framesLimit = framesLimit;

        this.keyFields = keyFields;
        this.comparators = comparators;

        this.inRecDesc = inRecDesc;

        this.outAppender = new FrameTupleAppender(ctx.getFrameSize());

        this.outputWriter = outputWriter;

        this.outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);

        this.isLoadOptimized = true;
    }

    public ExternalSortRunMergerWithLoadOpt(IHyracksTaskContext ctx, int[] keyFields, int framesLimit,
            IBinaryComparator[] comparators, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            IFrameWriter outputWriter, boolean loadOptimized) throws HyracksDataException {
        this.ctx = ctx;
        this.framesLimit = framesLimit;

        this.keyFields = keyFields;
        this.comparators = comparators;

        this.inRecDesc = inRecDesc;

        this.outAppender = new FrameTupleAppender(ctx.getFrameSize());

        this.outputWriter = outputWriter;

        this.outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);

        this.isLoadOptimized = loadOptimized;
    }

    public void initialize(List<IFrameReader> runFiles) throws HyracksDataException {
        // FIXME
        long mergeTimer = System.currentTimeMillis();

        runs = runFiles;

        try {
            if (runs.size() <= 0) {
                return;
            } else {
                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.allocateFrame();
                outAppender.reset(outFrame, true);
                outFrameAccessor.reset(outFrame);
                int runProcOffset = 0;
                while (runs.size() > 0) {
                    try {
                        doPass(runs, runProcOffset);
                        if (runs.size() + 2 <= framesLimit) {
                            // final phase
                            runProcOffset = 0;
                        } else {
                            // one more merge level
                            runProcOffset++;
                        }
                        mergeLevels++;
                    } catch (Exception e) {
                        throw new HyracksDataException(e);
                    }
                }
                inFrames.clear();
            }
        } catch (Exception e) {
            outputWriter.fail();
            throw new HyracksDataException(e);
        } finally {
            outputWriter.close();
            LOGGER.warning("ExternalSortRunMergerWithLoadOpt\t" + (System.currentTimeMillis() - mergeTimer) + "\t"
                    + mergeLevels + "\t" + comparisonCount + "\t" + queuePopTime + "\t" + flushingTime + "\t"
                    + readingTime);
        }
    }

    private void doPass(List<IFrameReader> runs, int offset) throws HyracksDataException {
        FileReference newRun = null;
        IFrameWriter writer = outputWriter;
        boolean finalPass = false;

        int runNumber = runs.size() - offset;

        while (inFrames.size() + 2 < framesLimit) {
            inFrames.add(ctx.allocateFrame());
        }

        if (runs.size() + 2 <= framesLimit) {
            finalPass = true;
            if (isLoadOptimized)
                runFrameLimit = (framesLimit - 2) / runNumber;
            else
                runFrameLimit = 1;
        } else {
            runFrameLimit = 1;
            runNumber = framesLimit - 2;
            newRun = ctx.getJobletContext().createManagedWorkspaceFile(
                    ExternalSortRunMergerWithLoadOpt.class.getSimpleName());
            writer = new RunFileWriter(newRun, ctx.getIOManager());
            writer.open();
        }
        try {
            currentFrameIndexInRun = new int[runNumber];
            currentRunFrames = new int[runNumber];
            /**
             * Create file readers for each input run file, only for
             * the ones fit into the inFrames
             */
            IFrameReader[] runFileReaders = new RunFileReader[runNumber];
            FrameTupleAccessor[] tupleAccessors = new FrameTupleAccessor[inFrames.size()];
            Comparator<ReferenceEntry> comparator = createEntryComparator(comparators);
            ReferencedPriorityQueue topTuples = new ReferencedPriorityQueue(ctx.getFrameSize(), inRecDesc, runNumber,
                    comparator);
            /**
             * current tuple index in each run
             */
            int[] tupleIndices = new int[runNumber];

            for (int i = 0; i < runNumber; i++) {
                int runIndex = topTuples.peek().getRunid();
                tupleIndices[runIndex] = 0;
                // Load the run file
                runFileReaders[runIndex] = runs.get(runIndex);
                runFileReaders[runIndex].open();

                currentRunFrames[runIndex] = 0;
                currentFrameIndexInRun[runIndex] = runIndex * runFrameLimit;
                for (int j = 0; j < runFrameLimit; j++) {
                    int frameIndex = currentFrameIndexInRun[runIndex] + j;

                    long timer = System.currentTimeMillis();
                    boolean hasNextFrame = runFileReaders[runIndex].nextFrame(inFrames.get(frameIndex));
                    readingTime += System.currentTimeMillis() - timer;

                    if (hasNextFrame) {
                        tupleAccessors[frameIndex] = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
                        tupleAccessors[frameIndex].reset(inFrames.get(frameIndex));
                        currentRunFrames[runIndex]++;
                        if (j == 0) {
                            setNextTopTuple(runIndex, tupleIndices, runFileReaders, tupleAccessors, topTuples);
                        }
                    } else {
                        break;
                    }
                }
            }

            /**
             * Start merging
             */
            while (!topTuples.areRunsExhausted()) {
                /**
                 * Get the top record
                 */
                ReferenceEntry top = topTuples.peek();
                int tupleIndex = top.getTupleIndex();
                int runIndex = topTuples.peek().getRunid();

                FrameTupleAccessor fta = top.getAccessor();

                if (!outAppender.append(fta, tupleIndex)) {
                    long timer = System.currentTimeMillis();
                    FrameUtils.flushFrame(outFrame, writer);
                    if (!finalPass)
                        flushingTime += System.currentTimeMillis() - timer;
                    outAppender.reset(outFrame, true);
                    if (!outAppender.append(fta, tupleIndex)) {
                        throw new HyracksDataException("Failed to flush a record");
                    }
                }

                tupleIndices[runIndex]++;
                setNextTopTuple(runIndex, tupleIndices, runFileReaders, tupleAccessors, topTuples);
            }

            if (outAppender.getTupleCount() > 0) {
                long timer = System.currentTimeMillis();
                FrameUtils.flushFrame(outFrame, writer);
                if (!finalPass)
                    flushingTime += System.currentTimeMillis() - timer;
                outAppender.reset(outFrame, true);
            }

            runs.subList(0, runNumber).clear();
            /**
             * insert the new run file into the beginning of the run
             * file list
             */
            if (!finalPass) {
                runs.add(0, ((RunFileWriter) writer).createReader());
            }
        } finally {
            if (!finalPass) {
                writer.close();
            }
        }
    }

    private void setNextTopTuple(int runIndex, int[] tupleIndices, IFrameReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples) throws HyracksDataException {
        long popTimer = System.currentTimeMillis();

        int runStart = runIndex * runFrameLimit;
        boolean existNext = false;
        if (tupleAccessors[currentFrameIndexInRun[runIndex]] == null || runCursors[runIndex] == null) {
            /**
             * run already closed
             */
            existNext = false;
        } else if (currentFrameIndexInRun[runIndex] - runStart < currentRunFrames[runIndex] - 1) {
            /**
             * not the last frame for this run
             */
            existNext = true;
            if (tupleIndices[runIndex] >= tupleAccessors[currentFrameIndexInRun[runIndex]].getTupleCount()) {
                tupleIndices[runIndex] = 0;
                currentFrameIndexInRun[runIndex]++;
            }
        } else if (tupleIndices[runIndex] < tupleAccessors[currentFrameIndexInRun[runIndex]].getTupleCount()) {
            /**
             * the last frame has expired
             */
            existNext = true;
        } else {
            /**
             * If all tuples in the targeting frame have been
             * checked.
             */
            tupleIndices[runIndex] = 0;
            currentFrameIndexInRun[runIndex] = runStart;
            /**
             * read in batch
             */
            currentRunFrames[runIndex] = 0;
            for (int j = 0; j < runFrameLimit; j++) {
                int frameIndex = currentFrameIndexInRun[runIndex] + j;

                long timer = System.currentTimeMillis();
                boolean hasNextFrame = runCursors[runIndex].nextFrame(inFrames.get(frameIndex));
                readingTime += System.currentTimeMillis() - timer;

                if (hasNextFrame) {
                    tupleAccessors[frameIndex].reset(inFrames.get(frameIndex));
                    existNext = true;
                    currentRunFrames[runIndex]++;
                } else {
                    break;
                }
            }
        }

        if (existNext) {
            topTuples.popAndReplace(tupleAccessors[currentFrameIndexInRun[runIndex]], tupleIndices[runIndex]);
        } else {
            topTuples.pop();
            closeRun(runIndex, runCursors, tupleAccessors);
        }

        queuePopTime += System.currentTimeMillis() - popTimer;
    }

    /**
     * Close the run file, and also the corresponding readers and
     * input frame.
     * 
     * @param index
     * @param runCursors
     * @param tupleAccessor
     * @throws HyracksDataException
     */
    private void closeRun(int index, IFrameReader[] runCursors, IFrameTupleAccessor[] tupleAccessor)
            throws HyracksDataException {
        if (runCursors[index] != null) {
            runCursors[index].close();
            runCursors[index] = null;
            int frameOffset = index * runFrameLimit;
            for (int j = 0; j < runFrameLimit; j++) {
                tupleAccessor[frameOffset + j] = null;
            }
        }
    }

    private Comparator<ReferenceEntry> createEntryComparator(final IBinaryComparator[] comparators) {
        return new Comparator<ReferenceEntry>() {

            @Override
            public int compare(ReferenceEntry o1, ReferenceEntry o2) {
                // FIXME
                comparisonCount++;
                FrameTupleAccessor fta1 = (FrameTupleAccessor) o1.getAccessor();
                FrameTupleAccessor fta2 = (FrameTupleAccessor) o2.getAccessor();
                int j1 = o1.getTupleIndex();
                int j2 = o2.getTupleIndex();
                byte[] b1 = fta1.getBuffer().array();
                byte[] b2 = fta2.getBuffer().array();
                for (int f = 0; f < keyFields.length; ++f) {
                    int fIdx = f;
                    int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength()
                            + fta1.getFieldStartOffset(j1, fIdx);
                    int l1 = fta1.getFieldEndOffset(j1, fIdx) - fta1.getFieldStartOffset(j1, fIdx);
                    int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength()
                            + fta2.getFieldStartOffset(j2, fIdx);
                    int l2 = fta2.getFieldEndOffset(j2, fIdx) - fta2.getFieldStartOffset(j2, fIdx);
                    int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }

        };
    }
}
