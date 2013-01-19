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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;

public class HybridHashSortGrouperBucketMerge {

    private final int[] keyFields;
    private final IBinaryComparator[] comparators;

    private final IAggregatorDescriptor merger;
    private final AggregateState mergeState;

    private final int framesLimit, tableSize;

    private final RecordDescriptor inRecDesc;

    private final IHyracksTaskContext ctx;

    private final ArrayTupleBuilder tupleBuilder;

    private final IFrameWriter outputWriter;

    private final ITuplePartitionComputer tpc;

    private final boolean isLoadOptimized;

    List<ByteBuffer> inFrames;
    ByteBuffer outFrame, writerFrame;
    FrameTupleAppender outAppender, writerAppender;
    LinkedList<RunFileReader> runs;
    ArrayTupleBuilder finalTupleBuilder;
    FrameTupleAccessor outFrameAccessor;
    int[] currentFrameIndexInRun, currentRunFrames, currentBucketInRun;
    int runFrameLimit = 1;

    public HybridHashSortGrouperBucketMerge(IHyracksTaskContext ctx, int[] keyFields, int framesLimit, int tableSize,
            ITuplePartitionComputer tpc, IBinaryComparator[] comparators, IAggregatorDescriptor merger,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, IFrameWriter outputWriter)
            throws HyracksDataException {
        this.ctx = ctx;
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;

        this.keyFields = keyFields;
        this.comparators = comparators;
        this.merger = merger;
        this.mergeState = merger.createAggregateStates();

        this.inRecDesc = inRecDesc;

        this.tupleBuilder = new ArrayTupleBuilder(inRecDesc.getFieldCount());

        this.outAppender = new FrameTupleAppender(ctx.getFrameSize());

        this.outputWriter = outputWriter;

        this.outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);

        this.tpc = tpc;

        this.isLoadOptimized = true;
    }

    public HybridHashSortGrouperBucketMerge(IHyracksTaskContext ctx, int[] keyFields, int framesLimit, int tableSize,
            ITuplePartitionComputer tpc, IBinaryComparator[] comparators, IAggregatorDescriptor merger,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, IFrameWriter outputWriter, boolean loadOptimized)
            throws HyracksDataException {
        this.ctx = ctx;
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;

        this.keyFields = keyFields;
        this.comparators = comparators;
        this.merger = merger;
        this.mergeState = merger.createAggregateStates();

        this.inRecDesc = inRecDesc;

        this.tupleBuilder = new ArrayTupleBuilder(inRecDesc.getFieldCount());

        this.outAppender = new FrameTupleAppender(ctx.getFrameSize());

        this.outputWriter = outputWriter;

        this.outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);

        this.tpc = tpc;

        this.isLoadOptimized = loadOptimized;
    }

    public void initialize(LinkedList<RunFileReader> runFiles) throws HyracksDataException {

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
            mergeState.close();
        }
    }

    private void doPass(LinkedList<RunFileReader> runs, int offset) throws HyracksDataException {
        FileReference newRun = null;
        IFrameWriter writer = outputWriter;
        boolean finalPass = false;

        int runNumber = runs.size() - offset;

        while (inFrames.size() + 2 < framesLimit) {
            inFrames.add(ctx.allocateFrame());
        }

        if (runNumber + 2 <= framesLimit) {
            finalPass = true;
            if (isLoadOptimized)
                runFrameLimit = (framesLimit - 2) / runNumber;
            else
                runFrameLimit = 1;
        } else {
            runFrameLimit = 1;
            runNumber = framesLimit - 2;
            newRun = ctx.getJobletContext().createManagedWorkspaceFile(
                    HybridHashSortGrouperBucketMerge.class.getSimpleName());
            writer = new RunFileWriter(newRun, ctx.getIOManager());
            writer.open();
        }
        try {
            currentFrameIndexInRun = new int[runNumber];
            currentRunFrames = new int[runNumber];
            currentBucketInRun = new int[runNumber];
            /**
             * Create file readers for each input run file, only for
             * the ones fit into the inFrames
             */
            RunFileReader[] runFileReaders = new RunFileReader[runNumber];
            FrameTupleAccessor[] tupleAccessors = new FrameTupleAccessor[inFrames.size()];
            Comparator<ReferenceHashEntry> comparator = createEntryComparator(comparators);
            ReferencedBucketBasedPriorityQueue topTuples = new ReferencedBucketBasedPriorityQueue(ctx.getFrameSize(),
                    inRecDesc, runNumber, comparator, tpc, tableSize);
            /**
             * current tuple index in each run
             */
            int[] tupleIndices = new int[runNumber];

            for (int i = 0; i < runNumber; i++) {
                int runIndex = i + offset;
                tupleIndices[i] = 0;
                // Load the run file
                runFileReaders[i] = runs.get(runIndex);
                runFileReaders[i].open();

                currentRunFrames[i] = 0;
                currentFrameIndexInRun[i] = i * runFrameLimit;
                for (int j = 0; j < runFrameLimit; j++) {
                    int frameIndex = currentFrameIndexInRun[i] + j;
                    boolean hasNextFrame = runFileReaders[runIndex].nextFrame(inFrames.get(frameIndex));
                    if (hasNextFrame) {
                        tupleAccessors[frameIndex] = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
                        tupleAccessors[frameIndex].reset(inFrames.get(frameIndex));
                        currentRunFrames[i]++;
                        if (j == 0) {
                            currentBucketInRun[i] = tpc.partition(tupleAccessors[frameIndex], tupleIndices[i],
                                    tableSize);
                            setNextTopTuple(i, tupleIndices, runFileReaders, tupleAccessors, topTuples);
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

                int currentTupleInOutFrame = outFrameAccessor.getTupleCount() - 1;
                if (currentTupleInOutFrame < 0
                        || compareFrameTuples(fta, tupleIndex, outFrameAccessor, currentTupleInOutFrame) != 0) {

                    tupleBuilder.reset();

                    for (int k = 0; k < keyFields.length; k++) {
                        tupleBuilder.addField(fta, tupleIndex, keyFields[k]);
                    }

                    merger.init(tupleBuilder, fta, tupleIndex, mergeState);

                    if (!outAppender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(),
                            tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                        flushOutFrame(writer, finalPass);
                        if (!outAppender.appendSkipEmptyField(tupleBuilder.getFieldEndOffsets(),
                                tupleBuilder.getByteArray(), 0, tupleBuilder.getSize())) {
                            throw new HyracksDataException(
                                    "The partial result is too large to be initialized in a frame.");
                        }
                    }

                } else {
                    /**
                     * if new tuple is in the same group of the
                     * current aggregator do merge and output to the
                     * outFrame
                     */

                    merger.aggregate(fta, tupleIndex, outFrameAccessor, currentTupleInOutFrame, mergeState);

                }
                tupleIndices[runIndex]++;
                setNextTopTuple(runIndex, tupleIndices, runFileReaders, tupleAccessors, topTuples);
            }

            if (outAppender.getTupleCount() > 0) {
                flushOutFrame(writer, finalPass);
                outAppender.reset(outFrame, true);
            }

            merger.close();

            runs.subList(offset, runNumber).clear();
            /**
             * insert the new run file into the beginning of the run
             * file list
             */
            if (!finalPass) {
                runs.add(offset, ((RunFileWriter) writer).createReader());
            }
        } finally {
            if (!finalPass) {
                writer.close();
            }
            mergeState.reset();
        }
    }

    private void flushOutFrame(IFrameWriter writer, boolean isFinal) throws HyracksDataException {

        if (finalTupleBuilder == null) {
            finalTupleBuilder = new ArrayTupleBuilder(inRecDesc.getFields().length);
        }

        if (writerFrame == null) {
            writerFrame = ctx.allocateFrame();
        }

        if (writerAppender == null) {
            writerAppender = new FrameTupleAppender(ctx.getFrameSize());
        }
        writerAppender.reset(writerFrame, true);

        outFrameAccessor.reset(outFrame);

        for (int i = 0; i < outFrameAccessor.getTupleCount(); i++) {

            finalTupleBuilder.reset();

            for (int k = 0; k < keyFields.length; k++) {
                finalTupleBuilder.addField(outFrameAccessor, i, keyFields[k]);
            }

            if (isFinal) {

                merger.outputFinalResult(finalTupleBuilder, outFrameAccessor, i, mergeState);

            } else {

                merger.outputPartialResult(finalTupleBuilder, outFrameAccessor, i, mergeState);
            }

            if (!writerAppender.appendSkipEmptyField(finalTupleBuilder.getFieldEndOffsets(),
                    finalTupleBuilder.getByteArray(), 0, finalTupleBuilder.getSize())) {
                FrameUtils.flushFrame(writerFrame, writer);
                writerAppender.reset(writerFrame, true);
                if (!writerAppender.appendSkipEmptyField(finalTupleBuilder.getFieldEndOffsets(),
                        finalTupleBuilder.getByteArray(), 0, finalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Aggregation output is too large to be fit into a frame.");
                }
            }
        }
        if (writerAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(writerFrame, writer);
            writerAppender.reset(writerFrame, true);
        }

        outAppender.reset(outFrame, true);
    }

    private void setNextTopTuple(int runIndex, int[] tupleIndices, RunFileReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors, ReferencedBucketBasedPriorityQueue topTuples)
            throws HyracksDataException {
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
                if (runCursors[runIndex].nextFrame(inFrames.get(frameIndex))) {
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
    private void closeRun(int index, RunFileReader[] runCursors, IFrameTupleAccessor[] tupleAccessor)
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

    private int compareFrameTuples(IFrameTupleAccessor fta1, int j1, IFrameTupleAccessor fta2, int j2) {
        byte[] b1 = fta1.getBuffer().array();
        byte[] b2 = fta2.getBuffer().array();
        for (int f = 0; f < keyFields.length; ++f) {
            int fIdx = f;
            int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength() + fta1.getFieldStartOffset(j1, fIdx);
            int l1 = fta1.getFieldLength(j1, fIdx);
            int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength() + fta2.getFieldStartOffset(j2, fIdx);
            int l2_start = fta2.getFieldStartOffset(j2, fIdx);
            int l2_end = fta2.getFieldEndOffset(j2, fIdx);
            int l2 = l2_end - l2_start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private Comparator<ReferenceHashEntry> createEntryComparator(final IBinaryComparator[] comparators) {
        return new Comparator<ReferenceHashEntry>() {

            @Override
            public int compare(ReferenceHashEntry o1, ReferenceHashEntry o2) {
                int cmp = o1.getHashValue() - o2.getHashValue();
                if (cmp != 0) {
                    return cmp;
                } else {
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
            }

        };
    }
}
