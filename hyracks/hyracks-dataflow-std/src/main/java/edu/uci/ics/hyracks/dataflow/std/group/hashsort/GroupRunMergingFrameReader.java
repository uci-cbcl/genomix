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
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class GroupRunMergingFrameReader implements IFrameReader {

    private static final int INT_SIZE = 4;

    private final IHyracksTaskContext ctx;
    private final IFrameReader[] runCursors;
    private final List<ByteBuffer> inFrames;
    private final int[] keyFields;
    private final int framesLimit;
    private final int tableSize;
    private final IBinaryComparator[] comparators;
    private final RecordDescriptor recordDesc;
    private final FrameTupleAppender outFrameAppender;
    private final ITuplePartitionComputer tpc;
    private ReferencedPriorityQueue topTuples;
    private int[] tupleIndexes;
    private int[] currentFrameIndexForRuns, bufferedFramesForRuns;
    private FrameTupleAccessor[] tupleAccessors;
    private int framesBuffered;

    private final IAggregatorDescriptor grouper;
    private final AggregateState groupState;

    private final boolean isLoadBuffered;

    private final boolean isFinalPhase;

    private final ArrayTupleBuilder groupTupleBuilder, outputTupleBuilder;

    private byte[] groupResultCache;
    private ByteBuffer groupResultCacheBuffer;
    private IFrameTupleAccessor groupResultCacheAccessor;
    private FrameTupleAppender groupResultCacheAppender;

    // FIXME
    long queueCompCounter = 0, mergeCompCounter = 0;

    public GroupRunMergingFrameReader(IHyracksTaskContext ctx, IFrameReader[] runCursors, int framesLimit,
            int tableSize, int[] keyFields, ITuplePartitionComputer tpc, IBinaryComparator[] comparators,
            IAggregatorDescriptor grouper, RecordDescriptor recordDesc, boolean isFinalPhase) {
        this(ctx, runCursors, framesLimit, tableSize, keyFields, tpc, comparators, grouper, recordDesc, isFinalPhase,
                false);
    }

    public GroupRunMergingFrameReader(IHyracksTaskContext ctx, IFrameReader[] runCursors, int framesLimit,
            int tableSize, int[] keyFields, ITuplePartitionComputer tpc, IBinaryComparator[] comparators,
            IAggregatorDescriptor grouper, RecordDescriptor recordDesc, boolean isFinalPhase, boolean isLoadBuffered) {
        this.ctx = ctx;
        this.runCursors = runCursors;
        this.inFrames = new ArrayList<ByteBuffer>();
        this.keyFields = keyFields;
        this.tableSize = tableSize;
        this.comparators = comparators;
        this.recordDesc = recordDesc;
        this.grouper = grouper;
        this.groupState = grouper.createAggregateStates();
        this.outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
        this.isLoadBuffered = isLoadBuffered;
        this.isFinalPhase = isFinalPhase;
        this.framesLimit = framesLimit;
        this.tpc = tpc;

        this.groupTupleBuilder = new ArrayTupleBuilder(recordDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(recordDesc.getFieldCount());
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameReader#open()
     */
    @Override
    public void open() throws HyracksDataException {
        if (isLoadBuffered) {
            while (inFrames.size() + 1 < framesLimit) {
                inFrames.add(ctx.allocateFrame());
            }
            framesBuffered = inFrames.size() / runCursors.length;
        } else {
            while (inFrames.size() < framesLimit - 1 && inFrames.size() < runCursors.length) {
                inFrames.add(ctx.allocateFrame());
            }
            framesBuffered = 1;
        }
        tupleAccessors = new FrameTupleAccessor[runCursors.length];
        currentFrameIndexForRuns = new int[runCursors.length];
        bufferedFramesForRuns = new int[runCursors.length];
        Comparator<ReferenceEntryWithBucketID> comparator = createEntryComparator(comparators);
        topTuples = new ReferencedPriorityQueue(ctx.getFrameSize(), recordDesc, runCursors.length, comparator);
        tupleIndexes = new int[runCursors.length];

        for (int i = 0; i < runCursors.length; i++) {
            int runIndex = topTuples.peek().getRunid();
            tupleIndexes[runIndex] = 0;
            runCursors[runIndex].open();
            for (int j = 0; j < framesBuffered; j++) {

                if (runCursors[runIndex].nextFrame(inFrames.get(runIndex * framesBuffered + j))) {

                    bufferedFramesForRuns[runIndex]++;
                    if (j == 0) {
                        tupleAccessors[runIndex] = new FrameTupleAccessor(ctx.getFrameSize(), recordDesc);
                        tupleAccessors[runIndex].reset(inFrames.get(runIndex * framesBuffered + j));
                        setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
                        currentFrameIndexForRuns[runIndex] = runIndex * framesBuffered;
                    }
                } else {
                    break;
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameReader#nextFrame(java.nio.ByteBuffer)
     */
    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        outFrameAppender.reset(buffer, true);

        while (!topTuples.areRunsExhausted()) {
            ReferenceEntryWithBucketID top = topTuples.peek();
            int runIndex = top.getRunid();
            FrameTupleAccessor fta = top.getAccessor();
            int tupleIndex = top.getTupleIndex();

            // check whether we can do aggregation
            boolean needInsert = true;
            if (groupResultCache != null && groupResultCacheAccessor.getTupleCount() > 0) {
                groupResultCacheAccessor.reset(ByteBuffer.wrap(groupResultCache));
                if (compareFrameTuples(fta, tupleIndex, groupResultCacheAccessor, 0) == 0) {
                    needInsert = false;
                }
            }

            if (needInsert) {

                // try to flush the group cache into the output buffer, if any
                if (groupResultCacheAccessor != null && groupResultCacheAccessor.getFieldCount() > 0) {
                    outputTupleBuilder.reset();
                    for (int k = 0; k < keyFields.length; k++) {
                        outputTupleBuilder.addField(groupResultCacheAccessor, 0, k);
                    }
                    if (isFinalPhase) {
                        grouper.outputFinalResult(outputTupleBuilder, groupResultCacheAccessor, 0, groupState);
                    } else {
                        grouper.outputPartialResult(outputTupleBuilder, groupResultCacheAccessor, 0, groupState);
                    }

                    // return if the buffer is full
                    if (!outFrameAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        return true;
                    }
                    groupResultCacheBuffer.putInt(groupResultCache.length - 4, 0);
                }

                groupTupleBuilder.reset();
                for (int k : keyFields) {
                    groupTupleBuilder.addField(fta, tupleIndex, k);
                }
                grouper.init(groupTupleBuilder, fta, tupleIndex, groupState);

                // enlarge the cache buffer if necessary
                int requiredSize = groupTupleBuilder.getSize() + groupTupleBuilder.getFieldEndOffsets().length
                        * INT_SIZE + 2 * INT_SIZE;

                if (groupResultCache == null || groupResultCache.length < requiredSize) {
                    groupResultCache = new byte[requiredSize];
                    groupResultCacheAppender = new FrameTupleAppender(groupResultCache.length);
                    groupResultCacheBuffer = ByteBuffer.wrap(groupResultCache);
                    groupResultCacheAccessor = new FrameTupleAccessor(groupResultCache.length, recordDesc);
                }

                // always reset the group cache
                groupResultCacheAppender.reset(groupResultCacheBuffer, true);
                if (!groupResultCacheAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                        groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize())) {
                    throw new HyracksDataException("The partial result is too large to be initialized in a frame.");
                }

                groupResultCacheAccessor.reset(groupResultCacheBuffer);

            } else {
                grouper.aggregate(fta, tupleIndex, groupResultCacheAccessor, 0, groupState);
            }

            ++tupleIndexes[runIndex];
            setNextTopTuple(runIndex, tupleIndexes, runCursors, tupleAccessors, topTuples);
        }

        if (groupResultCacheAccessor != null && groupResultCacheAccessor.getTupleCount() > 0) {
            outputTupleBuilder.reset();
            for (int k = 0; k < keyFields.length; k++) {
                outputTupleBuilder.addField(groupResultCacheAccessor, 0, k);
            }
            if (isFinalPhase) {
                grouper.outputFinalResult(outputTupleBuilder, groupResultCacheAccessor, 0, groupState);
            } else {
                grouper.outputPartialResult(outputTupleBuilder, groupResultCacheAccessor, 0, groupState);
            }

            // return if the buffer is full
            if (!outFrameAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(), 0,
                    outputTupleBuilder.getSize())) {
                return true;
            }

            groupResultCacheAccessor = null;
            groupResultCache = null;
            groupResultCacheBuffer = null;
            groupResultCacheAppender = null;
        }

        if (outFrameAppender.getTupleCount() > 0) {
            return true;
        }

        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameReader#close()
     */
    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < runCursors.length; ++i) {
            closeRun(i, runCursors, tupleAccessors);
        }
    }

    private void setNextTopTuple(int runIndex, int[] tupleIndexes, IFrameReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors, ReferencedPriorityQueue topTuples) throws HyracksDataException {
        boolean exists = hasNextTuple(runIndex, tupleIndexes, runCursors, tupleAccessors);
        if (exists) {
            int h = tpc.partition(tupleAccessors[runIndex], tupleIndexes[runIndex], tableSize);
            topTuples.popAndReplace(tupleAccessors[runIndex], tupleIndexes[runIndex], h);
        } else {
            topTuples.pop();
            closeRun(runIndex, runCursors, tupleAccessors);
        }
    }

    private boolean hasNextTuple(int runIndex, int[] tupleIndexes, IFrameReader[] runCursors,
            FrameTupleAccessor[] tupleAccessors) throws HyracksDataException {
        if (tupleAccessors[runIndex] == null || runCursors[runIndex] == null) {
            return false;
        } else if (tupleIndexes[runIndex] >= tupleAccessors[runIndex].getTupleCount()) {
            if (currentFrameIndexForRuns[runIndex] - runIndex * framesBuffered < bufferedFramesForRuns[runIndex] - 1) {
                currentFrameIndexForRuns[runIndex]++;
            } else {
                bufferedFramesForRuns[runIndex] = 0;
                for (int j = 0; j < framesBuffered; j++) {
                    if (runCursors[runIndex].nextFrame(inFrames.get(runIndex * framesBuffered + j))) {
                        bufferedFramesForRuns[runIndex]++;
                    } else {
                        break;
                    }
                }
                currentFrameIndexForRuns[runIndex] = runIndex * framesBuffered;
            }
            if (bufferedFramesForRuns[runIndex] > 0) {
                tupleAccessors[runIndex].reset(inFrames.get(currentFrameIndexForRuns[runIndex]));
                tupleIndexes[runIndex] = 0;
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    private void closeRun(int index, IFrameReader[] runCursors, IFrameTupleAccessor[] tupleAccessors)
            throws HyracksDataException {
        if (runCursors[index] != null) {
            runCursors[index].close();
            runCursors[index] = null;
            tupleAccessors[index] = null;
        }
    }

    private int compareFrameTuples(IFrameTupleAccessor fta1, int j1, IFrameTupleAccessor fta2, int j2) {
        mergeCompCounter++;
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

    private Comparator<ReferenceEntryWithBucketID> createEntryComparator(final IBinaryComparator[] comparators) {
        return new Comparator<ReferenceEntryWithBucketID>() {
            public int compare(ReferenceEntryWithBucketID tp1, ReferenceEntryWithBucketID tp2) {

                queueCompCounter++;

                int cmp = tp1.getBucketID() - tp2.getBucketID();

                if (cmp != 0) {
                    return cmp;
                }

                FrameTupleAccessor fta1 = (FrameTupleAccessor) tp1.getAccessor();
                FrameTupleAccessor fta2 = (FrameTupleAccessor) tp2.getAccessor();
                int j1 = tp1.getTupleIndex();
                int j2 = tp2.getTupleIndex();
                byte[] b1 = fta1.getBuffer().array();
                byte[] b2 = fta2.getBuffer().array();
                for (int f = 0; f < keyFields.length; ++f) {
                    int fIdx = keyFields[f];
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

                return cmp;
            }
        };
    }
}
