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
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.el.InMemHybridHashSortELGroupHash;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceEntry;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceMinHeap;
import edu.uci.ics.hyracks.dataflow.std.util.ReferencedPriorityQueue;

public class HybridHashSortELRunMerger {

    private static final int INT_SIZE = 4;

    private final IHyracksTaskContext ctx;

    // for loading
    private final IFrameReader[] runCursors;
    private final List<ByteBuffer> inFrames;
    private int framesBuffered;
    private IFrameTupleAccessor[] runFrameAccessors;
    private int[] runFrameIndex, runFrameTupleIndex, runFramesBuffered;
    private final int frameSize;

    private final int[] keyFields;
    private final int framesLimit;
    private final int sortThreshold;

    private final int tableSize;
    private final ITuplePartitionComputer partComputer;

    // for output
    private final IFrameWriter outputWriter;
    private final FrameTupleAppender outFrameAppender;
    private final ArrayTupleBuilder outputTupleBuilder;
    private ByteBuffer outputBuffer;

    // for in-memory hash table buffer
    private ByteBuffer inMemHTBuffer;
    private IFrameTupleAccessor inMemHTBufferAccessor;
    private boolean isBufferDone;
    private int inMemHTFrameTupleIndex;

    // for merging
    private byte[] groupCache;
    private ByteBuffer groupCacheBuffer;
    private IFrameTupleAccessor groupCacheAccessor;
    private final ArrayTupleBuilder groupTupleBuilder;
    private FrameTupleAppender groupCacheAppender;
    private int currentWorkingEntryID;

    // for in-memory hashing
    private InMemHybridHashSortELGroupHash inmemHashtable;
    private final int maxRecordLength;
    private final ITuplePartitionComputer inMemHashtableTpc;
    private final IBinaryComparator[] comparators;
    private final INormalizedKeyComputer firstNormalizer;

    // for sorted-access of entries
    private ReferenceMinHeap<RunReferenceHashEntry> runHeap;
    private RunReferenceHashEntry[] runRefs;
    private BitSet runFileInQueueFlags;

    // for record merging
    private final Comparator<ReferenceEntry> comparator;

    private final RecordDescriptor outRecDesc;

    // for aggregation
    private final IAggregatorDescriptor grouper;
    private final AggregateState groupState;

    // for multi-entry merging
    private final boolean allowMultipleEntriesMerging;

    // for debugging
    private static final Logger LOGGER = Logger.getLogger(HybridHashSortELRunMerger.class.getSimpleName());
    private long totalComparisons = 0, recordProcessed = 0, runHeapComparisons = 0, recordPriorityQueueComparisons = 0,
            groupCacheComparisons = 0, recordInsertedInHT = 0, recordMergedFromRuns = 0, outputRecFromInMemHT = 0,
            outputRecFromSortRuns = 0, mergeTime = 0;
    private int inmemHTResetCount = 0;

    private final boolean isLoadBuffered;

    public HybridHashSortELRunMerger(IHyracksTaskContext ctx, IFrameReader[] runCursors, int framesLimit,
            int tableSize, int sortThreshold, int maxRecordLength, final int[] keyFields, ITuplePartitionComputer tpc,
            ITuplePartitionComputer inMemTpc, final IBinaryComparator[] comparators, INormalizedKeyComputer nkc,
            IAggregatorDescriptor grouper, RecordDescriptor recordDesc, IFrameWriter outputWriter,
            boolean isLoadBuffered, boolean allowMultiEntriesMerging) {
        this.ctx = ctx;
        this.runCursors = runCursors;
        this.framesLimit = framesLimit;
        this.sortThreshold = sortThreshold;
        this.keyFields = keyFields;
        this.tableSize = tableSize;

        this.partComputer = tpc;

        this.frameSize = ctx.getFrameSize();

        this.outputWriter = outputWriter;
        this.outputBuffer = ctx.allocateFrame();
        this.outFrameAppender = new FrameTupleAppender(frameSize);
        this.outFrameAppender.reset(outputBuffer, true);
        this.outputTupleBuilder = new ArrayTupleBuilder(recordDesc.getFieldCount());

        this.groupTupleBuilder = new ArrayTupleBuilder(recordDesc.getFieldCount());

        this.grouper = grouper;
        this.groupState = grouper.createAggregateStates();

        this.inFrames = new ArrayList<ByteBuffer>();

        this.isLoadBuffered = isLoadBuffered;

        this.maxRecordLength = maxRecordLength;

        this.comparators = comparators;
        this.inMemHashtableTpc = inMemTpc;
        this.firstNormalizer = nkc;
        this.outRecDesc = recordDesc;

        this.allowMultipleEntriesMerging = allowMultiEntriesMerging;

        this.comparator = new Comparator<ReferenceEntry>() {

            @Override
            public int compare(ReferenceEntry tp1, ReferenceEntry tp2) {
                // FIXME
                totalComparisons++;
                recordPriorityQueueComparisons++;

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

                return 0;
            }
        };

    }

    /**
     * Process the merging of run files. Runs containing the same bucket will be
     * popped from the run heap and inserted into a priority queue for merging. After
     * the merging is finished, runs are inserted back to the queue for the next
     * bucket.
     * 
     * @throws HyracksDataException
     */
    public void process() throws HyracksDataException {

        // if allow multiple entry merging, all available frames are assigned to the hash table; otherwise, only
        // necessary hash table (capable for all unsorted records from all runs) is allocated
        int inmemTableSize = allowMultipleEntriesMerging ? (tableSize / framesLimit * (framesLimit - runCursors.length - 2))
                : ((sortThreshold + 1) * runCursors.length * 2);

        int inmemHashTableSizeInFrame = InMemHybridHashSortELGroupHash.getMinimumFrameRequirement(inmemTableSize,
                sortThreshold, runCursors.length, maxRecordLength, frameSize);

        // memory requirement: in-memory hash table, run file buffers, and in-memory hash table buffer, and the output buffer
        if (inmemHashTableSizeInFrame + runCursors.length + 2 > framesLimit) {
            throw new HyracksDataException("Memory is not large enough for the in-memory merging");
        }

        outputWriter.open();

        int framesForInputBuffer = frameSize - inmemHashTableSizeInFrame - 1;

        framesBuffered = 1;
        if (isLoadBuffered) {
            framesBuffered = framesForInputBuffer / runCursors.length;
        }

        inmemHashtable = new InMemHybridHashSortELGroupHash(ctx, framesLimit - runCursors.length * framesBuffered - 1,
                inmemTableSize, sortThreshold, runCursors.length, maxRecordLength, keyFields, comparators,
                inMemHashtableTpc, firstNormalizer, grouper, outRecDesc, allowMultipleEntriesMerging);

        runFileInQueueFlags = new BitSet(runCursors.length);

        // initialize reference for each run file
        runRefs = new RunReferenceHashEntry[runCursors.length];
        runHeap = new ReferenceMinHeap<RunReferenceHashEntry>(RunReferenceHashEntry.class,
                new RunReferenceHashEntryComparator(), runRefs.length);

        runFrameAccessors = new FrameTupleAccessor[runCursors.length];
        runFrameIndex = new int[runCursors.length];
        runFrameTupleIndex = new int[runCursors.length];
        runFramesBuffered = new int[runCursors.length];

        // initialize the run heap, by inserting runs into the heap with the top bucket id
        for (int i = 0; i < runCursors.length; i++) {
            runFrameTupleIndex[i] = 0;
            runCursors[i].open();
            for (int j = 0; j < framesBuffered; j++) {
                if (i * framesBuffered + j >= inFrames.size()) {
                    inFrames.add(ctx.allocateFrame());
                }
                if (!runCursors[i].nextFrame(inFrames.get(i * framesBuffered + j))) {
                    break;
                }
                runFramesBuffered[i]++;
                if (j == 0) {
                    if (runFrameAccessors[i] == null) {
                        runFrameAccessors[i] = new FrameTupleAccessor(frameSize, outRecDesc);
                    }
                    runFrameAccessors[i].reset(inFrames.get(i * framesBuffered));
                    runFrameIndex[i] = i * framesBuffered;
                }
            }
            if (runFramesBuffered[i] == 0) {
                continue;
            }
            runRefs[i] = new RunReferenceHashEntry(i, partComputer.partition(runFrameAccessors[i], 0, tableSize));
            runHeap.insert(runRefs[i]);
            runFileInQueueFlags.set(i);
        }

        while (!runHeap.isEmpty()) {

            if (allowMultipleEntriesMerging) {
                inmemHashtable.markNewEntry();
            }

            // long timer = System.currentTimeMillis();
            // fetch the runs with the same top entry
            RunReferenceHashEntry topEntry = runHeap.pop();
            runFileInQueueFlags.clear(topEntry.runID);
            currentWorkingEntryID = topEntry.hashEntryID;
            while (!runHeap.isEmpty() && runHeap.top().hashEntryID == currentWorkingEntryID) {
                topEntry = runHeap.pop();
                runFileInQueueFlags.clear(topEntry.runID);
            }

            // process the un-sorted records
            for (int i = runFileInQueueFlags.nextClearBit(0); i >= 0 && i < runCursors.length; i = runFileInQueueFlags
                    .nextClearBit(i + 1)) {
                if (runRefs[i] == null) {
                    continue;
                }
                for (int j = 0; j < sortThreshold + 1; j++) {

                    int entryID = partComputer.partition(runFrameAccessors[i], runFrameTupleIndex[i], tableSize);
                    if (entryID != currentWorkingEntryID) {
                        // new entry; stop
                        runRefs[i].setHashEntryID(entryID);
                        runHeap.insert(runRefs[i]);
                        runFileInQueueFlags.set(i);
                        break;
                    } else {
                        recordProcessed++;
                        recordInsertedInHT++;
                        inmemHashtable.insert((FrameTupleAccessor) runFrameAccessors[i], runFrameTupleIndex[i]);
                        runFrameTupleIndex[i]++;
                    }

                    if (runFrameTupleIndex[i] >= runFrameAccessors[i].getTupleCount()) {
                        // try to load a new frame
                        if (runFrameIndex[i] - i * framesBuffered < runFramesBuffered[i] - 1) {
                            runFrameIndex[i]++;
                        } else {
                            runFramesBuffered[i] = 0;
                            for (int k = 0; k < framesBuffered; k++) {
                                if (runCursors[i].nextFrame(inFrames.get(i * framesBuffered + k))) {
                                    runFramesBuffered[i]++;
                                } else {
                                    break;
                                }
                            }
                            runFrameIndex[i] = i * framesBuffered;
                        }
                        if (runFramesBuffered[i] > 0) {
                            runFrameAccessors[i].reset(inFrames.get(runFrameIndex[i]));
                            runFrameTupleIndex[i] = 0;
                        } else {
                            closeRun(i);
                            break;
                        }
                    }
                }
            }

            // do merging
            int runsNotFinished = runCursors.length - runFileInQueueFlags.cardinality();
            if (runsNotFinished > 0) {
                // more records; sort the hash table
                inmemHashtable.sortHashtable();
                isBufferDone = false;

                IFrameReader[] mergeRuns = new IFrameReader[runsNotFinished + 1];
                int[] runsIndex = new int[runsNotFinished + 1];
                int mergeRunsIdx = 0;
                for (int i = runFileInQueueFlags.nextClearBit(0); i >= 0 && i < runCursors.length; i = runFileInQueueFlags
                        .nextClearBit(i + 1)) {
                    mergeRuns[mergeRunsIdx] = runCursors[i];
                    runsIndex[mergeRunsIdx] = i;
                    mergeRunsIdx++;
                }
                mergeRuns[mergeRunsIdx] = inmemHashtable;
                runsIndex[mergeRunsIdx] = -1;
                merge(mergeRuns, runsIndex);
            }

            if (!inmemHashtable.hasEnoughSpaceForNewEntry()) {
                // directly flush the hash table
                outputRecFromInMemHT += inmemHashtable.getTupleCount();
                while (true) {
                    int rtn = inmemHashtable.nextFrameForFinalOutput(outputBuffer);
                    if (rtn <= 0) {
                        break;
                    } else {
                        FrameUtils.flushFrame(outputBuffer, outputWriter);
                        outFrameAppender.reset(outputBuffer, true);
                    }
                }
                inmemHashtable.reset();
                inmemHTResetCount++;
            }
            // LOGGER.warning("Processed\t" + currentWorkingEntryID + "\t" + (System.currentTimeMillis() - timer));

        }

        if (inmemHashtable.getTupleCount() > 0) {
            // directly flush the hash table
            outputRecFromInMemHT += inmemHashtable.getTupleCount();
            while (true) {
                int rtn = inmemHashtable.nextFrameForFinalOutput(outputBuffer);
                if (rtn <= 0) {
                    break;
                } else {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    outFrameAppender.reset(outputBuffer, true);
                }
            }
        }

        inmemHashtable.close();

        outFrameAppender.reset(outputBuffer, false);
        if (outFrameAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
        }
        LOGGER.warning(HybridHashSortELRunMerger.class.getSimpleName() + "-Process\t" + recordProcessed + "\t"
                + recordInsertedInHT + "\t" + recordMergedFromRuns + "\t" + outputRecFromInMemHT + "\t"
                + outputRecFromSortRuns + "\t" + totalComparisons + "\t" + runHeapComparisons + "\t"
                + recordPriorityQueueComparisons + "\t" + groupCacheComparisons + "\t" + inmemHTResetCount + "\t"
                + mergeTime);
    }

    private void merge(IFrameReader[] runs, int[] runsIndex) throws HyracksDataException {
        long timer = System.currentTimeMillis();
        outFrameAppender.reset(outputBuffer, false);
        ReferencedPriorityQueue topTuples = new ReferencedPriorityQueue(frameSize, outRecDesc, runs.length, comparator);
        for (int i = 0; i < runs.length; i++) {
            int runId = topTuples.peek().getRunid();
            if (runId < runs.length - 1) {
                // regular run files
                setNextTopTuple(runsIndex[runId], topTuples);
                //                topTuples.popAndReplace((FrameTupleAccessor) runFrameAccessors[runsIndex[runId]],
                //                        runFrameTupleIndex[runsIndex[runId]]);
            } else {
                // in-memory hash table
                if (inMemHTBuffer == null) {
                    inMemHTBuffer = ctx.allocateFrame();
                }
                if (inMemHTBufferAccessor == null) {
                    inMemHTBufferAccessor = new FrameTupleAccessor(frameSize, outRecDesc);
                }
                if (!inmemHashtable.nextFrame(inMemHTBuffer)) {
                    isBufferDone = true;
                } else {
                    inMemHTBufferAccessor.reset(inMemHTBuffer);
                    inMemHTFrameTupleIndex = 0;
                    topTuples.popAndReplace((FrameTupleAccessor) inMemHTBufferAccessor, inMemHTFrameTupleIndex);
                }
            }
        }

        while (!topTuples.areRunsExhausted()) {
            ReferenceEntry top = topTuples.peek();

            FrameTupleAccessor fta = top.getAccessor();
            int tupleIndex = top.getTupleIndex();

            boolean needInsert = true;
            if (groupCache != null && groupCacheAccessor.getTupleCount() > 0) {
                if (compareFrameTuples(fta, tupleIndex, groupCacheAccessor, 0) == 0) {

                    needInsert = false;
                }
            }
            if (needInsert) {
                // try to flush the group cache into the output buffer, if any
                if (groupCacheAccessor != null && groupCacheAccessor.getTupleCount() > 0) {
                    outputTupleBuilder.reset();
                    for (int k = 0; k < keyFields.length; k++) {
                        outputTupleBuilder.addField(groupCacheAccessor, 0, k);
                    }
                    grouper.outputFinalResult(outputTupleBuilder, groupCacheAccessor, 0, groupState);

                    // return if the buffer is full
                    if (!outFrameAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outputBuffer, outputWriter);
                        outFrameAppender.reset(outputBuffer, true);
                        if (!outFrameAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to output an aggregation result.");
                        }
                    }
                    groupCacheBuffer.putInt(groupCache.length - INT_SIZE, 0);
                }

                // insert the record into the grouping cache
                groupTupleBuilder.reset();
                for (int k : keyFields) {
                    groupTupleBuilder.addField(fta, tupleIndex, k);
                }
                grouper.init(groupTupleBuilder, fta, tupleIndex, groupState);
                // enlarge the cache buffer if necessary
                int requiredSize = groupTupleBuilder.getSize() + groupTupleBuilder.getFieldEndOffsets().length
                        * INT_SIZE + 2 * INT_SIZE;

                if (groupCache == null || groupCache.length < requiredSize) {
                    groupCache = new byte[requiredSize];
                    groupCacheBuffer = ByteBuffer.wrap(groupCache);
                    groupCacheAppender = new FrameTupleAppender(groupCache.length);
                    groupCacheAppender.reset(groupCacheBuffer, true);
                    groupCacheAccessor = new FrameTupleAccessor(groupCache.length, outRecDesc);
                }

                groupCacheAppender.reset(groupCacheBuffer, true);
                if (!groupCacheAppender.append(groupTupleBuilder.getFieldEndOffsets(),
                        groupTupleBuilder.getByteArray(), 0, groupTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to insert a partial aggregation result into the group cache");
                }

                groupCacheAccessor.reset(groupCacheBuffer);

                if (runsIndex[top.getRunid()] >= 0) {
                    outputRecFromSortRuns++;
                }

            } else {
                // aggregate
                grouper.aggregate(fta, tupleIndex, groupCacheAccessor, 0, groupState);
            }
            if (runsIndex[top.getRunid()] >= 0) {
                runFrameTupleIndex[runsIndex[top.getRunid()]]++;
                recordProcessed++;
                recordMergedFromRuns++;
            } else {
                inMemHTFrameTupleIndex++;
            }
            setNextTopTuple(runsIndex[top.getRunid()], topTuples);
        }
        if (groupCache != null && groupCacheAccessor.getTupleCount() > 0) {
            outputTupleBuilder.reset();
            for (int k = 0; k < keyFields.length; k++) {
                outputTupleBuilder.addField(groupCacheAccessor, 0, k);
            }
            grouper.outputFinalResult(outputTupleBuilder, groupCacheAccessor, 0, groupState);

            // return if the buffer is full
            if (!outFrameAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(), 0,
                    outputTupleBuilder.getSize())) {
                FrameUtils.flushFrame(outputBuffer, outputWriter);
                outFrameAppender.reset(outputBuffer, true);
                if (!outFrameAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                        outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to output an aggregation result.");
                }
            }

            groupCacheBuffer.putInt(groupCache.length - INT_SIZE, 0);
        }

        mergeTime += System.currentTimeMillis() - timer;
    }

    private int compareFrameTuples(IFrameTupleAccessor fta1, int j1, IFrameTupleAccessor fta2, int j2) {
        // FIXME
        totalComparisons++;
        groupCacheComparisons++;

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

    private void setNextTopTuple(int runIndex, ReferencedPriorityQueue topTuples) throws HyracksDataException {
        boolean exists = hasNextTuple(runIndex);
        if (exists) {
            if (runIndex >= 0) {
                int entryID = partComputer.partition(runFrameAccessors[runIndex], runFrameTupleIndex[runIndex],
                        tableSize);
                if (entryID != currentWorkingEntryID) {
                    // get the next entry now. stop for merging
                    topTuples.pop();
                    runRefs[runIndex].setHashEntryID(entryID);
                    runHeap.insert(runRefs[runIndex]);
                    runFileInQueueFlags.set(runIndex);
                } else {
                    topTuples.popAndReplace((FrameTupleAccessor) runFrameAccessors[runIndex],
                            runFrameTupleIndex[runIndex]);
                }
            } else {
                topTuples.popAndReplace((FrameTupleAccessor) inMemHTBufferAccessor, inMemHTFrameTupleIndex);
            }
        } else {
            topTuples.pop();
            if (runIndex >= 0) {
                closeRun(runIndex);
            } else {
                isBufferDone = true;
            }
        }
    }

    private void closeRun(int runIndex) throws HyracksDataException {
        if (runCursors[runIndex] != null) {
            runCursors[runIndex].close();
            runCursors[runIndex] = null;
            runFrameAccessors[runIndex] = null;
            runRefs[runIndex] = null;
            runFileInQueueFlags.set(runIndex);
        }
    }

    private boolean hasNextTuple(int runIndex) throws HyracksDataException {
        if (runIndex < 0) {
            // for in-memory hash table
            if (isBufferDone) {
                return false;
            } else if (inMemHTFrameTupleIndex >= inMemHTBufferAccessor.getTupleCount()) {
                if (inmemHashtable.nextFrame(inMemHTBuffer)) {
                    inMemHTFrameTupleIndex = 0;
                    inMemHTBufferAccessor.reset(inMemHTBuffer);
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        } else {
            if (runFileInQueueFlags.get(runIndex)) {
                return false;
            } else if (runFrameAccessors[runIndex] == null || runCursors[runIndex] == null) {
                return false;
            } else if (runFrameTupleIndex[runIndex] >= runFrameAccessors[runIndex].getTupleCount()) {
                if (runFrameIndex[runIndex] - runIndex * framesBuffered < runFramesBuffered[runIndex] - 1) {
                    runFrameIndex[runIndex]++;
                } else {
                    runFramesBuffered[runIndex] = 0;
                    for (int j = 0; j < framesBuffered; j++) {
                        if (runCursors[runIndex].nextFrame(inFrames.get(runIndex * framesBuffered + j))) {
                            runFramesBuffered[runIndex]++;
                        } else {
                            break;
                        }
                    }
                    runFrameIndex[runIndex] = runIndex * framesBuffered;
                }
                if (runFramesBuffered[runIndex] > 0) {
                    runFrameAccessors[runIndex].reset(inFrames.get(runFrameIndex[runIndex]));
                    runFrameTupleIndex[runIndex] = 0;
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        }
    }

    class RunReferenceHashEntry {
        final int runID;
        int hashEntryID;

        public RunReferenceHashEntry(int runID, int hashEntryID) {
            this.runID = runID;
            this.hashEntryID = hashEntryID;
        }

        void setHashEntryID(int newHashEntryID) {
            this.hashEntryID = newHashEntryID;
        }

        public String toString() {
            return runID + ":" + hashEntryID;
        }
    }

    class RunReferenceHashEntryComparator implements Comparator<RunReferenceHashEntry> {

        @Override
        public int compare(RunReferenceHashEntry o1, RunReferenceHashEntry o2) {
            totalComparisons++;
            runHeapComparisons++;
            return o1.hashEntryID - o2.hashEntryID;
        }

    }

}
