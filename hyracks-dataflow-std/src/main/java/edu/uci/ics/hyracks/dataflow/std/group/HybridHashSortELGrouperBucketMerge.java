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
import java.util.BitSet;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.el.InMemHybridHashSortELMergeHashTable;
import edu.uci.ics.hyracks.dataflow.std.util.ReferenceMinHeap;

public class HybridHashSortELGrouperBucketMerge {

    private static final Logger LOGGER = Logger.getLogger(HybridHashSortELGrouperBucketMerge.class.getSimpleName());

    private final int[] keyFields;
    private final IBinaryComparator[] comparators;

    private final IAggregatorDescriptor merger;

    private final int framesLimit, tableSize;

    private final RecordDescriptor inRecDesc;

    private final IHyracksTaskContext ctx;

    private final IFrameWriter outputWriter;

    private final ITuplePartitionComputer aggTpc;
    private final ITuplePartitionComputerFamily tpcf;

    List<ByteBuffer> inFrames;
    ByteBuffer outFrame, writerFrame;
    FrameTupleAppender outAppender, writerAppender;
    ArrayTupleBuilder finalTupleBuilder;
    FrameTupleAccessor outFrameAccessor;
    int[] currentFrameIndexInRun, currentRunFrames, currentBucketInRun;
    int runFrameLimit = 1;

    private final int sortedThreshold;

    private final INormalizedKeyComputer firstNormalizer;

    private final RecordDescriptor outRecDesc;

    private final int estimatedRecSizeInBytes;

    // FIXME
    private long totalComparisons = 0;

    private int hashFunctionSeed = 1;

    public HybridHashSortELGrouperBucketMerge(IHyracksTaskContext ctx, int[] keyFields, int framesLimit, int tableSize,
            int sortThreshold, int estimatedRecSize, ITuplePartitionComputer aggTpc,
            ITuplePartitionComputerFamily tpcf, IBinaryComparator[] comparators,
            INormalizedKeyComputer firstNormalizerComputer, IAggregatorDescriptor merger, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;

        this.keyFields = keyFields;
        this.comparators = comparators;
        this.merger = merger;

        this.estimatedRecSizeInBytes = estimatedRecSize;

        this.inRecDesc = inRecDesc;

        this.outAppender = new FrameTupleAppender(ctx.getFrameSize());

        this.outputWriter = outputWriter;

        this.outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);

        this.aggTpc = aggTpc;
        this.tpcf = tpcf;

        this.sortedThreshold = sortThreshold;

        this.firstNormalizer = firstNormalizerComputer;

        this.outRecDesc = outRecDesc;
    }

    public void initialize(LinkedList<RunFileReader> runFiles) throws HyracksDataException {
        BitSet runFileInQueueFlags = new BitSet(runFiles.size());

        // initialize reference for each run file
        RunReferenceHashEntry[] runRefs = new RunReferenceHashEntry[runFiles.size()];
        ReferenceMinHeap<RunReferenceHashEntry> bucketHeap = new ReferenceMinHeap<RunReferenceHashEntry>(
                RunReferenceHashEntry.class, new RunReferenceHashEntryComparator(), runRefs.length);
        FrameTupleAccessor[] runFrameAccessors = new FrameTupleAccessor[runFiles.size()];
        int[] currentTupleIndexInBuffer = new int[runFiles.size()];
        ByteBuffer[] runBuffers = new ByteBuffer[runFiles.size()];

        // initialize the references and bucket heap
        for (int i = 0; i < runFiles.size(); i++) {
            runBuffers[i] = ctx.allocateFrame();
            runFiles.get(i).open();
            if (!runFiles.get(i).nextFrame(runBuffers[i])) {
                continue;
            }
            runFrameAccessors[i] = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
            runFrameAccessors[i].reset(runBuffers[i]);
            runRefs[i] = new RunReferenceHashEntry(i, aggTpc.partition(runFrameAccessors[i], 0, tableSize));
            bucketHeap.insert(runRefs[i]);
            currentTupleIndexInBuffer[i] = 0;
            runFileInQueueFlags.set(i);
        }

        int inMemTableFrames = framesLimit - runFiles.size();
        if (inMemTableFrames <= 0) {
            inMemTableFrames = 2;
        }

        // calculate the in-memory hash table size
        int inMemTableSize = inMemTableFrames * (tableSize / framesLimit);

        InMemHybridHashSortELMergeHashTable inmemHashtable = new InMemHybridHashSortELMergeHashTable(ctx, inMemTableFrames,
                inMemTableSize, sortedThreshold, runFiles.size(), keyFields, comparators,
                tpcf.createPartitioner(hashFunctionSeed++), firstNormalizer, this.merger, inRecDesc, outRecDesc,
                outputWriter);

        int inmemHTCapacity = InMemHybridHashSortELMergeHashTable.getHashTableCapacity(inMemTableFrames, ctx.getFrameSize(),
                estimatedRecSizeInBytes, inMemTableSize);

        int currentWorkingBucketID;

        ReferenceMinHeap<EntryReference> recHeap = new ReferenceMinHeap<EntryReference>(EntryReference.class,
                new EntryReferenceComparator(), runFiles.size());
        EntryReference[] runRecReferences = new EntryReference[runFiles.size()];

        while (!bucketHeap.isEmpty()) {

            if (inmemHTCapacity * 0.8 - inmemHashtable.getTupleCount() <= runFiles.size() * (sortedThreshold + 1)) {
                inmemHashtable.flushHashtableToOutput(outputWriter);
                inmemHashtable.reset();
            }

            RunReferenceHashEntry topEntry = bucketHeap.pop();
            runFileInQueueFlags.clear(topEntry.runID);
            currentWorkingBucketID = topEntry.hashEntryID;
            while (!bucketHeap.isEmpty() && bucketHeap.top().hashEntryID == currentWorkingBucketID) {
                topEntry = bucketHeap.pop();
                runFileInQueueFlags.clear(topEntry.runID);
            }
            // load the non-sorted records
            for (int i = runFileInQueueFlags.nextClearBit(0); i >= 0 && i < runFiles.size(); i = runFileInQueueFlags
                    .nextClearBit(i + 1)) {
                if (runRefs[i] == null) {
                    continue;
                }
                for (int j = 0; j < sortedThreshold + 1; j++) {
                    int entryID = aggTpc.partition(runFrameAccessors[i], currentTupleIndexInBuffer[i], tableSize);
                    if (entryID != currentWorkingBucketID) {
                        runRefs[i].setHashEntryID(entryID);
                        bucketHeap.insert(runRefs[i]);
                        runFileInQueueFlags.set(i);
                        break;
                    } else {
                        inmemHashtable.insert(runFrameAccessors[i], currentTupleIndexInBuffer[i], entryID);
                    }
                    currentTupleIndexInBuffer[i]++;
                    if (currentTupleIndexInBuffer[i] >= runFrameAccessors[i].getTupleCount()) {
                        // load another frame
                        if (runFiles.get(i).nextFrame(runBuffers[i])) {
                            runFrameAccessors[i].reset(runBuffers[i]);
                            currentTupleIndexInBuffer[i] = 0;
                        } else {
                            // run file is exhausted
                            runRefs[i] = null;
                            runFiles.get(i).close();
                            break;
                        }
                    }
                }
            }
            // do sort, if necessary
            boolean hasMoreRec = false;
            for (int i = runFileInQueueFlags.nextClearBit(0); i >= 0 && i < runFiles.size(); i = runFileInQueueFlags
                    .nextClearBit(i + 1)) {
                if (runRefs[i] != null) {
                    hasMoreRec = true;
                    break;
                }
            }

            if (hasMoreRec) {

                inmemHashtable.buildSortRefs();

                recHeap.reset();
                // insert the records into the queue
                for (int i = runFileInQueueFlags.nextClearBit(0); i >= 0 && i < runFiles.size(); i = runFileInQueueFlags
                        .nextClearBit(i + 1)) {
                    if (runRefs[i] != null) {
                        if (runRecReferences[i] == null) {
                            runRecReferences[i] = new EntryReference(i, runFrameAccessors[i],
                                    currentTupleIndexInBuffer[i]);
                        } else {
                            runRecReferences[i].reset(runFrameAccessors[i], currentTupleIndexInBuffer[i]);
                        }
                        recHeap.insert(runRecReferences[i]);
                    }
                }

                while (!recHeap.isEmpty()) {
                    EntryReference topRec = recHeap.top();
                    int e = aggTpc.partition(topRec.getAccessor(), topRec.getTupleIndex(), tableSize);
                    if (e != currentWorkingBucketID) {
                        recHeap.pop();
                        runRefs[topRec.runID].setHashEntryID(e);
                        bucketHeap.insert(runRefs[topRec.runID]);
                        runFileInQueueFlags.set(topRec.runID);
                    } else {
                        inmemHashtable.insertAfterSorted(topRec.getAccessor(), topRec.getTupleIndex());
                        currentTupleIndexInBuffer[topRec.runID]++;
                        if (currentTupleIndexInBuffer[topRec.runID] >= runFrameAccessors[topRec.runID].getTupleCount()) {
                            if (runFiles.get(topRec.runID).nextFrame(runBuffers[topRec.runID])) {
                                runFrameAccessors[topRec.runID].reset(runBuffers[topRec.runID]);
                                currentTupleIndexInBuffer[topRec.runID] = 0;
                            } else {
                                recHeap.pop();
                                runRefs[topRec.runID] = null;
                                runFiles.get(topRec.runID).close();
                                continue;
                            }
                        }
                        topRec.reset(currentTupleIndexInBuffer[topRec.runID]);
                        recHeap.heapfyTop();
                    }
                }

                inmemHashtable.flushSortedList();
                inmemHashtable.flushUnSortedRecords();
                inmemHashtable.reset();

            }
        }

        if (inmemHashtable.getTupleCount() > 0) {
            inmemHashtable.flushHashtableToOutput(outputWriter);
        }

        inmemHashtable.close();

        LOGGER.warning("HybridHashSortELHashTable\t" + totalComparisons);
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
            return o1.hashEntryID - o2.hashEntryID;
        }

    }

    class EntryReference {
        final int runID;
        FrameTupleAccessor accessor;
        int tupleIndex;

        public EntryReference(int runID, FrameTupleAccessor accessor, int tupleIndex) {
            this.runID = runID;
            this.accessor = accessor;
            this.tupleIndex = tupleIndex;
        }

        public void reset(FrameTupleAccessor accessor, int tupleIndex) {
            this.accessor = accessor;
            this.tupleIndex = tupleIndex;
        }

        public void reset(int tupleIndex) {
            this.tupleIndex = tupleIndex;
        }

        FrameTupleAccessor getAccessor() {
            return accessor;
        }

        int getTupleIndex() {
            return tupleIndex;
        }

        public String toString() {
            return runID + ":" + accessor.toString() + ":" + tupleIndex;
        }
    }

    class EntryReferenceComparator implements Comparator<EntryReference> {

        @Override
        public int compare(EntryReference o1, EntryReference o2) {
            totalComparisons++;
            FrameTupleAccessor fta1 = (FrameTupleAccessor) o1.getAccessor();
            FrameTupleAccessor fta2 = (FrameTupleAccessor) o2.getAccessor();
            int j1 = o1.getTupleIndex();
            int j2 = o2.getTupleIndex();
            byte[] b1 = fta1.getBuffer().array();
            byte[] b2 = fta2.getBuffer().array();
            for (int f = 0; f < keyFields.length; ++f) {
                int fIdx = f;
                int s1 = fta1.getTupleStartOffset(j1) + fta1.getFieldSlotsLength() + fta1.getFieldStartOffset(j1, fIdx);
                int l1 = fta1.getFieldEndOffset(j1, fIdx) - fta1.getFieldStartOffset(j1, fIdx);
                int s2 = fta2.getTupleStartOffset(j2) + fta2.getFieldSlotsLength() + fta2.getFieldStartOffset(j2, fIdx);
                int l2 = fta2.getFieldEndOffset(j2, fIdx) - fta2.getFieldStartOffset(j2, fIdx);
                int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }

    }

}
