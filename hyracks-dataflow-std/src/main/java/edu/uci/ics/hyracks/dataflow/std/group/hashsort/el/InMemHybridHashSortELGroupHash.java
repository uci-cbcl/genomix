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
package edu.uci.ics.hyracks.dataflow.std.group.hashsort.el;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksCommonContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAccessorForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAppenderForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class InMemHybridHashSortELGroupHash implements IFrameReader {

    private static final int INT_SIZE = 4;
    private static final int HASH_REF_LENGTH = 2 * INT_SIZE;
    private static final int PTR_SIZE = 3;

    private final IHyracksCommonContext ctx;
    private final int[] keys;

    private final int framesLimit, tableSize, frameSize;

    private final ByteBuffer[] headers;
    private final List<ByteBuffer> contents;
    private int currentWorkingFrameIndex;
    private final byte[] rawHashBytes;

    // for hashing
    private final IBinaryComparator[] comparators;
    private final ITuplePartitionComputer partComputer;

    // for sort
    private final int[] tPointers;
    private final INormalizedKeyComputer firstNormalizerComputer;
    private final FrameTupleAccessorForGroupHashtable compHashtableAccessor1, compHashtableAccessor2;
    private boolean isSorted;

    // for hashtable lookup
    private final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;
    private final TuplePointer matchPointer;

    // for aggregation
    private final IAggregatorDescriptor aggregator;
    private final AggregateState aggregateState;
    private final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;
    private final FrameTupleAppenderForGroupHashtable internalAppender;
    private final FrameTupleAppender outputAppender;

    // for multiple entry processing
    private int sortFrameIndex;
    private int sortTupleIndex;
    private final int minimumContentFrameRequirement;
    private final boolean allowMultipleEntries;
    private int sortTupleInTotalIndex;

    private int tupleInTable = 0;

    // for hash table instrumenting
    private static final Logger LOGGER = Logger.getLogger(InMemHybridHashSortELGroupHash.class.getSimpleName());
    private int entriesUsed = 0;
    private long totalComparisons = 0, sortTime = 0, hashTime = 0, totalSwaps = 0;
    private double maxHashtableRatio = 0;

    private int sortFlushPosition, hashFlushFrameIndex, hashFlushTupleIndex;

    public InMemHybridHashSortELGroupHash(IHyracksCommonContext ctx, int framesLimit, int tableSize, int sortThreshold,
            int runCount, int expectedRecordLength, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputer tpc, INormalizedKeyComputer firstNormalizerComputer,
            IAggregatorDescriptor aggregator, RecordDescriptor outRecDesc) throws HyracksDataException {
        this(ctx, framesLimit, tableSize, sortThreshold, runCount, expectedRecordLength, keys, comparators, tpc,
                firstNormalizerComputer, aggregator, outRecDesc, false);
    }

    public InMemHybridHashSortELGroupHash(IHyracksCommonContext ctx, int framesLimit, int tableSize, int sortThreshold,
            int runCount, int expectedRecordLength, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputer tpc, INormalizedKeyComputer firstNormalizerComputer,
            IAggregatorDescriptor aggregator, RecordDescriptor outRecDesc, boolean allowMultiEntries)
            throws HyracksDataException {
        this.ctx = ctx;
        this.keys = keys;

        this.framesLimit = framesLimit;
        this.tableSize = tableSize;

        this.comparators = comparators;
        this.partComputer = tpc;

        this.firstNormalizerComputer = firstNormalizerComputer;

        this.frameSize = ctx.getFrameSize();

        // for multiple entries
        this.allowMultipleEntries = allowMultiEntries;

        // calculate the memory requirement 
        int headersFrameCount = getMinimunHeaderSize(tableSize, frameSize);

        minimumContentFrameRequirement = getMinimumContentSize(sortThreshold, runCount, expectedRecordLength, frameSize);

        if (headersFrameCount + minimumContentFrameRequirement + 1 > framesLimit) {
            throw new HyracksDataException("Not enough memory for the in-memory hash table");
        }

        // create the empty byte array for fast header flushing
        this.rawHashBytes = new byte[frameSize];
        for (int i = 0; i < rawHashBytes.length; i++) {
            rawHashBytes[i] = -1;
        }

        // initialize the sort reference
        this.tPointers = new int[(sortThreshold + 1) * runCount * PTR_SIZE];

        this.headers = new ByteBuffer[headersFrameCount];
        for (int i = 0; i < headersFrameCount; i++) {
            this.headers[i] = ctx.allocateFrame();
            resetHeader(i);
        }
        this.contents = new ArrayList<ByteBuffer>();
        for (int i = 0; i < minimumContentFrameRequirement; i++) {
            this.contents.add(ctx.allocateFrame());
        }
        this.currentWorkingFrameIndex = 0;

        this.hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.compHashtableAccessor1 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.compHashtableAccessor2 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);

        this.matchPointer = new TuplePointer();

        this.aggregator = aggregator;
        this.aggregateState = aggregator.createAggregateStates();

        this.internalTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());

        this.internalAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
        this.outputAppender = new FrameTupleAppender(frameSize);

        this.isSorted = false;
        this.sortFlushPosition = 0;
        this.hashFlushFrameIndex = 0;
        this.hashFlushTupleIndex = 0;

        this.sortFrameIndex = 0;
        this.sortTupleIndex = 0;
        this.sortTupleInTotalIndex = 0;

    }

    /**
     * Get the minimum frame requirement of the in-memory hash table, given the parameters about
     * the table and the input data.
     * 
     * @param tableSize
     * @param sortThreshold
     * @param runCount
     * @param expectedRecordLength
     * @param frameSize
     * @return
     */
    public static int getMinimumFrameRequirement(int tableSize, int sortThreshold, int runCount,
            int expectedRecordLength, int frameSize) {
        return getMinimunHeaderSize(tableSize, frameSize)
                + getMinimumContentSize(sortThreshold, runCount, expectedRecordLength, frameSize) + 1;
    }

    private static int getMinimunHeaderSize(int tableSize, int frameSize) {

        int residual = tableSize % frameSize * HASH_REF_LENGTH % frameSize == 0 ? 0 : 1;

        return tableSize / frameSize * HASH_REF_LENGTH + tableSize % frameSize * HASH_REF_LENGTH / frameSize + residual;
    }

    private static int getMinimumContentSize(int sortThreshold, int runCount, int expectedRecordLength, int frameSize) {
        int rtn = (sortThreshold + 1) * runCount * (expectedRecordLength + HASH_REF_LENGTH + INT_SIZE);
        rtn = rtn / frameSize + (rtn % frameSize == 0 ? 0 : 1);
        return rtn;
    }

    public void insert(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
        int entryID = partComputer.partition(accessor, tIndex, tableSize);

        boolean foundMatch = lookup(accessor, tIndex, entryID);

        if (foundMatch) {
            hashtableRecordAccessor.reset(contents.get(matchPointer.frameIndex));
            aggregator.aggregate(accessor, tIndex, hashtableRecordAccessor, matchPointer.tupleIndex, aggregateState);
        } else {
            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilder, accessor, tIndex, aggregateState);
            internalAppender.reset(contents.get(currentWorkingFrameIndex), false);
            if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                    internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                //                currentWorkingFrameIndex++;
                //                if (contents.size() == currentWorkingFrameIndex) {
                //                    if (contents.size() + headers.length + 1 > framesLimit) {
                //                        throw new HyracksDataException("Not enough memory for the in-memory hash table.");
                //                    }
                //                    contents.add(ctx.allocateFrame());
                //                }
                if (allocateFrame()) {
                    internalAppender.reset(contents.get(currentWorkingFrameIndex), true);
                    if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                            internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to insert an aggregation result into an empty frame");
                    }
                } else {
                    throw new HyracksDataException("Not enough memory for the in-memory hash table.");
                }
            }

            // update the hash table reference
            if (matchPointer.frameIndex < 0) {
                // new entry in the hash table
                int headerFrameIndex = getHeaderFrameIndex(entryID);
                int headerFrameOffset = getHeaderFrameOffset(entryID);
                if (headers[headerFrameIndex] == null) {
                    headers[headerFrameIndex] = ctx.allocateFrame();
                    resetHeader(headerFrameIndex);
                }
                headers[headerFrameIndex].putInt(headerFrameOffset, currentWorkingFrameIndex);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, internalAppender.getTupleCount() - 1);
                entriesUsed++;
            } else {
                // hash collision
                hashtableRecordAccessor.reset(contents.get(matchPointer.frameIndex));
                int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                contents.get(matchPointer.frameIndex).putInt(refOffset, currentWorkingFrameIndex);
                contents.get(matchPointer.frameIndex)
                        .putInt(refOffset + INT_SIZE, internalAppender.getTupleCount() - 1);
            }
            tupleInTable++;
        }
    }

    public void markNewEntry() {
        this.sortFrameIndex = currentWorkingFrameIndex;
        internalAppender.reset(contents.get(currentWorkingFrameIndex), false);
        this.sortTupleIndex = internalAppender.getTupleCount();
        this.sortTupleInTotalIndex = tupleInTable;
    }

    public boolean hasEnoughSpaceForNewEntry() {
        if (allowMultipleEntries) {
            if (!isSorted)
                return framesLimit - 1 - currentWorkingFrameIndex - 1 - headers.length >= minimumContentFrameRequirement;
            else
                return false;
        } else {
            return false;
        }
    }

    private boolean allocateFrame() {
        if (!allowMultipleEntries) {
            return false;
        }
        if (headers.length + 1 + currentWorkingFrameIndex + 1 < framesLimit) {
            currentWorkingFrameIndex++;
            if (contents.size() - 1 < currentWorkingFrameIndex) {
                contents.add(ctx.allocateFrame());
            }
            return true;
        }
        return false;
    }

    public boolean lookup(FrameTupleAccessor accessor, int tIndex, int entryID) {
        long timer = System.currentTimeMillis();
        // reset the match pointer
        matchPointer.frameIndex = -1;
        matchPointer.tupleIndex = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entryID);
        int headerFrameOffset = getHeaderFrameOffset(entryID);

        if (headers[headerFrameIndex] == null) {
            hashTime += System.currentTimeMillis() - timer;
            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            matchPointer.frameIndex = entryFrameIndex;
            matchPointer.tupleIndex = entryTupleIndex;
            hashtableRecordAccessor.reset(contents.get(entryFrameIndex));
            if (compare(accessor, tIndex, hashtableRecordAccessor, entryTupleIndex) == 0) {
                hashTime += System.currentTimeMillis() - timer;
                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents.get(prevFrameIndex).getInt(refOffset);
            entryTupleIndex = contents.get(prevFrameIndex).getInt(refOffset + INT_SIZE);
        }
        hashTime += System.currentTimeMillis() - timer;
        return false;
    }

    public void sortHashtable() throws HyracksDataException {
        int ptr = 0;
        for (int i = sortFrameIndex; i <= currentWorkingFrameIndex; i++) {
            hashtableRecordAccessor.reset(contents.get(i));
            int tupleCount = hashtableRecordAccessor.getTupleCount();
            int j = 0;
            if (i == sortFrameIndex) {
                j = sortTupleIndex;
            }
            for (; j < tupleCount; j++) {
                tPointers[ptr * PTR_SIZE] = i;
                tPointers[ptr * PTR_SIZE + 1] = j;
                int tStart = hashtableRecordAccessor.getTupleStartOffset(j);
                int f0StartRel = hashtableRecordAccessor.getFieldStartOffset(j, keys[0]);
                int f0EndRel = hashtableRecordAccessor.getFieldEndOffset(j, keys[0]);
                int f0Start = f0StartRel + tStart + hashtableRecordAccessor.getFieldSlotsLength();
                tPointers[ptr * PTR_SIZE + 2] = firstNormalizerComputer == null ? 0 : firstNormalizerComputer
                        .normalize(hashtableRecordAccessor.getBuffer().array(), f0Start, f0EndRel - f0StartRel);

                ptr++;
            }
        }

        if (ptr > 0) {
            long timer = System.currentTimeMillis();
            sort(0, ptr);
            sortTime += System.currentTimeMillis() - timer;
        }
        isSorted = true;
    }

    private void sort(int offset, int len) {
        int m = offset + (len >> 1);
        int mFrameIndex = tPointers[m * PTR_SIZE];
        int mTupleIndex = tPointers[m * PTR_SIZE + 1];
        int mNormKey = tPointers[m * PTR_SIZE + 2];
        compHashtableAccessor1.reset(contents.get(mFrameIndex));

        int a = offset;
        int b = a;
        int c = offset + len - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int bFrameIndex = tPointers[b * PTR_SIZE];
                int bTupleIndex = tPointers[b * PTR_SIZE + 1];
                int bNormKey = tPointers[b * PTR_SIZE + 2];
                int cmp = 0;
                if (bNormKey != mNormKey) {
                    cmp = ((((long) bNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    compHashtableAccessor2.reset(contents.get(bFrameIndex));
                    cmp = compare(compHashtableAccessor2, bTupleIndex, compHashtableAccessor1, mTupleIndex);
                }
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(a++, b);
                }
                ++b;
            }
            while (c >= b) {
                int cFrameIndex = tPointers[c * PTR_SIZE];
                int cTupleIndex = tPointers[c * PTR_SIZE + 1];
                int cNormKey = tPointers[c * PTR_SIZE + 2];
                int cmp = 0;
                if (cNormKey != mNormKey) {
                    cmp = ((((long) cNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    compHashtableAccessor2.reset(contents.get(cFrameIndex));
                    cmp = compare(compHashtableAccessor2, cTupleIndex, compHashtableAccessor1, mTupleIndex);
                }
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(c, d--);
                }
                --c;
            }
            if (b > c)
                break;
            swap(b++, c--);
        }

        int s;
        int n = offset + len;
        s = Math.min(a - offset, b - a);
        vecswap(offset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(b, n - s, s);

        if ((s = b - a) > 1) {
            sort(offset, s);
        }
        if ((s = d - c) > 1) {
            sort(n - s, s);
        }
    }

    private void swap(int a, int b) {
        totalSwaps++;
        for (int i = 0; i < PTR_SIZE; i++) {
            int t = tPointers[a * PTR_SIZE + i];
            tPointers[a * PTR_SIZE + i] = tPointers[b * PTR_SIZE + i];
            tPointers[b * PTR_SIZE + i] = t;
        }
    }

    private void vecswap(int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(a, b);
        }
    }

    public int getTupleCount() {
        return tupleInTable;
    }

    public void reset() throws HyracksDataException {

        long resetTime = System.currentTimeMillis();

        // FIXME
        printHashtableStatistics();

        for (int i = 0; i < headers.length; i++) {
            resetHeader(i);
        }
        for (int i = 0; i < contents.size(); i++) {
            resetContent(i);
        }
        this.currentWorkingFrameIndex = 0;
        this.isSorted = false;
        this.sortFlushPosition = 0;
        this.hashFlushFrameIndex = 0;
        this.hashFlushTupleIndex = 0;
        this.aggregateState.reset();

        this.sortFrameIndex = 0;
        this.sortTupleIndex = 0;

        resetTime = System.currentTimeMillis() - resetTime;

        this.tupleInTable = 0;
        this.entriesUsed = 0;
        // this.totalComparisons = 0;
        // this.totalSwaps = 0;
        // this.sortTime = 0;
    }

    private void resetHeader(int headerIndex) {
        headers[headerIndex].position(0);
        headers[headerIndex].put(rawHashBytes);
        headers[headerIndex].position(0);
    }

    private void resetContent(int contentIndex) {
        contents.get(contentIndex).putInt(frameSize - INT_SIZE, 0);
    }

    /**
     * Get the header frame index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderFrameIndex(int entry) {
        int frameIndex = entry / frameSize * HASH_REF_LENGTH + entry % frameSize * HASH_REF_LENGTH / frameSize;
        return frameIndex;
    }

    /**
     * Get the tuple index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderFrameOffset(int entry) {
        int offset = entry % frameSize * HASH_REF_LENGTH % frameSize;
        return offset;
    }

    /**
     * Comparator between the input record and a record in the hash table.
     * 
     * @param accessor
     * @param tupleIndex
     * @param hashAccessor
     * @param hashTupleIndex
     * @return
     */
    private int compare(IFrameTupleAccessor accessor, int tupleIndex, IFrameTupleAccessor hashAccessor,
            int hashTupleIndex) {
        totalComparisons++;
        int tStart0 = accessor.getTupleStartOffset(tupleIndex);
        int fStartOffset0 = accessor.getFieldSlotsLength() + tStart0;

        int tStart1 = hashAccessor.getTupleStartOffset(hashTupleIndex);
        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < keys.length; ++i) {
            int fStart0 = accessor.getFieldStartOffset(tupleIndex, keys[i]);
            int fEnd0 = accessor.getFieldEndOffset(tupleIndex, keys[i]);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = hashAccessor.getFieldStartOffset(hashTupleIndex, keys[i]);
            int fEnd1 = hashAccessor.getFieldEndOffset(hashTupleIndex, keys[i]);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0, hashAccessor
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private void printHashtableStatistics() {

        if ((double) (tupleInTable / entriesUsed) > maxHashtableRatio) {
            maxHashtableRatio = (double) (tupleInTable / entriesUsed);
            LOGGER.warning(InMemHybridHashSortELGroupHash.class.getSimpleName() + "-hashtableStats\t" + tableSize
                    + "\t" + entriesUsed + "\t" + tupleInTable);
        }
    }

    @Override
    public void open() throws HyracksDataException {
        // do nothing
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {

        if (!isSorted || sortFlushPosition + sortTupleInTotalIndex >= tupleInTable) {
            return false;
        }

        outputAppender.reset(buffer, true);

        while (true) {
            int fIndex, tIndex;
            fIndex = tPointers[sortFlushPosition * PTR_SIZE];
            tIndex = tPointers[sortFlushPosition * PTR_SIZE + 1];
            hashtableRecordAccessor.reset(contents.get(fIndex));
            int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(tIndex);
            int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

            outputTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                        + hashtableRecordAccessor.getFieldStartOffset(tIndex, k),
                        hashtableRecordAccessor.getFieldLength(tIndex, k));
            }

            aggregator.outputPartialResult(outputTupleBuilder, hashtableRecordAccessor, tIndex, aggregateState);
            if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(), 0,
                    outputTupleBuilder.getSize())) {
                return true;
            }

            sortFlushPosition++;
            if (sortFlushPosition + sortTupleInTotalIndex >= tupleInTable) {
                break;
            }
        }

        if (outputAppender.getTupleCount() > 0) {
            return true;
        }

        return false;
    }

    public int nextFrameForFinalOutput(ByteBuffer buffer) throws HyracksDataException {
        outputAppender.reset(buffer, false);

        for (int i = hashFlushFrameIndex; i <= currentWorkingFrameIndex; i++) {
            hashtableRecordAccessor.reset(contents.get(i));
            int tIndex;
            if (i == hashFlushFrameIndex) {
                tIndex = hashFlushTupleIndex;
            } else {
                tIndex = 0;
            }
            while (tIndex < hashtableRecordAccessor.getTupleCount()) {

                if (isSorted) {
                    if (i == sortFrameIndex && tIndex == sortTupleIndex) {
                        // reached the sorted record
                        if (outputAppender.getTupleCount() > 0) {
                            return 0;
                        } else {
                            return -1;
                        }
                    }
                }

                int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(tIndex);
                int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                outputTupleBuilder.reset();
                for (int k = 0; k < keys.length; k++) {
                    outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + hashtableRecordAccessor.getFieldStartOffset(tIndex, k),
                            hashtableRecordAccessor.getFieldLength(tIndex, k));
                }

                aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, tIndex, aggregateState);
                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    hashFlushFrameIndex = i;
                    hashFlushTupleIndex = tIndex;
                    return 1;
                }
                tIndex++;
            }
        }

        if (outputAppender.getTupleCount() > 0) {
            return 0;
        }

        return -1;
    }

    @Override
    public void close() throws HyracksDataException {
        aggregateState.close();
        // FIXME
        LOGGER.warning(InMemHybridHashSortELGroupHash.class.getSimpleName() + "-close\t" + sortTime + "\t" + hashTime
                + "\t" + totalComparisons + "\t" + totalSwaps);
    }

}
