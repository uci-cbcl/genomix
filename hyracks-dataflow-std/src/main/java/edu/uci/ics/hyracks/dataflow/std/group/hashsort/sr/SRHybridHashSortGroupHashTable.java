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
package edu.uci.ics.hyracks.dataflow.std.group.hashsort.sr;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
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
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.struct.HashArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.sort.BSTMemMgr;
import edu.uci.ics.hyracks.dataflow.std.sort.IMemoryManager;
import edu.uci.ics.hyracks.dataflow.std.sort.Slot;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class SRHybridHashSortGroupHashTable implements ISerializableGroupHashTable {

    private static final int INT_SIZE = 4;
    private static final int INIT_REF_COUNT = 8;
    private static final int PTR_SIZE = 3;

    private final int tableSize, framesLimit, frameSize;

    private final ByteBuffer[] headers;

    private final IHyracksTaskContext ctx;

    private int totalTupleCount;

    private final IAggregatorDescriptor aggregator;
    private final AggregateState aggState;

    private final int[] keys, internalKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer tpc;

    private final INormalizedKeyComputer firstNormalizer;

    private ByteBuffer outputBuffer;

    private LinkedList<RunFileReader> runReaders;

    private TuplePointer matchPointer;

    /**
     * Tuple builder for hash table insertion
     */
    private final ArrayTupleBuilder outputTupleBuilder;

    private final HashArrayTupleBuilder internalTupleBuilder;

    /**
     * pointers for sort records in an entry
     */
    private int[] tPointers;

    private IMemoryManager memMgr;

    private Slot allocationPtr, outputedTuple;

    private final RecordDescriptor outRecDesc;

    private int entryIndexToFlush;
    private RunFileWriter runWriter = null;

    private final FrameTupleAppender outputAppender;

    private final static Logger LOGGER = Logger.getLogger(SRHybridHashSortGroupHashTable.class.getSimpleName());

    int spilledGroups = 0, insertGroups = 0, insertedRecords = 0;
    long comparsionCount = 0, swapCount = 0, hashTimer = 0, sortTimer = 0, actualSortTimer = 0, memAllocTime = 0,
            memUnAllocTime = 0;

    int runSizeInTuple = 0, runSizeInFrame = 0;

    int usedEntries = 0;

    public SRHybridHashSortGroupHashTable(IHyracksTaskContext ctx, int framesLimit, int tableSize, int[] keys,
            IBinaryComparator[] comparators, ITuplePartitionComputer tpc,
            INormalizedKeyComputer firstNormalizerComputer, IAggregatorDescriptor aggregator,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc) {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.frameSize = ctx.getFrameSize();
        this.framesLimit = framesLimit;

        this.outRecDesc = outRecDesc;

        this.keys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.aggregator = aggregator;
        this.aggState = aggregator.createAggregateStates();

        this.tpc = tpc;
        this.comparators = comparators;
        this.firstNormalizer = firstNormalizerComputer;

        // initialize the hash table
        int residual = tableSize % frameSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        this.headers = new ByteBuffer[tableSize / frameSize * INT_SIZE * 2 + tableSize % frameSize * INT_SIZE * 2
                / frameSize + residual];

        this.outputBuffer = ctx.allocateFrame();

        this.memMgr = new BSTMemMgr(ctx, framesLimit - 1 - headers.length);

        this.totalTupleCount = 0;

        this.runReaders = new LinkedList<RunFileReader>();

        this.internalTupleBuilder = new HashArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());

        this.matchPointer = new TuplePointer();

        this.entryIndexToFlush = 0;

        this.outputAppender = new FrameTupleAppender(frameSize);

        this.outputedTuple = new Slot();
        this.allocationPtr = new Slot();
    }

    /**
     * Get the header frame index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderFrameIndex(int entry) {
        int frameIndex = entry / frameSize * 2 * INT_SIZE + entry % frameSize * 2 * INT_SIZE / frameSize;
        return frameIndex;
    }

    /**
     * Get the tuple index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderTupleIndex(int entry) {
        int offset = entry % frameSize * 2 * INT_SIZE % frameSize;
        return offset;
    }

    /**
     * Reset the header page
     * 
     * @param headerFrameIndex
     */
    private void resetHeader(int headerFrameIndex) {
        for (int i = 0; i < frameSize; i += INT_SIZE) {
            headers[headerFrameIndex].putInt(i, -1);
        }
    }

    private int getTupleLength(int frameIndex, int frameOffset) {
        return outRecDesc.getFieldCount() * INT_SIZE
                + memMgr.getFrame(frameIndex).getInt(frameOffset + (outRecDesc.getFieldCount() - 1) * INT_SIZE);
    }

    private int getTupleHashReference(int frameIndex, int frameOffset) {
        return frameOffset + getTupleLength(frameIndex, frameOffset);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#insert(edu.uci.ics.hyracks.dataflow
     * .common.comm.io.FrameTupleAccessor, int)
     */
    @Override
    public void insert(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {
        int entry = tpc.partition(accessor, tupleIndex, tableSize);

        long timer = System.currentTimeMillis();
        boolean foundMatch = findMatch(entry, accessor, tupleIndex);
        hashTimer += System.currentTimeMillis() - timer;

        if (foundMatch) {
            // find match; do aggregation
            aggregator
                    .aggregate(accessor, tupleIndex, memMgr.getFrame(matchPointer.frameIndex).array(),
                            matchPointer.tupleIndex, getTupleLength(matchPointer.frameIndex, matchPointer.tupleIndex),
                            aggState);
        } else {
            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilder, accessor, tupleIndex, aggState);
            internalTupleBuilder.addHashReference(-1, -1);
            allocationPtr.clear();
            int slotRequirement = internalTupleBuilder.getSize() + internalTupleBuilder.getFieldEndOffsets().length
                    * INT_SIZE;

            long allocTimer = System.currentTimeMillis();
            memMgr.allocate(slotRequirement, allocationPtr);
            memAllocTime += System.currentTimeMillis() - allocTimer;

            while (allocationPtr.isNull()) {
                int recycleSlotLength = -1;

                while (recycleSlotLength < slotRequirement) {
                    int firstEntryIndexToFlush = entryIndexToFlush;
                    recycleSlotLength = flushEntries();

                    if (recycleSlotLength < 0) {
                        throw new HyracksDataException(
                                "Failed to recycle space for new insertion: not space after recycling");
                    }

                    if ((firstEntryIndexToFlush <= entry && (entryIndexToFlush > entry || entryIndexToFlush < firstEntryIndexToFlush))
                            || (firstEntryIndexToFlush > entry && entryIndexToFlush < firstEntryIndexToFlush && entryIndexToFlush > entry)) {
                        matchPointer.frameIndex = -1;
                        matchPointer.tupleIndex = -1;
                    }

                }

                if (recycleSlotLength < slotRequirement) {
                    throw new HyracksDataException(
                            "Failed to recycle space for new insertion: not enough space after recycling");
                }

                allocTimer = System.currentTimeMillis();
                memMgr.allocate(slotRequirement, allocationPtr);
                memAllocTime += System.currentTimeMillis() - allocTimer;
            }
            memMgr.writeTuple(allocationPtr.getFrameIx(), allocationPtr.getOffset(),
                    internalTupleBuilder.getFieldEndOffsets(), internalTupleBuilder.getByteArray(), 0,
                    internalTupleBuilder.getSize());

            // update reference
            if (matchPointer.frameIndex < 0) {
                // first record for this entry; update the header references
                int headerFrameIndex = getHeaderFrameIndex(entry);
                int headerFrameOffset = getHeaderTupleIndex(entry);
                if (headers[headerFrameIndex] == null) {
                    headers[headerFrameIndex] = ctx.allocateFrame();
                    resetHeader(headerFrameIndex);
                }
                headers[headerFrameIndex].putInt(headerFrameOffset, allocationPtr.getFrameIx());
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE,
                        memMgr.getTupleStartOffset(allocationPtr.getFrameIx(), allocationPtr.getOffset()));
                usedEntries++;
            } else {
                // update the previous reference
                int refOffset = getTupleHashReference(matchPointer.frameIndex, matchPointer.tupleIndex);
                memMgr.getFrame(matchPointer.frameIndex).putInt(refOffset, allocationPtr.getFrameIx());
                memMgr.getFrame(matchPointer.frameIndex).putInt(refOffset + INT_SIZE,
                        memMgr.getTupleStartOffset(allocationPtr.getFrameIx(), allocationPtr.getOffset()));
            }

            insertGroups++;
            totalTupleCount++;
        }
        insertedRecords++;
    }

    private boolean findMatch(int entry, IFrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {
        // reset the match pointer
        matchPointer.frameIndex = -1;
        matchPointer.tupleIndex = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryFrameOffset = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            matchPointer.frameIndex = entryFrameIndex;
            matchPointer.tupleIndex = entryFrameOffset;

            if (compare(accessor, tupleIndex, memMgr.getFrame(entryFrameIndex).array(), entryFrameOffset) == 0) {
                return true;
            }

            // move to the next record along the linked list
            int refOffset = getTupleHashReference(entryFrameIndex, entryFrameOffset);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = memMgr.getFrame(prevFrameIndex).getInt(refOffset);
            entryFrameOffset = memMgr.getFrame(prevFrameIndex).getInt(refOffset + INT_SIZE);
        }

        return false;
    }

    private int flushEntries() throws HyracksDataException {

        int recycleSlotLength = -1;

        // start flush entry
        int flushedEntryCount = 0;
        while (true) {
            long timer = System.currentTimeMillis();
            int tupleInEntry = sortEntry(entryIndexToFlush);
            sortTimer += System.currentTimeMillis() - timer;

            spilledGroups += tupleInEntry;

            if (tupleInEntry > 0) {
                if (runWriter == null) {
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                ExternalGroupOperatorDescriptor.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    runWriter = new RunFileWriter(runFile, ctx.getIOManager());
                    runWriter.open();
                    outputAppender.reset(outputBuffer, true);
                }
            }
            // FIXME
            runSizeInTuple += tupleInEntry;
            if (tupleInEntry > 0) {
                usedEntries--;
            }
            for (int ptr = 0; ptr < tupleInEntry; ptr++) {
                int frameIndex = tPointers[ptr * PTR_SIZE];
                int frameOffset = tPointers[ptr * PTR_SIZE + 1];

                outputTupleBuilder.reset();

                int tupleLength = getTupleLength(frameIndex, frameOffset);
                int fieldSlotOffset = outRecDesc.getFieldCount() * INT_SIZE;

                for (int k = 0; k < internalKeys.length; k++) {
                    int fieldOffset = k == 0 ? 0 : memMgr.getFrame(frameIndex).getInt(frameOffset + (k - 1) * INT_SIZE);
                    int fieldLength = memMgr.getFrame(frameIndex).getInt(frameOffset + k * INT_SIZE) - fieldOffset;
                    outputTupleBuilder.addField(memMgr.getFrame(frameIndex).array(), frameOffset + fieldSlotOffset
                            + fieldOffset, fieldLength);
                }

                aggregator.outputPartialResult(outputTupleBuilder, memMgr.getFrame(frameIndex).array(), frameOffset,
                        tupleLength, outRecDesc.getFieldCount(), INT_SIZE, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, runWriter);
                    // FIXME
                    runSizeInFrame++;
                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to write aggregation result to a run file");
                    }
                }
                outputedTuple.set(frameIndex, memMgr.getSlotStartOffset(frameIndex, frameOffset));

                long deallocTimer = System.currentTimeMillis();
                int recycled = memMgr.unallocate(outputedTuple);
                memUnAllocTime += System.currentTimeMillis() - deallocTimer;

                if (recycled > recycleSlotLength) {
                    recycleSlotLength = recycled;
                }
            }

            // reset headers

            int headerFrameIndex = getHeaderFrameIndex(entryIndexToFlush);
            int headerFrameOffset = getHeaderTupleIndex(entryIndexToFlush);

            if (headers[headerFrameIndex] != null) {
                headers[headerFrameIndex].putInt(headerFrameOffset, -1);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, -1);
            }

            entryIndexToFlush++;
            // reset run file if all buckets have been flushed
            if (entryIndexToFlush == tableSize) {
                if (outputAppender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outputBuffer, runWriter);
                    outputAppender.reset(outputBuffer, true);
                    runSizeInFrame++;
                }
                // FIXME
                printHashtableStatistics();
                LOGGER.warning(SRHybridHashSortGroupHashTable.class.getSimpleName() + "-flush\t" + runSizeInFrame
                        + "\t" + runSizeInTuple + "\t" + insertedRecords);

                if (runWriter != null) {
                    runWriter.close();
                    runReaders.add(runWriter.createReader());
                    runWriter = null;
                    runSizeInFrame = 0;
                    runSizeInTuple = 0;
                }
                entryIndexToFlush = 0;
            }
            // remove the flushed records from the tuple count
            totalTupleCount -= tupleInEntry;
            // break if one non-empty entry has been recycled
            if (tupleInEntry > 0) {
                break;
            }
            flushedEntryCount++;
            if (flushedEntryCount == tableSize || totalTupleCount <= 0) {
                // at most flush all table entries
                break;
            }
        }

        return recycleSlotLength;
    }

    /**
     * Sort the given entry. If there is no record in this entry,
     * no sort is processed.
     * 
     * @param entryID
     * @return
     */
    private int sortEntry(int entryID) {
        if (tPointers == null)
            tPointers = new int[INIT_REF_COUNT * PTR_SIZE];
        int ptr = 0;

        int headerFrameIndex = getHeaderFrameIndex(entryID);
        int headerFrameOffset = getHeaderTupleIndex(entryID);

        if (headers[headerFrameIndex] == null) {
            return 0;
        }

        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryFrameOffset = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        do {
            if (entryFrameIndex < 0) {
                break;
            }
            tPointers[ptr * PTR_SIZE] = entryFrameIndex;
            tPointers[ptr * PTR_SIZE + 1] = entryFrameOffset;
            int f0StartRel = internalKeys[0] == 0 ? 0 : getInt(memMgr.getFrame(entryFrameIndex).array(),
                    entryFrameOffset + outRecDesc.getFieldCount() * INT_SIZE + (internalKeys[0] - 1) * INT_SIZE);
            int f0EndRel = getInt(memMgr.getFrame(entryFrameIndex).array(),
                    entryFrameOffset + outRecDesc.getFieldCount() * INT_SIZE + internalKeys[0] * INT_SIZE);
            int f0Start = f0StartRel + entryFrameOffset + outRecDesc.getFieldCount() * INT_SIZE;
            tPointers[ptr * PTR_SIZE + 2] = firstNormalizer == null ? 0 : firstNormalizer.normalize(
                    memMgr.getFrame(entryFrameIndex).array(), f0Start, f0EndRel - f0StartRel);

            ptr++;

            if (ptr * PTR_SIZE >= tPointers.length) {
                int[] newTPointers = new int[tPointers.length * 2];
                System.arraycopy(tPointers, 0, newTPointers, 0, tPointers.length);
                tPointers = newTPointers;
            }

            // move to the next record
            int refOffset = getTupleHashReference(entryFrameIndex, entryFrameOffset);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = memMgr.getFrame(prevFrameIndex).getInt(refOffset);
            entryFrameOffset = memMgr.getFrame(prevFrameIndex).getInt(refOffset + INT_SIZE);

        } while (true);

        // sort records
        if (ptr > 0) {
            long timer = System.currentTimeMillis();
            sort(0, ptr);
            actualSortTimer += System.currentTimeMillis() - timer;
        }

        return ptr;
    }

    private void sort(int offset, int len) {
        int m = offset + (len >> 1);
        int mFrameIndex = tPointers[m * PTR_SIZE];
        int mFrameOffset = tPointers[m * PTR_SIZE + 1];
        int mNormKey = tPointers[m * PTR_SIZE + 2];
        int mLength = getTupleLength(mFrameIndex, mFrameOffset);

        int a = offset;
        int b = a;
        int c = offset + len - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int bFrameIndex = tPointers[b * PTR_SIZE];
                int bFrameOffset = tPointers[b * PTR_SIZE + 1];
                int bNormKey = tPointers[b * PTR_SIZE + 2];
                int bLength = getTupleLength(bFrameIndex, bFrameOffset);
                int cmp = 0;
                if (bNormKey != mNormKey) {
                    cmp = ((((long) bNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    cmp = compare(memMgr.getFrame(bFrameIndex).array(), bFrameOffset, bLength,
                            memMgr.getFrame(mFrameIndex).array(), mFrameOffset, mLength);
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
                int cFrameOffset = tPointers[c * PTR_SIZE + 1];
                int cNormKey = tPointers[c * PTR_SIZE + 2];
                int cLength = getTupleLength(cFrameIndex, cFrameOffset);
                int cmp = 0;
                if (cNormKey != mNormKey) {
                    cmp = ((((long) cNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                } else {
                    cmp = compare(memMgr.getFrame(cFrameIndex).array(), cFrameOffset, cLength,
                            memMgr.getFrame(mFrameIndex).array(), mFrameOffset, mLength);
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
        swapCount++;
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

    @Override
    public void reset() {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                resetHeader(i);
            }
        }

        usedEntries = 0;
        totalTupleCount = 0;
        insertedRecords = 0;
    }

    @Override
    public void flushHashtableToOutput(IFrameWriter outputWriter) throws HyracksDataException {

        // FIXME
        printHashtableStatistics();

        outputAppender.reset(outputBuffer, true);

        // flush the content of hash table to the output
        for (int entryID = 0; entryID < tableSize; entryID++) {

            int headerFrameIndex = getHeaderFrameIndex(entryID);
            int headerFrameOffset = getHeaderTupleIndex(entryID);

            if (headers[headerFrameIndex] == null) {
                continue;
            }

            spilledGroups++;

            int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
            int entryFrameOffset = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

            do {
                if (entryFrameIndex < 0) {
                    break;
                }

                outputTupleBuilder.reset();

                int tupleLength = getTupleLength(entryFrameIndex, entryFrameOffset);
                int fieldSlotOffset = outRecDesc.getFieldCount() * INT_SIZE;

                for (int k = 0; k < internalKeys.length; k++) {
                    int fieldOffset = k == 0 ? 0 : memMgr.getFrame(entryFrameIndex).getInt(
                            entryFrameOffset + (k - 1) * INT_SIZE);
                    int fieldLength = memMgr.getFrame(entryFrameIndex).getInt(entryFrameOffset + k * INT_SIZE)
                            - fieldOffset;
                    outputTupleBuilder.addField(memMgr.getFrame(entryFrameIndex).array(), entryFrameOffset
                            + fieldSlotOffset + fieldOffset, fieldLength);
                }

                aggregator.outputPartialResult(outputTupleBuilder, memMgr.getFrame(entryFrameIndex).array(),
                        entryFrameOffset, tupleLength, outRecDesc.getFieldCount(), INT_SIZE, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to write aggregation result to a run file");
                    }
                }

                // move to the next record
                int refOffset = getTupleHashReference(entryFrameIndex, entryFrameOffset);
                int prevFrameIndex = entryFrameIndex;
                entryFrameIndex = memMgr.getFrame(prevFrameIndex).getInt(refOffset);
                entryFrameOffset = memMgr.getFrame(prevFrameIndex).getInt(refOffset + INT_SIZE);

            } while (true);
        }
        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
            outputAppender.reset(outputBuffer, true);
        }
    }

    @Override
    public void finishup() throws HyracksDataException {
        // If there are still records not flushed, flush a new run file
        if (runWriter != null && totalTupleCount > 0) {
            do {
                flushEntries();
                if (runWriter == null)
                    break;
            } while (true);
        }
        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, runWriter);
            outputAppender.reset(outputBuffer, true);
        }
        if (runWriter != null) {
            runWriter.close();
            runReaders.add(runWriter.createReader());
            runWriter = null;
        }

        LOGGER.warning("SRHybridHashSortHashTable-Finishup\t" + insertGroups + "\t" + spilledGroups + "\t"
                + headers.length + "\t" + comparsionCount + "\t" + swapCount + "\t" + runReaders.size() + "\t"
                + hashTimer + "\t" + sortTimer + "\t" + actualSortTimer + "\t" + memAllocTime + "\t" + memUnAllocTime);
    }

    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < headers.length; i++) {
            headers[i] = null;
        }
        memMgr.close();
        outputBuffer = null;
    }

    @Override
    public int getTupleCount() {
        return totalTupleCount;
    }

    @Override
    public int getFrameSize() {
        return framesLimit;
    }

    @Override
    public LinkedList<RunFileReader> getRunFileReaders() {
        return runReaders;
    }

    private int compare(IFrameTupleAccessor accessor, int tupleIndex, byte[] data, int offset) {
        comparsionCount++;
        int tStart0 = accessor.getTupleStartOffset(tupleIndex);
        int fStartOffset0 = accessor.getFieldSlotsLength() + tStart0;

        int tStart1 = offset;
        int fStartOffset1 = outRecDesc.getFieldCount() * INT_SIZE + tStart1;

        for (int i = 0; i < keys.length; i++) {
            int fStart0 = accessor.getFieldStartOffset(tupleIndex, keys[i]);
            int fEnd0 = accessor.getFieldEndOffset(tupleIndex, keys[i]);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = internalKeys[i] == 0 ? 0 : getInt(data, tStart1 + (internalKeys[i] - 1) * INT_SIZE);
            int fEnd1 = getInt(data, tStart1 + internalKeys[i] * INT_SIZE);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0, data, fStart1
                    + fStartOffset1, fLen1);

            if (c != 0) {
                return c;
            }
        }

        return 0;
    }

    private int compare(byte[] data0, int offset0, int len0, byte[] data1, int offset1, int len1) {
        comparsionCount++;
        int fStartOffset0 = outRecDesc.getFieldCount() * INT_SIZE + offset0;

        int fStartOffset1 = outRecDesc.getFieldCount() * INT_SIZE + offset1;

        for (int i = 0; i < internalKeys.length; i++) {
            int fStart0 = internalKeys[i] == 0 ? 0 : getInt(data0, offset0 + (internalKeys[i] - 1) * INT_SIZE);
            int fEnd0 = getInt(data0, offset0 + internalKeys[i] * INT_SIZE);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = internalKeys[i] == 0 ? 0 : getInt(data1, offset1 + (internalKeys[i] - 1) * INT_SIZE);
            int fEnd1 = getInt(data1, offset1 + internalKeys[i] * INT_SIZE);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i]
                    .compare(data0, fStart0 + fStartOffset0, fLen0, data1, fStart1 + fStartOffset1, fLen1);

            if (c != 0) {
                return c;
            }
        }

        return 0;
    }

    private int getInt(byte[] data, int offset) {
        return ((data[offset] & 0xff) << 24) | ((data[offset + 1] & 0xff) << 16) | ((data[offset + 2] & 0xff) << 8)
                | (data[offset + 3] & 0xff);
    }

    private void printHashtableStatistics() {
        LOGGER.warning(SRHybridHashSortGroupHashTable.class.getSimpleName() + "-HashtableStatistics\t" + tableSize
                + "\t" + usedEntries + "\t" + totalTupleCount + "\t" + insertedRecords);
        //        if (usedEntries != 0 && totalTupleCount / usedEntries >= 2) {
        //            // print the hash table distribution
        //            long startTimers = System.currentTimeMillis();
        //            StringBuilder sbder = new StringBuilder();
        //            for (int i = 0; i < tableSize; i++) {
        //                int cnt = getEntryLength(i);
        //                if (cnt > 0)
        //                    sbder.append(i).append(" ").append(cnt).append("\n");
        //            }
        //            LOGGER.warning("HashtableHistogram\t" + (System.currentTimeMillis() - startTimers) + "\n"
        //                    + sbder.toString());
        //        }
    }

    private int getEntryLength(int entry) {

        int cnt = 0;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            return cnt;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryFrameOffset = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            // Move to the next record in this entry following the linked list
            int refOffset = getTupleHashReference(entryFrameIndex, entryFrameOffset);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = memMgr.getFrame(prevFrameIndex).getInt(refOffset);
            entryFrameOffset = memMgr.getFrame(prevFrameIndex).getInt(refOffset + INT_SIZE);
            cnt++;
        }

        return cnt;
    }

}
