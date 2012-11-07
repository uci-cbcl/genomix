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
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
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
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAccessorForAdjustableFrame;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAccessorForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAppenderForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class InMemHybridHashSortELMergeHashTable {

    private static final int INT_SIZE = 4;
    private static final int PTR_SIZE = 3;

    private final int tableSize, framesLimit, frameSize;

    private final ByteBuffer[] headers;
    private final ByteBuffer[] contents;

    private final byte[] emptyBufContent;

    private final IHyracksTaskContext ctx;

    private int currentLargestFrameIndex;

    private final IAggregatorDescriptor aggregator;
    private final AggregateState aggState;

    private final int[] keys, internalKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer tpc;

    private final INormalizedKeyComputer firstNormalizer;

    private ByteBuffer outputBuffer;

    private TuplePointer matchPointer;

    private final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    // for sort
    private final FrameTupleAccessorForGroupHashtable compFrameAccessor1, compFrameAccessor2;

    // for hash table tuple insertion
    private final FrameTupleAppenderForGroupHashtable internalAppender;

    // for output record
    private final FrameTupleAppender outputAppender;

    /**
     * Tuple builder for hash table insertion
     */
    private final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;

    /**
     * pointers for sort records in an entry
     */
    private final int[] tPointers;

    private ByteBuffer sortedInputBuffer = ByteBuffer.allocate(8);
    private FrameTupleAccessorForAdjustableFrame sortedTupleAccessor;

    private int pointerFlushOffset = -1;
    private int sortedRecordCount;

    private final IFrameWriter outputWriter;

    private int currentWorkingEntryID = -1;

    private int frameIndexForBeginningOfCurrentWorkingEntry = 0;
    private int tupleIndexForBeginningOfCurrentWorkingEntry = 0;

    /*
     * tuples inserted into the table (i.e., # of records currently in the hash table)
     */
    private int tupleInserted = 0, entriesUsed = 0;

    private static final Logger LOGGER = Logger.getLogger(InMemHybridHashSortELMergeHashTable.class.getSimpleName());

    private int aggregatedCounter = 0, hashCollisionCount = 0;

    private long totalComparison = 0, resetTimer = 0, hashTimer = 0;

    private long procTimer = System.currentTimeMillis();

    public InMemHybridHashSortELMergeHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int sortThreshold, int runCount, int[] keys, IBinaryComparator[] comparators, ITuplePartitionComputer tpc,
            INormalizedKeyComputer firstNormalizerComputer, IAggregatorDescriptor aggregator,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, IFrameWriter outputWriter)
            throws HyracksDataException {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.framesLimit = frameLimits;
        this.frameSize = ctx.getFrameSize();

        this.emptyBufContent = new byte[frameSize];
        for (int i = 0; i < emptyBufContent.length; i++) {
            this.emptyBufContent[i] = -1;
        }

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

        this.contents = new ByteBuffer[framesLimit - 1 - headers.length];
        this.currentLargestFrameIndex = -1;

        this.hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.compFrameAccessor1 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.compFrameAccessor2 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);

        this.internalTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.internalAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
        this.outputAppender = new FrameTupleAppender(frameSize);

        this.matchPointer = new TuplePointer();

        this.tPointers = new int[PTR_SIZE * (sortThreshold + 1) * runCount];

        this.outputWriter = outputWriter;
        this.sortedTupleAccessor = new FrameTupleAccessorForAdjustableFrame(outRecDesc);
    }

    /**
     * Reset the header page
     * 
     * @param headerFrameIndex
     */
    private void resetHeader(int headerFrameIndex) {
        //        for (int i = 0; i < frameSize; i += INT_SIZE) {
        //            headers[headerFrameIndex].putInt(i, -1);
        //        }
        headers[headerFrameIndex].position(0);
        headers[headerFrameIndex].put(emptyBufContent);
        headers[headerFrameIndex].position(0);
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

    public void insert(FrameTupleAccessor accessor, int tupleIndex, int currentEntryID) throws HyracksDataException {
        int entry = tpc.partition(accessor, tupleIndex, tableSize);

        long timer = System.currentTimeMillis();
        boolean foundMatch = findMatch(entry, accessor, tupleIndex);
        hashTimer += System.currentTimeMillis() - timer;

        if (foundMatch) {
            // find match; do aggregation
            hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
            aggregator.aggregate(accessor, tupleIndex, hashtableRecordAccessor, matchPointer.tupleIndex, aggState);
            aggregatedCounter++;
        } else {
            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilder, accessor, tupleIndex, aggState);
            int insertFrameIndex = -1, insertTupleIndex = -1;
            boolean inserted = false;

            if (currentLargestFrameIndex < 0) {
                currentLargestFrameIndex = 0;
            }

            if (contents[currentLargestFrameIndex] == null) {
                contents[currentLargestFrameIndex] = ctx.allocateFrame();
            }

            internalAppender.reset(contents[currentLargestFrameIndex], false);
            if (internalAppender.append(internalTupleBuilder.getFieldEndOffsets(), internalTupleBuilder.getByteArray(),
                    0, internalTupleBuilder.getSize())) {
                inserted = true;
                insertFrameIndex = currentLargestFrameIndex;
                insertTupleIndex = internalAppender.getTupleCount() - 1;
            }

            if (!inserted && currentLargestFrameIndex < contents.length - 1) {
                currentLargestFrameIndex++;
                if (contents[currentLargestFrameIndex] == null) {
                    contents[currentLargestFrameIndex] = ctx.allocateFrame();
                }
                internalAppender.reset(contents[currentLargestFrameIndex], true);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to insert an aggregation value.");
                } else {
                    insertFrameIndex = currentLargestFrameIndex;
                    insertTupleIndex = internalAppender.getTupleCount() - 1;
                    inserted = true;
                }
            }

            // memory is full
            if (!inserted) {
                throw new HyracksDataException("Not enough memory for the in-memory merging hash table.");
            }

            // update the currentEntryID to maintain the pointer to start the sorting, if necessary
            if (currentEntryID != currentWorkingEntryID) {
                currentWorkingEntryID = currentEntryID;
                frameIndexForBeginningOfCurrentWorkingEntry = insertFrameIndex;
                tupleIndexForBeginningOfCurrentWorkingEntry = insertTupleIndex;
            }

            // no match; new insertion
            if (matchPointer.frameIndex < 0) {
                // first record for this entry; update the header references
                int headerFrameIndex = getHeaderFrameIndex(entry);
                int headerFrameOffset = getHeaderTupleIndex(entry);
                if (headers[headerFrameIndex] == null) {
                    headers[headerFrameIndex] = ctx.allocateFrame();
                    resetHeader(headerFrameIndex);
                }
                headers[headerFrameIndex].putInt(headerFrameOffset, insertFrameIndex);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, insertTupleIndex);
                entriesUsed++;
            } else {
                // update the previous reference
                hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                contents[matchPointer.frameIndex].putInt(refOffset, insertFrameIndex);
                contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE, insertTupleIndex);
                hashCollisionCount++;
            }
            tupleInserted++;
        }
    }

    /**
     * Flush the whole in-memory hash table into the output writer.
     * 
     * @param outputWriter
     * @throws HyracksDataException
     */
    public void flushHashtableToOutput(IFrameWriter outputWriter) throws HyracksDataException {

        // FIXME
        printHashtableStatistics();

        outputAppender.reset(outputBuffer, false);
        for (int i = 0; i < contents.length; i++) {
            if (contents[i] == null) {
                continue;
            }
            hashtableRecordAccessor.reset(contents[i]);
            int tupleCount = hashtableRecordAccessor.getTupleCount();
            for (int j = 0; j < tupleCount; j++) {
                outputTupleBuilder.reset();

                int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(j);
                int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + hashtableRecordAccessor.getFieldStartOffset(j, k),
                            hashtableRecordAccessor.getFieldLength(j, k));
                }

                aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, j, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }
            }
        }
    }

    private boolean findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex) {

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
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            matchPointer.frameIndex = entryFrameIndex;
            matchPointer.tupleIndex = entryTupleIndex;
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            if (compare(accessor, tupleIndex, hashtableRecordAccessor, entryTupleIndex) == 0) {
                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
        }

        return false;
    }

    /**
     * Build a reference list for sorting the current working entry
     */
    public void buildSortRefs() {
        int ptr = 0;

        for (int i = frameIndexForBeginningOfCurrentWorkingEntry; i <= currentLargestFrameIndex; i++) {
            if (contents[i] == null) {
                LOGGER.severe("Frames in contents are not supposed to be skipped during the allocation! Please check the code!");
                continue;
            }
            hashtableRecordAccessor.reset(contents[i]);
            int tupleCount = hashtableRecordAccessor.getTupleCount();
            int j = (i == frameIndexForBeginningOfCurrentWorkingEntry) ? tupleIndexForBeginningOfCurrentWorkingEntry
                    : 0;
            for (; j < tupleCount; j++) {
                tPointers[ptr * PTR_SIZE] = i;
                tPointers[ptr * PTR_SIZE + 1] = j;
                int tStart = hashtableRecordAccessor.getTupleStartOffset(j);
                int f0StartRel = hashtableRecordAccessor.getFieldStartOffset(j, internalKeys[0]);
                int f0EndRel = hashtableRecordAccessor.getFieldEndOffset(j, internalKeys[0]);
                int f0Start = f0StartRel + tStart + hashtableRecordAccessor.getFieldSlotsLength();
                tPointers[ptr * PTR_SIZE + 2] = firstNormalizer == null ? 0 : firstNormalizer.normalize(
                        hashtableRecordAccessor.getBuffer().array(), f0Start, f0EndRel - f0StartRel);

                ptr++;
            }
        }

        if (ptr > 0) {
            sort(0, ptr);
        }
        sortedRecordCount = ptr;
        pointerFlushOffset = 0;
    }

    /**
     * insert record into the sorted list, and flush records when necessary.
     * <p/>
     * an in-memory byte array is used to maintain the aggregation result currently working on, so it is guaranteed to
     * be the record with the smallest key value in the current group.
     * <p/>
     * When a new record is inserted, it will be compared with the record in the in-memory byte array to see whether it
     * can be aggregated (it has the same grouping key as the one in the in-memory byte array). Note that the inserted
     * record will never be smaller than the in-memory record. So if the inserted record has larger key than the
     * in-memory record, the in-memory one can be popped and flushed. And all records of the same entry with smaller key
     * value can also be directly flushed.
     * 
     * @param accessor
     * @param tupleIndex
     * @throws HyracksDataException
     */
    public void insertAfterSorted(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {

        outputAppender.reset(outputBuffer, false);

        int comp;
        sortedTupleAccessor.reset(sortedInputBuffer);
        if (sortedTupleAccessor.getTupleCount() > 0) {
            // the record in the sorted input buffer is the smallest one
            comp = compare(accessor, tupleIndex, sortedTupleAccessor, 0);

            if (comp < 0) {
                throw new HyracksDataException("The inserted records are not sorted.");
            }

            if (comp == 0) {
                // aggregate
                aggregator.aggregate(accessor, tupleIndex, sortedTupleAccessor, 0, aggState);
            } else {
                // flush the maintained sorted record
                int tupleOffset = 0;
                int fieldOffset = sortedTupleAccessor.getFieldCount() * INT_SIZE;
                outputTupleBuilder.reset();
                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(sortedTupleAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + sortedTupleAccessor.getFieldStartOffset(0, k), sortedTupleAccessor.getFieldLength(0, k));
                }

                aggregator.outputFinalResult(outputTupleBuilder, sortedTupleAccessor, 0, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }

                sortedInputBuffer.putInt(sortedInputBuffer.capacity() - INT_SIZE, 0);
            }
        }

        boolean isInputWritten = false;

        while (pointerFlushOffset < sortedRecordCount) {

            int fIndex = tPointers[pointerFlushOffset * PTR_SIZE];
            int tIndex = tPointers[pointerFlushOffset * PTR_SIZE + 1];
            hashtableRecordAccessor.reset(contents[fIndex]);
            comp = compare(accessor, tupleIndex, hashtableRecordAccessor, tIndex);
            if (comp > 0) {
                // flush smaller record from hash table
                int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(tIndex);
                int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;
                outputTupleBuilder.reset();
                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + hashtableRecordAccessor.getFieldStartOffset(tIndex, k),
                            hashtableRecordAccessor.getFieldLength(tIndex, k));
                }

                aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, tIndex, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }
                pointerFlushOffset++;
            } else if (comp < 0) {

                break;
            } else if (comp == 0) {
                aggregator.aggregate(accessor, tupleIndex, hashtableRecordAccessor, tIndex, aggState);
                isInputWritten = true;
                break;
            }
        }

        if (!isInputWritten) {
            // write the sorted record into the sorted buffer
            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilder, accessor, tupleIndex, aggState);
            int requiredLength = internalTupleBuilder.getFieldEndOffsets().length * INT_SIZE
                    + internalTupleBuilder.getSize() + 2 * INT_SIZE;
            if (requiredLength > sortedInputBuffer.capacity()) {
                sortedInputBuffer = ByteBuffer.allocate(requiredLength);
            }

            for (int i = 0; i < internalTupleBuilder.getFieldEndOffsets().length; i++) {
                sortedInputBuffer.putInt(i * INT_SIZE, internalTupleBuilder.getFieldEndOffsets()[i]);
            }

            System.arraycopy(internalTupleBuilder.getByteArray(), 0, sortedInputBuffer.array(),
                    internalTupleBuilder.getFieldEndOffsets().length * INT_SIZE, internalTupleBuilder.getSize());

            sortedInputBuffer.putInt(sortedInputBuffer.capacity() - INT_SIZE, 1);
            sortedInputBuffer.putInt(sortedInputBuffer.capacity() - 2 * INT_SIZE, requiredLength - 2 * INT_SIZE);
        }

    }

    /**
     * flush the sorted records into the writer. Note that since the in-memory record is also included in the hash
     * table if any, we should also flush it together.
     * 
     * @throws HyracksDataException
     */
    public void flushSortedList() throws HyracksDataException {
        // flush the in-memory record, if any
        sortedTupleAccessor.reset(sortedInputBuffer);
        if (sortedTupleAccessor.getTupleCount() > 0) {
            // flush the maintained sorted record
            int tupleOffset = 0;
            int fieldOffset = sortedTupleAccessor.getFieldCount() * INT_SIZE;
            outputTupleBuilder.reset();
            for (int k = 0; k < internalKeys.length; k++) {
                outputTupleBuilder.addField(sortedTupleAccessor.getBuffer().array(), tupleOffset + fieldOffset
                        + sortedTupleAccessor.getFieldStartOffset(0, k), sortedTupleAccessor.getFieldLength(0, k));
            }

            aggregator.outputFinalResult(outputTupleBuilder, sortedTupleAccessor, 0, aggState);

            if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(), 0,
                    outputTupleBuilder.getSize())) {
                FrameUtils.flushFrame(outputBuffer, outputWriter);
                outputAppender.reset(outputBuffer, true);
                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to flush the hash table to the final output");
                }
            }

            sortedInputBuffer.putInt(sortedInputBuffer.capacity() - INT_SIZE, 0);
        }
        outputAppender.reset(outputBuffer, false);
        // flush all sorted records not been flushed before
        while (pointerFlushOffset < sortedRecordCount) {
            int fIndex = tPointers[pointerFlushOffset * PTR_SIZE];
            int tIndex = tPointers[pointerFlushOffset * PTR_SIZE + 1];
            hashtableRecordAccessor.reset(contents[fIndex]);
            // flush record from hash table
            int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(tIndex);
            int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;
            outputTupleBuilder.reset();
            for (int k = 0; k < internalKeys.length; k++) {
                outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                        + hashtableRecordAccessor.getFieldStartOffset(tIndex, k),
                        hashtableRecordAccessor.getFieldLength(tIndex, k));
            }

            aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, tIndex, aggState);

            if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(), 0,
                    outputTupleBuilder.getSize())) {
                FrameUtils.flushFrame(outputBuffer, outputWriter);
                outputAppender.reset(outputBuffer, true);
                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to flush the hash table to the final output");
                }
            }

            pointerFlushOffset++;
        }

    }

    /**
     * flush unsorted records to the output.
     * 
     * @throws HyracksDataException
     */
    public void flushUnSortedRecords() throws HyracksDataException {
        // flush the records before the current entry
        for (int i = 0; i <= frameIndexForBeginningOfCurrentWorkingEntry; i++) {
            if (contents[i] == null) {
                continue;
            }

            hashtableRecordAccessor.reset(contents[i]);
            int tupleCount = (i == frameIndexForBeginningOfCurrentWorkingEntry) ? tupleIndexForBeginningOfCurrentWorkingEntry
                    : hashtableRecordAccessor.getTupleCount();

            for (int j = 0; j < tupleCount; j++) {
                outputTupleBuilder.reset();

                int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(j);
                int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + hashtableRecordAccessor.getFieldStartOffset(j, k),
                            hashtableRecordAccessor.getFieldLength(j, k));
                }

                aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, j, aggState);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }
            }
        }
    }

    public void reset() throws HyracksDataException {
        procTimer = System.currentTimeMillis() - procTimer;
        LOGGER.warning("InMEMHybridHashSortELHashTable-reset\t" + headers.length + "\t" + totalComparison + "\t"
                + procTimer + "\t" + resetTimer + "\t" + tupleInserted + "\t" + aggregatedCounter + "\t"
                + hashCollisionCount + "\t" + hashTimer);
        resetTimer = System.currentTimeMillis();

        aggregatedCounter = 0;
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                resetHeader(i);
            }
        }
        for (int i = 0; i < contents.length; i++) {
            if (contents[i] != null) {
                contents[i].putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
            }
        }
        currentLargestFrameIndex = -1;
        sortedRecordCount = 0;
        pointerFlushOffset = 0;
        totalComparison = 0;
        procTimer = System.currentTimeMillis();
        resetTimer = procTimer - resetTimer;
        hashCollisionCount = 0;
        hashTimer = 0;
        tupleInserted = 0;
        outputAppender.reset(outputBuffer, false);

        entriesUsed = 0;

        frameIndexForBeginningOfCurrentWorkingEntry = 0;
        tupleIndexForBeginningOfCurrentWorkingEntry = 0;
        currentWorkingEntryID = -1;
    }

    public void close() throws HyracksDataException {
        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
            outputBuffer = null;
        }

    }

    private void sort(int offset, int len) {
        int m = offset + (len >> 1);
        int mFrameIndex = tPointers[m * PTR_SIZE];
        int mTupleIndex = tPointers[m * PTR_SIZE + 1];
        int mNormKey = tPointers[m * PTR_SIZE + 2];
        compFrameAccessor1.reset(contents[mFrameIndex]);

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
                    compFrameAccessor2.reset(contents[bFrameIndex]);
                    cmp = compare(compFrameAccessor2, bTupleIndex, compFrameAccessor1, mTupleIndex);
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
                    compFrameAccessor2.reset(contents[cFrameIndex]);
                    cmp = compare(compFrameAccessor2, cTupleIndex, compFrameAccessor1, mTupleIndex);
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

    /**
     * Comparator for two records in the hash table.
     * 
     * @param accessor1
     * @param tupleIndex1
     * @param accessor2
     * @param tupleIndex2
     * @return
     */
    private int compare(FrameTupleAccessorForGroupHashtable accessor1, int tupleIndex1,
            FrameTupleAccessorForGroupHashtable accessor2, int tupleIndex2) {
        totalComparison++;
        int tStart1 = accessor1.getTupleStartOffset(tupleIndex1);
        int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

        int tStart2 = accessor2.getTupleStartOffset(tupleIndex2);
        int fStartOffset2 = accessor2.getFieldSlotsLength() + tStart2;

        for (int i = 0; i < internalKeys.length; ++i) {
            int fStart1 = accessor1.getFieldStartOffset(tupleIndex1, internalKeys[i]);
            int fEnd1 = accessor1.getFieldEndOffset(tupleIndex1, internalKeys[i]);
            int fLen1 = fEnd1 - fStart1;

            int fStart2 = accessor2.getFieldStartOffset(tupleIndex2, internalKeys[i]);
            int fEnd2 = accessor2.getFieldEndOffset(tupleIndex2, internalKeys[i]);
            int fLen2 = fEnd2 - fStart2;

            int c = comparators[i].compare(accessor1.getBuffer().array(), fStart1 + fStartOffset1, fLen1, accessor2
                    .getBuffer().array(), fStart2 + fStartOffset2, fLen2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
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
    private int compare(FrameTupleAccessor accessor, int tupleIndex, IFrameTupleAccessor hashAccessor,
            int hashTupleIndex) {
        totalComparison++;
        int tStart0 = accessor.getTupleStartOffset(tupleIndex);
        int fStartOffset0 = accessor.getFieldSlotsLength() + tStart0;

        int tStart1 = hashAccessor.getTupleStartOffset(hashTupleIndex);
        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < keys.length; ++i) {
            int fStart0 = accessor.getFieldStartOffset(tupleIndex, keys[i]);
            int fEnd0 = accessor.getFieldEndOffset(tupleIndex, keys[i]);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = hashAccessor.getFieldStartOffset(hashTupleIndex, internalKeys[i]);
            int fEnd1 = hashAccessor.getFieldEndOffset(hashTupleIndex, internalKeys[i]);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0, hashAccessor
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    public int getTupleCount() {
        return tupleInserted;
    }

    public static int getHashTableCapacity(int fCount, int fSize, int recSize, int htSzie) {
        int htHeadCount = htSzie % fSize == 0 ? 0 : 1;
        htHeadCount += htSzie / fSize;
        return (int) ((fCount - htHeadCount - 1) * fSize / (recSize + 2 * INT_SIZE) * 0.8);
    }

    private void printHashtableStatistics() {
        LOGGER.warning(InMemHybridHashSortELMergeHashTable.class.getSimpleName() + "-hashtableStat\t" + tableSize
                + "\t" + entriesUsed + "\t" + tupleInserted);
    }

}
