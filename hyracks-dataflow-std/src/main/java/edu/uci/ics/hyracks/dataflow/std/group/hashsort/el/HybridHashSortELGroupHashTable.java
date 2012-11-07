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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
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
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAccessorForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAppenderForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HybridHashSortELGroupHashTable implements ISerializableGroupHashTable {

    private static final int INT_SIZE = 4;
    private static final int END_REF = -1;

    private final int tableSize, framesLimit, frameSize;

    private ByteBuffer[] headers, contents;
    private int[] frameNext;
    private int freeFramesCounter;

    private final IHyracksTaskContext ctx;

    private final IAggregatorDescriptor aggregator;
    private final AggregateState[] partitionAggregateStates;

    private final int[] keys, internalKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer tpc;

    private ByteBuffer outputBuffer;

    private LinkedList<RunFileReader> runReaders;

    private TuplePointer matchPointer;

    private final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    private final FrameTupleAccessorForGroupHashtable compFrameAccessor1, compFrameAccessor2;

    private final FrameTupleAppenderForGroupHashtable internalAppender;

    private final FrameTupleAppender outputAppender;

    /**
     * Tuple builder for hash table insertion
     */
    private final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;

    private int numOfPartitions;

    private final int[] partitionBeginFrameIndex, partitionCurrentFrameIndex, partitionSizeInFrame;

    private int spilledPartitionCurrentFrameIndex, spilledPartitionCurrentFrameCount;

    private final IFrameWriter outputWriter;

    private int nextFreeFrameIndex;

    private int totalTupleCount;

    private final int tupleCountOffsetInFrame;

    private final int sortThreshold;

    /**
     * pointers for sort records in an entry
     */
    private int[] tPointers;

    private static final int INIT_REF_COUNT = 8;
    private static final int PTR_SIZE = 3;

    private final INormalizedKeyComputer firstNormalizer;

    public int maxRecLength = -1;

    private static final Logger LOGGER = Logger.getLogger(HybridHashSortELGroupHashTable.class.getSimpleName());

    int spilledGroups = 0, directOutputGroups = 0, insertGroups = 0, spillCount = 0, spilledPart = 0,
            sortedEntries = 0;
    long comparsionCount = 0, hashTimer = 0, sortTimer = 0, swapCount = 0, flushTimer = 0;
    double maxHashtableRatio = 0.0;

    private int[] partitionEntriesUsed, partitionUniqueRecs;
    private int spilledPartitionEntriesUsed, spilledPartitionUniqueRecs;

    public HybridHashSortELGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize, int[] keys,
            int sortThreshold, double partitionFactor, IBinaryComparator[] comparators, ITuplePartitionComputer tpc,
            INormalizedKeyComputer firstNormalizerComputer, IAggregatorDescriptor aggregator,
            IAggregatorDescriptor merger, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.framesLimit = frameLimits;
        this.frameSize = ctx.getFrameSize();
        this.tupleCountOffsetInFrame = FrameHelper.getTupleCountOffset(frameSize);
        this.sortThreshold = sortThreshold;

        this.firstNormalizer = firstNormalizerComputer;

        this.keys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.outputWriter = outputWriter;

        this.aggregator = aggregator;

        this.tpc = tpc;
        this.comparators = comparators;

        this.hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);

        this.compFrameAccessor1 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.compFrameAccessor2 = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);

        this.internalTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.internalAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
        this.outputAppender = new FrameTupleAppender(frameSize);

        this.matchPointer = new TuplePointer();

        // initialize the hash table
        int residual = (tableSize % frameSize) * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        this.headers = new ByteBuffer[(tableSize / frameSize * 2 * INT_SIZE)
                + ((tableSize % frameSize) * 2 * INT_SIZE / frameSize) + residual];

        // set the output buffer frame
        this.outputBuffer = ctx.allocateFrame();

        // set the content offset
        this.contents = new ByteBuffer[frameLimits - headers.length - 1];
        this.frameNext = new int[contents.length];
        for (int i = 0; i < contents.length - 1; i++) {
            frameNext[i] = i + 1;
        }
        frameNext[frameNext.length - 1] = END_REF;
        this.nextFreeFrameIndex = 0;
        this.freeFramesCounter = frameNext.length;

        this.numOfPartitions = (int) (contents.length * partitionFactor);

        this.partitionBeginFrameIndex = new int[this.numOfPartitions];
        this.partitionCurrentFrameIndex = new int[this.numOfPartitions];
        this.partitionSizeInFrame = new int[this.numOfPartitions];
        this.partitionAggregateStates = new AggregateState[this.numOfPartitions];

        this.partitionUniqueRecs = new int[this.numOfPartitions];
        this.partitionEntriesUsed = new int[this.numOfPartitions];

        for (int i = 0; i < this.numOfPartitions; i++) {
            int newFrame = allocateFrame();
            if (newFrame == END_REF) {
                throw new HyracksDataException("Not enough memory for this partition plan: expecting at least "
                        + numOfPartitions + " but " + contents.length + " available");
            }
            partitionBeginFrameIndex[i] = newFrame;
            partitionCurrentFrameIndex[i] = newFrame;
            partitionSizeInFrame[i] = 1;
            frameNext[newFrame] = END_REF;
            partitionUniqueRecs[i] = 0;
            partitionEntriesUsed[i] = 0;
        }

        this.spilledPartitionCurrentFrameIndex = END_REF;
        this.spilledPartitionCurrentFrameCount = 0;

        this.spilledPartitionEntriesUsed = 0;
        this.spilledPartitionUniqueRecs = 0;

        this.totalTupleCount = 0;
        this.runReaders = new LinkedList<RunFileReader>();
    }

    public static int getMinimumFramesLimit(int tableSize, int frameSize) {
        int residual = (tableSize % frameSize) * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        return tableSize / frameSize * INT_SIZE * 2 + tableSize % frameSize * INT_SIZE * 2 / frameSize + residual + 2;
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
        int pid = entry % numOfPartitions;
        if (findMatch(entry, accessor, tupleIndex)) {
            // find match; do aggregation
            hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
            aggregator.aggregate(accessor, tupleIndex, hashtableRecordAccessor, matchPointer.tupleIndex,
                    partitionAggregateStates[pid]);
        } else {

            if (partitionAggregateStates[pid] == null) {
                partitionAggregateStates[pid] = aggregator.createAggregateStates();
            }

            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilder, accessor, tupleIndex, partitionAggregateStates[pid]);

            int currentPartFrame = partitionCurrentFrameIndex[pid] == END_REF ? spilledPartitionCurrentFrameIndex
                    : partitionCurrentFrameIndex[pid];

            // get the maximum record length
            int recLength = internalTupleBuilder.getFieldEndOffsets().length * INT_SIZE
                    + internalTupleBuilder.getSize() + INT_SIZE;

            if (recLength > maxRecLength) {
                maxRecLength = recLength;
            }

            internalAppender.reset(contents[currentPartFrame], false);
            if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                    internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {

                // if the same partition is flushed, the aggregation value should be
                // re-initialized since the aggregate state is reset.
                boolean needReinit = false;
                boolean isInserted = false;

                // try to allocate a new frame
                int newFrame = allocateFrame();
                if (newFrame != END_REF) {
                    frameNext[newFrame] = currentPartFrame;
                    if (partitionCurrentFrameIndex[pid] == END_REF) {
                        spilledPartitionCurrentFrameIndex = newFrame;
                        spilledPartitionCurrentFrameCount++;
                        currentPartFrame = spilledPartitionCurrentFrameIndex;
                    } else {
                        partitionCurrentFrameIndex[pid] = newFrame;
                        partitionSizeInFrame[pid]++;
                        currentPartFrame = partitionCurrentFrameIndex[pid];
                    }
                } else {
                    // need to flush
                    while (spilledPartitionCurrentFrameCount * 2 < contents.length) {
                        int residentPartToSpill = getResidentPartitionToSpill();
                        if (residentPartToSpill < 0) {
                            throw new HyracksDataException(
                                    "No resident partition to spill, but spilled partition has not reached half of the memory capacity.");
                        }

                        LOGGER.warning(HybridHashSortELGroupHashTable.class.getSimpleName() + "-insert-flush\t"
                                + residentPartToSpill + "\t" + partitionSizeInFrame[residentPartToSpill] + "\t"
                                + partitionEntriesUsed[residentPartToSpill] + "\t"
                                + partitionUniqueRecs[residentPartToSpill]);

                        // merging spilled partitions
                        frameNext[partitionBeginFrameIndex[residentPartToSpill]] = spilledPartitionCurrentFrameIndex;
                        spilledPartitionCurrentFrameIndex = partitionCurrentFrameIndex[residentPartToSpill];

                        currentPartFrame = spilledPartitionCurrentFrameIndex;

                        spilledPartitionCurrentFrameCount += partitionSizeInFrame[residentPartToSpill];

                        spilledPartitionEntriesUsed += partitionEntriesUsed[residentPartToSpill];
                        spilledPartitionUniqueRecs += partitionUniqueRecs[residentPartToSpill];

                        // set the partition as spilled
                        partitionBeginFrameIndex[residentPartToSpill] = END_REF;
                        partitionCurrentFrameIndex[residentPartToSpill] = END_REF;
                        partitionSizeInFrame[residentPartToSpill] = END_REF;
                        partitionEntriesUsed[residentPartToSpill] = 0;
                        partitionUniqueRecs[residentPartToSpill] = 0;

                        // check whether there is space for the insertion
                        if (partitionCurrentFrameIndex[pid] == END_REF) {
                            internalAppender.reset(contents[currentPartFrame], false);
                            if (internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                    internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                                isInserted = true;
                                break;
                            }
                        }
                        spilledPart++;
                    }
                    if (!isInserted) {
                        // need to flush spilled partitions
                        flushSpilledPartition();
                        spillCount++;

                        if (partitionCurrentFrameIndex[pid] == END_REF) {
                            needReinit = true;
                            matchPointer.frameIndex = -1;
                            matchPointer.tupleIndex = -1;
                            currentPartFrame = spilledPartitionCurrentFrameIndex;
                        } else {
                            // try to allocate a frame again
                            newFrame = allocateFrame();
                            if (newFrame == END_REF) {
                                throw new HyracksDataException("Cannot allocate memory for insertion");
                            }
                            frameNext[newFrame] = partitionCurrentFrameIndex[pid];
                            partitionCurrentFrameIndex[pid] = newFrame;
                            currentPartFrame = partitionCurrentFrameIndex[pid];
                        }
                    }
                }

                if (!isInserted) {
                    if (needReinit) {
                        internalTupleBuilder.reset();
                        for (int k = 0; k < keys.length; k++) {
                            internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
                        }
                        aggregator.init(internalTupleBuilder, accessor, tupleIndex, partitionAggregateStates[pid]);
                    }

                    internalAppender.reset(contents[currentPartFrame], true);
                    if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                            internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to insert an aggregation value to the hash table.");
                    }
                }
            }

            insertGroups++;

            // update hash reference
            if (matchPointer.frameIndex < 0) {
                // first record for this entry; update the header references
                int headerFrameIndex = getHeaderFrameIndex(entry);
                int headerFrameOffset = getHeaderTupleIndex(entry);
                if (headers[headerFrameIndex] == null) {
                    headers[headerFrameIndex] = ctx.allocateFrame();
                    resetHeader(headerFrameIndex);
                }
                headers[headerFrameIndex].putInt(headerFrameOffset, currentPartFrame);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, internalAppender.getTupleCount() - 1);

                if (partitionCurrentFrameIndex[pid] == END_REF) {
                    spilledPartitionEntriesUsed++;
                } else {
                    partitionEntriesUsed[pid]++;
                }

            } else {
                // update the previous reference
                hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                contents[matchPointer.frameIndex].putInt(refOffset, currentPartFrame);
                contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE, internalAppender.getTupleCount() - 1);
            }

            if (partitionCurrentFrameIndex[pid] == END_REF) {
                spilledPartitionUniqueRecs++;
            } else {
                partitionUniqueRecs[pid]++;
            }

            totalTupleCount++;
        }
    }

    private boolean findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex) {
        long timer = System.currentTimeMillis();
        // reset the match pointer
        matchPointer.frameIndex = -1;
        matchPointer.tupleIndex = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            hashTimer += System.currentTimeMillis() - timer;
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
                hashTimer += System.currentTimeMillis() - timer;
                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
        }

        hashTimer += System.currentTimeMillis() - timer;
        return false;
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
     * Get the largest resident partition for spilling. If all partitions are spilled
     * partitions, -1 is returned.
     * 
     * @return
     */
    private int getResidentPartitionToSpill() {
        int partToSpill = -1;
        for (int i = 0; i < numOfPartitions; i++) {
            if (partitionCurrentFrameIndex[i] == END_REF) {
                continue;
            }
            if (partToSpill < 0) {
                partToSpill = i;
            } else {
                if (partitionSizeInFrame[i] > partitionSizeInFrame[partToSpill]) {
                    partToSpill = i;
                }
            }
        }
        return partToSpill;
    }

    private void flushSpilledPartition() throws HyracksDataException {

        long timer = System.currentTimeMillis();

        // FIXME
        printHashtableStatistics();

        FileReference runFile;
        try {
            runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                    HybridHashSortELGroupHashTable.class.getSimpleName());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        RunFileWriter runWriter = new RunFileWriter(runFile, ctx.getIOManager());
        runWriter.open();
        outputAppender.reset(outputBuffer, true);

        for (int i = 0; i < tableSize; i++) {
            int pid = i % numOfPartitions;
            if (partitionCurrentFrameIndex[pid] != END_REF) {
                continue;
            }
            // flush the entry, and sort it if necessary
            int tupleInEntry = getEntryPointers(i);

            for (int ptr = 0; ptr < tupleInEntry; ptr++) {
                int frameIndex = tPointers[ptr * PTR_SIZE];
                int tupleIndex = tPointers[ptr * PTR_SIZE + 1];

                hashtableRecordAccessor.reset(contents[frameIndex]);
                outputTupleBuilder.reset();

                int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(tupleIndex);
                int tupleLength = hashtableRecordAccessor.getTupleEndOffset(tupleIndex) - tupleOffset;
                int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + hashtableRecordAccessor.getFieldStartOffset(tupleIndex, k),
                            hashtableRecordAccessor.getFieldLength(tupleIndex, k));
                }

                aggregator.outputPartialResult(outputTupleBuilder, hashtableRecordAccessor.getBuffer().array(),
                        tupleOffset, tupleLength, hashtableRecordAccessor.getFieldCount(), INT_SIZE,
                        partitionAggregateStates[pid]);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, runWriter);

                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush an aggregation result.");
                    }
                }
                totalTupleCount--;
                spilledGroups++;
            }

            // reset the hash headers
            int headerFrameIndex = getHeaderFrameIndex(i);
            int headerFrameOffset = getHeaderTupleIndex(i);
            if (headers[headerFrameIndex] != null) {
                headers[headerFrameIndex].putInt(headerFrameOffset, -1);
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, -1);
            }
        }

        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, runWriter);
        }

        runWriter.close();
        runReaders.add(runWriter.createReader());

        // recycle frames

        int frameToFlush = spilledPartitionCurrentFrameIndex;
        int currentFrame;
        while (true) {
            currentFrame = frameToFlush;
            frameToFlush = frameNext[currentFrame];
            if (frameToFlush != END_REF) {
                recycleFrame(currentFrame);
            } else {
                contents[currentFrame].putInt(tupleCountOffsetInFrame, 0);
                spilledPartitionCurrentFrameIndex = currentFrame;
                spilledPartitionCurrentFrameCount = 1;
                break;
            }
        }

        spilledPartitionEntriesUsed = 0;
        spilledPartitionUniqueRecs = 0;

        flushTimer += System.currentTimeMillis() - timer;
    }

    private int getEntryPointers(int entry) {
        if (tPointers == null) {
            tPointers = new int[INIT_REF_COUNT * PTR_SIZE];
        }
        int ptr = 0;

        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            return 0;
        }

        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        do {
            if (entryFrameIndex < 0) {
                break;
            }
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            tPointers[ptr * PTR_SIZE] = entryFrameIndex;
            tPointers[ptr * PTR_SIZE + 1] = entryTupleIndex;
            int tStart = hashtableRecordAccessor.getTupleStartOffset(entryTupleIndex);
            int f0StartRel = hashtableRecordAccessor.getFieldStartOffset(entryTupleIndex, internalKeys[0]);
            int f0EndRel = hashtableRecordAccessor.getFieldEndOffset(entryTupleIndex, internalKeys[0]);
            int f0Start = f0StartRel + tStart + hashtableRecordAccessor.getFieldSlotsLength();
            tPointers[ptr * PTR_SIZE + 2] = firstNormalizer == null ? 0 : firstNormalizer.normalize(
                    hashtableRecordAccessor.getBuffer().array(), f0Start, f0EndRel - f0StartRel);

            ptr++;

            if (ptr * PTR_SIZE >= tPointers.length) {
                int[] newTPointers = new int[tPointers.length * 2];
                System.arraycopy(tPointers, 0, newTPointers, 0, tPointers.length);
                tPointers = newTPointers;
            }

            // move to the next record
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);

        } while (true);

        if (ptr > sortThreshold) {
            long timer = System.currentTimeMillis();
            sort(sortThreshold, ptr - sortThreshold);
            sortTimer += System.currentTimeMillis() - timer;
            sortedEntries++;
        }

        return ptr;
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

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#getRunFileReaders()
     */
    @Override
    public LinkedList<RunFileReader> getRunFileReaders() throws HyracksDataException {
        return runReaders;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#reset()
     */
    @Override
    public void reset() throws HyracksDataException {
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

        for (int j = 0; j < contents.length - 1; j++) {
            if (contents[j] != null)
                contents[j].putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
            if (j < contents.length - 2) {
                frameNext[j] = j + 1;
            } else {
                frameNext[j] = END_REF;
            }
        }
        this.nextFreeFrameIndex = 0;
        this.freeFramesCounter = contents.length;

        this.totalTupleCount = 0;

        for (int i = 0; i < this.numOfPartitions; i++) {
            int newFrame = allocateFrame();
            if (newFrame == END_REF) {
                throw new HyracksDataException("Not enough memory for this partition plan: expecting at least "
                        + numOfPartitions + " but " + contents.length + " available");
            }
            partitionBeginFrameIndex[i] = newFrame;
            partitionCurrentFrameIndex[i] = newFrame;
            partitionSizeInFrame[i] = 0;
            frameNext[newFrame] = END_REF;
            partitionEntriesUsed[i] = 0;
            partitionUniqueRecs[i] = 0;
        }

        this.maxRecLength = 0;

        this.spilledPartitionEntriesUsed = 0;
        this.spilledPartitionUniqueRecs = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#flushHashtableToOutput(edu.uci.ics.
     * hyracks.api.comm.IFrameWriter)
     */
    @Override
    public void flushHashtableToOutput(IFrameWriter outputWriter) throws HyracksDataException {

        // FIXME
        printHashtableStatistics();

        outputAppender.reset(outputBuffer, true);

        for (int i = 0; i < numOfPartitions; i++) {
            int frameToFlush = partitionCurrentFrameIndex[i];
            while (frameToFlush != END_REF) {
                hashtableRecordAccessor.reset(contents[frameToFlush]);
                int tupleCount = hashtableRecordAccessor.getTupleCount();
                for (int j = 0; j < tupleCount; j++) {
                    outputTupleBuilder.reset();

                    int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(j);
                    int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                    for (int k = 0; k < internalKeys.length; k++) {
                        outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset
                                + fieldOffset + hashtableRecordAccessor.getFieldStartOffset(j, k),
                                hashtableRecordAccessor.getFieldLength(j, k));
                    }

                    aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, j,
                            partitionAggregateStates[i]);

                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outputBuffer, outputWriter);
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to flush the hash table to the final output");
                        }
                    }
                    totalTupleCount--;
                }
                int currentFrame = frameToFlush;
                frameToFlush = frameNext[currentFrame];
                recycleFrame(currentFrame);
            }
            if (partitionAggregateStates[i] != null)
                partitionAggregateStates[i].reset();
            partitionEntriesUsed[i] = 0;
            partitionUniqueRecs[i] = 0;
        }

        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#finishup()
     */
    @Override
    public void finishup() throws HyracksDataException {

        // FIXME
        printHashtableStatistics();

        if (spilledPartitionCurrentFrameIndex >= 0)
            flushSpilledPartition();

        outputAppender.reset(outputBuffer, true);

        // flush all resident entries
        for (int i = 0; i < numOfPartitions; i++) {
            if (partitionCurrentFrameIndex[i] == END_REF) {
                continue;
            }
            int frameToFlush = partitionCurrentFrameIndex[i];
            while (frameToFlush != END_REF) {
                hashtableRecordAccessor.reset(contents[frameToFlush]);
                int tupleCount = hashtableRecordAccessor.getTupleCount();
                for (int j = 0; j < tupleCount; j++) {
                    outputTupleBuilder.reset();

                    int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(j);
                    int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                    for (int k = 0; k < internalKeys.length; k++) {
                        outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset
                                + fieldOffset + hashtableRecordAccessor.getFieldStartOffset(j, k),
                                hashtableRecordAccessor.getFieldLength(j, k));
                    }

                    aggregator.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, j,
                            partitionAggregateStates[i]);

                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        FrameUtils.flushFrame(outputBuffer, outputWriter);
                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to flush the hash table to the final output");
                        }
                    }
                    directOutputGroups++;
                    totalTupleCount--;
                }
                int currentFrame = frameToFlush;
                frameToFlush = frameNext[currentFrame];
                recycleFrame(currentFrame);
            }
            partitionCurrentFrameIndex[i] = END_REF;
            if (partitionAggregateStates[i] != null)
                partitionAggregateStates[i].reset();
        }
        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
        }

        LOGGER.warning("HybridHashSortELHashTable-finishup\t" + insertGroups + "\t" + directOutputGroups + "\t"
                + spilledGroups + "\t" + headers.length + "\t" + comparsionCount + "\t" + swapCount + "\t"
                + spilledPart + "\t" + spillCount + "\t" + hashTimer + "\t" + sortTimer + "\t" + flushTimer);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#close()
     */
    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < headers.length; i++) {
            headers[i] = null;
        }
        for (int i = 0; i < contents.length; i++) {
            contents[i] = null;
        }
        outputBuffer = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#getTupleCount()
     */
    @Override
    public int getTupleCount() {
        return totalTupleCount;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#getFrameSize()
     */
    @Override
    public int getFrameSize() {
        return framesLimit;
    }

    public int getMaxRecordLength() {
        return maxRecLength;
    }

    private int allocateFrame() throws HyracksDataException {
        if (nextFreeFrameIndex != END_REF) {
            int freeFrameIndex = nextFreeFrameIndex;
            if (contents[freeFrameIndex] == null) {
                contents[freeFrameIndex] = ctx.allocateFrame();
            }
            nextFreeFrameIndex = frameNext[freeFrameIndex];
            freeFramesCounter--;
            if (freeFramesCounter < 0) {
                throw new HyracksDataException("Memory underflow!");
            }
            return freeFrameIndex;
        }
        return END_REF;
    }

    private void recycleFrame(int frameIdx) {
        contents[frameIdx].putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
        int oldFreeHead = nextFreeFrameIndex;
        nextFreeFrameIndex = frameIdx;
        frameNext[frameIdx] = oldFreeHead;
        freeFramesCounter++;
    }

    private int compare(FrameTupleAccessor accessor, int tupleIndex, FrameTupleAccessorForGroupHashtable hashAccessor,
            int hashTupleIndex) {
        comparsionCount++;
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

    private int compare(FrameTupleAccessorForGroupHashtable accessor1, int tupleIndex1,
            FrameTupleAccessorForGroupHashtable accessor2, int tupleIndex2) {
        comparsionCount++;
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

    private void printHashtableStatistics() {
        int entriesUsed = 0;
        int uniqueRecInHT = 0;
        for (int i = 0; i < numOfPartitions; i++) {
            if (partitionCurrentFrameIndex[i] == END_REF) {
                continue;
            }
            entriesUsed += partitionEntriesUsed[i];
            uniqueRecInHT += partitionUniqueRecs[i];
        }
        entriesUsed += spilledPartitionEntriesUsed;
        uniqueRecInHT += spilledPartitionUniqueRecs;
        LOGGER.warning(HybridHashSortELGroupHashTable.class.getSimpleName() + "-HashtableStatistics\t" + tableSize
                + "\t" + entriesUsed + "\t" + uniqueRecInHT);
        if (entriesUsed != 0 && uniqueRecInHT / entriesUsed > maxHashtableRatio) {
            maxHashtableRatio = uniqueRecInHT / entriesUsed;
            StringBuilder partitionDistribution = new StringBuilder();
            for (int i = 0; i < numOfPartitions; i++) {
                if (partitionCurrentFrameIndex[i] == END_REF) {
                    continue;
                }
                partitionDistribution.append(i).append("\t").append(partitionEntriesUsed[i]).append("\t")
                        .append(partitionUniqueRecs[i]).append("\n");
            }
            partitionDistribution.append(-1).append("\t").append(spilledPartitionEntriesUsed).append("\t")
                    .append(spilledPartitionUniqueRecs).append("\n");
            LOGGER.warning(partitionDistribution.toString());
        }
    }
}
