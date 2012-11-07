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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartitionalloc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
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
import edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTableForHashHash;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HybridHashDynamicDestagingGroupHashTable implements ISerializableGroupHashTableForHashHash {

    protected static final int INT_SIZE = 4;
    protected static final int END_REF = -1;

    protected final int tableSize, framesLimit, frameSize;

    protected ByteBuffer[] headers, contents;
    protected int[] frameNext;
    protected int freeFramesCounter;

    protected final IHyracksTaskContext ctx;

    protected final IAggregatorDescriptor aggregator;

    protected final int[] keys, internalKeys;

    protected final IBinaryComparator[] comparators;

    protected final ITuplePartitionComputer tpc;

    protected ByteBuffer outputBuffer;

    protected RunFileWriter[] runWriters;

    protected int[] runSizeInFrames;

    /**
     * run size size in tuples.
     */
    protected int[] runSizeInTuples;

    protected TuplePointer matchPointer;

    protected final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    protected final FrameTupleAppenderForGroupHashtable internalAppender;

    protected final FrameTupleAppender spilledPartitionAppender;

    protected final FrameTupleAccessor spilledPartitionFrameAccessor;

    protected final FrameTupleAppender outputAppender;

    /**
     * Tuple builder for hash table insertion
     */
    protected final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;

    protected final int numOfPartitions;

    /**
     * Current working frame reference for each partition. After being spilled, this is the same
     * as the corresponding {@link #partitionBeginningFrame}.
     */
    protected int[] partitionCurrentFrameIndex;

    /**
     * Partition size (in memory) in frames.
     */
    protected int[] partitionSizeInFrame;

    /**
     * Raw records inserted into the partition.
     */
    protected int[] partitionRawRecordCounts;

    protected LinkedList<Integer> partitionRawRecords;
    protected LinkedList<RunFileReader> runReaders;

    protected AggregateState[] partitionAggregateStates;

    protected BitSet partitionSpillFlags;

    protected final IFrameWriter outputWriter;

    protected int nextFreeFrameIndex;

    protected int totalTupleCount;

    protected final boolean pickLargestPartitionForFlush;

    protected final RecordDescriptor outRecDesc;

    protected final int tupleCountOffsetInFrame;

    protected int familyGenerateSeed;

    protected final boolean hashSpilled = false;

    /**
     * Used for the statistics about the key distribution collected during the insertion
     */
    protected int hashedRawRecords, hashedKeys;

    /**
     * For debugging
     */
    private static final Logger LOGGER = Logger.getLogger(HybridHashDynamicDestagingGroupHashTable.class
            .getSimpleName());

    int spilledGroups = 0, directOutputGroups = 0, reloadedSpilledGroups = 0, spilledFrames = 0, reloadedFrames = 0,
            spilledRuns = 0, reloadedRuns = 0;

    int maxHeaderCount, minHeaderCount;

    long comparsionCount = 0;

    long totalTimer = 0, insertTimer = 0, insertSpilledPartitionTimer = 0, findMatchTimer = 0,
            flushSpilledPartitionTimer = 0, spillResidentPartitionTimer = 0, flushHashtableToOutputTimer = 0;
    long spilledFrameFlushTimer = 0, residentFrameFlushTimer = 0;

    long hashSuccComparisonCount = 0, hashUnsuccComparisonCount = 0;
    long hashInitCount = 0;

    long insertedRawRecords = 0;

    int[] partitionRecordsInMemory, partitionUsedHashEntries, partitionRecordsInserted;

    public HybridHashDynamicDestagingGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int numOfPartitions, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily tpcf, IAggregatorDescriptor aggregator, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter) throws HyracksDataException {
        this(ctx, frameLimits, tableSize, numOfPartitions, procLevel, keys, comparators, tpcf, aggregator, inRecDesc,
                outRecDesc, outputWriter, true);
    }

    public HybridHashDynamicDestagingGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int numOfPartitions, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily tpcf, IAggregatorDescriptor aggregator, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter, boolean pickMostToFlush)
            throws HyracksDataException {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.framesLimit = frameLimits;
        this.frameSize = ctx.getFrameSize();
        this.tupleCountOffsetInFrame = FrameHelper.getTupleCountOffset(frameSize);

        this.keys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.outputWriter = outputWriter;

        this.aggregator = aggregator;

        this.familyGenerateSeed = procLevel;
        this.tpc = tpcf.createPartitioner(familyGenerateSeed);
        this.comparators = comparators;

        this.hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.spilledPartitionFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        this.internalTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.internalAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
        this.spilledPartitionAppender = new FrameTupleAppender(frameSize);
        this.outputAppender = new FrameTupleAppender(frameSize);

        this.matchPointer = new TuplePointer();

        // initialize the hash table
        int possibleHeaderSize = getHeaderSize(tableSize, frameSize);
        if (numOfPartitions > framesLimit - 1 - possibleHeaderSize) {
            LOGGER.warning("Input partition count is too large for an in-memory hash table with " + tableSize
                    + " slots. Do pure partitioning instead.");
            this.headers = new ByteBuffer[0];
        } else {
            this.headers = new ByteBuffer[possibleHeaderSize];
        }

        this.numOfPartitions = numOfPartitions;

        this.maxHeaderCount = headers.length;
        this.minHeaderCount = headers.length;

        this.outputBuffer = ctx.allocateFrame();

        // for dynamic memory allocation 
        this.contents = new ByteBuffer[framesLimit - 1 - headers.length];
        this.frameNext = new int[this.contents.length];

        // reset the memory
        for (int i = 0; i < frameNext.length - 1; i++) {
            frameNext[i] = i + 1;
        }
        frameNext[frameNext.length - 1] = END_REF;
        this.nextFreeFrameIndex = 0;
        this.freeFramesCounter = frameNext.length;

        // partition information
        this.partitionCurrentFrameIndex = new int[this.numOfPartitions];
        this.partitionSizeInFrame = new int[this.numOfPartitions];
        this.runWriters = new RunFileWriter[this.numOfPartitions];
        this.runSizeInFrames = new int[this.numOfPartitions];
        this.partitionAggregateStates = new AggregateState[this.numOfPartitions];
        this.runSizeInTuples = new int[this.numOfPartitions];
        this.partitionRawRecordCounts = new int[this.numOfPartitions];

        // pre-allocate one frame for each partition
        for (int i = 0; i < this.numOfPartitions; i++) {
            int newFrame = allocateFrame();
            partitionCurrentFrameIndex[i] = newFrame;
            partitionSizeInFrame[i] = 1;
            frameNext[newFrame] = END_REF;
        }

        this.partitionSpillFlags = new BitSet(this.numOfPartitions);
        // if only for partitioning: mark all partitions as spilled
        if (this.headers.length == 0) {
            for (int i = 0; i < this.numOfPartitions; i++) {
                this.partitionSpillFlags.set(i);
                if (runWriters[i] == null) {
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                HybridHashDynamicDestagingGroupHashTable.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    runWriters[i] = new RunFileWriter(runFile, ctx.getIOManager());
                    runSizeInFrames[i] = 0;
                }
                runWriters[i].open();
            }
        }

        this.totalTupleCount = 0;

        this.pickLargestPartitionForFlush = pickMostToFlush;

        this.outRecDesc = outRecDesc;

        this.hashedRawRecords = 0;
        this.hashedKeys = 0;

        // FIXME for debugging hash table
        this.partitionRecordsInMemory = new int[this.numOfPartitions];
        this.partitionUsedHashEntries = new int[this.numOfPartitions];
        this.partitionRecordsInserted = new int[this.numOfPartitions];

        totalTimer = System.nanoTime();
    }

    /**
     * Compute the partition id for the given hash key.
     * 
     * @param hashKey
     * @return
     */
    protected int partition(int hashKey) {
        return hashKey % numOfPartitions;
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
        // FIXME
        long timer = System.nanoTime();
        insertedRawRecords++;

        int entry = tpc.partition(accessor, tupleIndex, tableSize);
        int pid = partition(entry);

        partitionRawRecordCounts[pid]++;

        if (partitionSpillFlags.get(pid)) {
            // for spilled partition: direct insertion
            insertSpilledPartition(pid, accessor, tupleIndex);
            totalTupleCount++;
            runSizeInTuples[pid]++;
            partitionRecordsInMemory[pid]++;
        } else {
            hashedRawRecords++;
            boolean foundMatch = findMatch(entry, accessor, tupleIndex);
            if (foundMatch) {
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

                internalAppender.reset(contents[partitionCurrentFrameIndex[pid]], false);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {

                    // if the same partition is flushed, the aggregation value should be
                    // re-initialized since the aggregate state is reset.
                    boolean isSelfSpilled = false;

                    // try to allocate a new frame
                    while (true) {
                        int newFrame = allocateFrame();
                        if (newFrame != END_REF) {
                            frameNext[newFrame] = partitionCurrentFrameIndex[pid];
                            partitionCurrentFrameIndex[pid] = newFrame;
                            partitionSizeInFrame[pid]++;
                            break;
                        } else {

                            // choose a partition to spill to make space
                            chooseResidentPartitionToFlush();
                            if (partitionSpillFlags.get(pid)) {
                                // in case that the current partition is spilled
                                isSelfSpilled = true;
                                break;
                            }
                        }
                    }

                    if (isSelfSpilled) {
                        // re-insert
                        insertSpilledPartition(pid, accessor, tupleIndex);
                    } else {
                        internalAppender.reset(contents[partitionCurrentFrameIndex[pid]], true);
                        if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to insert an aggregation value to the hash table.");
                        }
                    }
                }

                // update hash table reference, only if the insertion to a resident partition is successful
                if (!partitionSpillFlags.get(pid)) {

                    hashedKeys++;

                    // update hash reference
                    if (matchPointer.frameIndex < 0) {
                        // first record for this entry; update the header references
                        int headerFrameIndex = getHeaderFrameIndex(entry);
                        int headerFrameOffset = getHeaderTupleIndex(entry);
                        if (headers[headerFrameIndex] == null) {
                            headers[headerFrameIndex] = ctx.allocateFrame();
                            resetHeader(headerFrameIndex);
                        }
                        headers[headerFrameIndex].putInt(headerFrameOffset, partitionCurrentFrameIndex[pid]);
                        headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE,
                                internalAppender.getTupleCount() - 1);

                        partitionUsedHashEntries[pid]++;

                    } else {
                        // update the previous reference
                        hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                        int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                        contents[matchPointer.frameIndex].putInt(refOffset, partitionCurrentFrameIndex[pid]);
                        contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE,
                                internalAppender.getTupleCount() - 1);
                    }
                }

                totalTupleCount++;
                runSizeInTuples[pid]++;
                partitionRecordsInMemory[pid]++;
            }
        }
        partitionRecordsInserted[pid]++;

        // FIXME
        insertTimer += System.nanoTime() - timer;
    }

    /**
     * Insert a record into a spilled partition. No hashing and aggregation is for
     * such an insertion.
     * 
     * @param pid
     * @param accessor
     * @param tupleIndex
     * @throws HyracksDataException
     */
    protected void insertSpilledPartition(int pid, FrameTupleAccessor accessor, int tupleIndex)
            throws HyracksDataException {
        // FIXME
        long timer = System.nanoTime();

        // insert spilled partition without hashing
        if (partitionAggregateStates[pid] == null) {
            partitionAggregateStates[pid] = aggregator.createAggregateStates();
        }

        internalTupleBuilder.reset();
        for (int k = 0; k < keys.length; k++) {
            internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
        }
        aggregator.init(internalTupleBuilder, accessor, tupleIndex, partitionAggregateStates[pid]);

        spilledPartitionAppender.reset(contents[partitionCurrentFrameIndex[pid]], false);
        if (!spilledPartitionAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
            // partition is a spilled one; 
            int nextResidentPartition = partitionSpillFlags.nextClearBit(0);
            if (nextResidentPartition < 0 || nextResidentPartition >= numOfPartitions) {
                // if all partitions are spilled, do dynamic allocation
                int newFrame = allocateFrame();
                if (newFrame != END_REF) {
                    frameNext[newFrame] = partitionCurrentFrameIndex[pid];
                    partitionCurrentFrameIndex[pid] = newFrame;
                    partitionSizeInFrame[pid]++;
                } else {
                    spillPartition(pid);
                }
            } else {
                //flush and reinsert
                spillPartition(pid);
            }

            spilledPartitionAppender.reset(contents[partitionCurrentFrameIndex[pid]], true);

            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilder, accessor, tupleIndex, partitionAggregateStates[pid]);
            if (!spilledPartitionAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                    internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                throw new HyracksDataException("Failed to insert an aggregation value to the hash table.");
            }
        }

        // FIXME
        insertSpilledPartitionTimer += System.nanoTime() - timer;
    }

    /**
     * Reset the header page
     * 
     * @param headerFrameIndex
     */
    protected void resetHeader(int headerFrameIndex) {
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
    protected int getHeaderFrameIndex(int entry) {
        int frameIndex = (entry / frameSize * 2 * INT_SIZE) + (entry % frameSize * 2 * INT_SIZE / frameSize);
        return frameIndex;
    }

    /**
     * Get the tuple index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    protected int getHeaderTupleIndex(int entry) {
        int offset = (entry % frameSize) * 2 * INT_SIZE % frameSize;
        return offset;
    }

    protected boolean findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex) {
        // FIXME
        long timer = System.nanoTime();
        int comps = 0;

        // reset the match pointer
        matchPointer.frameIndex = -1;
        matchPointer.tupleIndex = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            // FIXME
            findMatchTimer += System.nanoTime() - timer;
            hashInitCount++;

            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            matchPointer.frameIndex = entryFrameIndex;
            matchPointer.tupleIndex = entryTupleIndex;
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            comps++;
            if (compare(accessor, tupleIndex, hashtableRecordAccessor, entryTupleIndex) == 0) {
                // FIXME
                findMatchTimer += System.nanoTime() - timer;
                hashSuccComparisonCount += comps;

                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
        }
        // FIXME
        findMatchTimer += System.nanoTime() - timer;
        if (comps == 0) {
            hashInitCount++;
        }
        hashUnsuccComparisonCount += comps;

        return false;
    }

    /**
     * Spill partition, and reset the last frame as the output buffer.
     * 
     * @param pid
     * @throws HyracksDataException
     */
    protected void spillPartition(int pid) throws HyracksDataException {
        // FIXME
        long methodTimer = System.nanoTime();
        long frameFlushTimer;

        outputAppender.reset(outputBuffer, true);
        int frameToFlush = partitionCurrentFrameIndex[pid];
        while (true) {
            spilledPartitionFrameAccessor.reset(contents[frameToFlush]);
            int tupleCount = spilledPartitionFrameAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                outputTupleBuilder.reset();

                int tupleOffset = spilledPartitionFrameAccessor.getTupleStartOffset(i);
                int fieldOffset = spilledPartitionFrameAccessor.getFieldCount() * INT_SIZE;

                for (int k : internalKeys) {
                    outputTupleBuilder.addField(spilledPartitionFrameAccessor.getBuffer().array(), tupleOffset
                            + fieldOffset + spilledPartitionFrameAccessor.getFieldStartOffset(i, k),
                            spilledPartitionFrameAccessor.getFieldLength(i, k));
                }

                aggregator.outputPartialResult(outputTupleBuilder, spilledPartitionFrameAccessor, i,
                        partitionAggregateStates[pid]);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    // FIXME
                    frameFlushTimer = System.nanoTime();
                    FrameUtils.flushFrame(outputBuffer, runWriters[pid]);
                    runSizeInFrames[pid]++;
                    spilledFrameFlushTimer += System.nanoTime() - frameFlushTimer;

                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }
                spilledGroups++;
                totalTupleCount--;
            }
            int currentFrame = frameToFlush;
            frameToFlush = frameNext[currentFrame];

            if (frameToFlush != END_REF) {
                recycleFrame(currentFrame);
            } else {
                partitionCurrentFrameIndex[pid] = currentFrame;
                contents[partitionCurrentFrameIndex[pid]].putInt(tupleCountOffsetInFrame, 0);
                partitionSizeInFrame[pid] = 1;
                break;
            }

        }
        if (outputAppender.getTupleCount() > 0) {
            // FIXME
            frameFlushTimer = System.nanoTime();
            FrameUtils.flushFrame(outputBuffer, runWriters[pid]);
            runSizeInFrames[pid]++;
            spilledFrameFlushTimer += System.nanoTime() - frameFlushTimer;

            outputAppender.reset(outputBuffer, true);
        }

        if (partitionAggregateStates[pid] != null)
            partitionAggregateStates[pid].reset();

        // FIXME
        partitionRecordsInMemory[pid] = 0;
        partitionUsedHashEntries[pid] = 0;
        partitionRecordsInserted[pid] = 0;

        flushSpilledPartitionTimer += System.nanoTime() - methodTimer;
    }

    /**
     * Pick a resident partition to flush. This partition will be marked as a spilled partition, and
     * only one frame is assigned to it after flushing.
     * 
     * @throws HyracksDataException
     */
    protected void chooseResidentPartitionToFlush() throws HyracksDataException {
        int partitionToPick = -1;
        // choose one resident partition to spill. Note that after this loop 
        // it always gets a resident partition in partitionToPick, if there is any.
        for (int i = partitionSpillFlags.nextClearBit(0); i >= 0 && i < numOfPartitions; i = partitionSpillFlags
                .nextClearBit(i + 1)) {
            if (partitionToPick < 0) {
                partitionToPick = i;
                continue;
            }
            if (pickLargestPartitionForFlush) {
                if (partitionSizeInFrame[i] > partitionSizeInFrame[partitionToPick]) {
                    partitionToPick = i;
                }
            } else {
                if (partitionSizeInFrame[i] < partitionSizeInFrame[partitionToPick] && partitionSizeInFrame[i] > 1) {
                    partitionToPick = i;
                }
            }
        }

        if (partitionToPick < 0 || partitionToPick >= numOfPartitions) {
            // no resident partition to spill
            return;
        }

        spilledRuns++;

        // spill the picked partition
        int frameToFlush = partitionCurrentFrameIndex[partitionToPick];
        if (frameToFlush != END_REF) {
            if (runWriters[partitionToPick] == null) {
                FileReference runFile;
                try {
                    runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                            HybridHashDynamicDestagingGroupHashTable.class.getSimpleName());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                runWriters[partitionToPick] = new RunFileWriter(runFile, ctx.getIOManager());
                runSizeInFrames[partitionToPick] = 0;
            }
        }
        runWriters[partitionToPick].open();

        // flush the partition picked
        spillPartition(partitionToPick);

        // mark the picked partition as spilled
        partitionSpillFlags.set(partitionToPick);
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

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#getRunFileReaders()
     */
    @Override
    public LinkedList<RunFileReader> getRunFileReaders() throws HyracksDataException {
        return runReaders;
    }

    public LinkedList<Integer> getRunFileSizesInTuple() throws HyracksDataException {
        LinkedList<Integer> runSizesInTuple = new LinkedList<Integer>();

        for (int i = 0; i < numOfPartitions; i++) {
            if (runWriters[i] == null) {
                continue;
            }
            runSizesInTuple.add(runSizeInTuples[i]);
        }

        return runSizesInTuple;
    }

    public LinkedList<Integer> getRunFileSizesInFrame() throws HyracksDataException {
        LinkedList<Integer> rSizesInFrame = new LinkedList<Integer>();

        for (int i = 0; i < numOfPartitions; i++) {
            if (runWriters[i] == null) {
                continue;
            }
            rSizesInFrame.add(runSizeInFrames[i]);
        }

        return rSizesInFrame;
    }

    public boolean isPartitionSpilled(int pid) {
        return partitionSpillFlags.get(pid);
    }

    public RunFileReader getRunReader(int pid) throws HyracksDataException {
        if (runWriters[pid] != null) {
            return runWriters[pid].createReader();
        }
        return null;
    }

    public int getSpilledPartitionSizeInTuple(int pid) {
        return runSizeInTuples[pid];
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
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
        this.familyGenerateSeed++;

        for (int i = 0; i < this.numOfPartitions; i++) {
            int newFrame = allocateFrame();
            if (newFrame == END_REF) {
                throw new HyracksDataException("Not enough memory for this partition plan: expecting at least "
                        + numOfPartitions + " but " + contents.length + " available");
            }
            partitionCurrentFrameIndex[i] = newFrame;
            partitionSizeInFrame[i] = 1;
            frameNext[newFrame] = END_REF;

            runSizeInTuples[i] = 0;

            if (partitionAggregateStates != null && partitionAggregateStates[i] != null) {
                partitionAggregateStates[i].reset();
            }

            partitionRecordsInMemory[i] = 0;
            partitionUsedHashEntries[i] = 0;
            partitionRecordsInserted[i] = 0;
        }

        // reset the writers
        runWriters = new RunFileWriter[numOfPartitions];
        runSizeInFrames = new int[numOfPartitions];

        partitionSpillFlags.clear();
    }

    /**
     * Flush the hash table as an in-memory table into the give output writer. Note that using this method all
     * partitions are considered as resident.
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#flushHashtableToOutput(edu.uci.ics.
     *      hyracks.api.comm.IFrameWriter)
     */
    @Override
    public void flushHashtableToOutput(IFrameWriter outputWriter) throws HyracksDataException {

        long methodTimer = System.nanoTime();
        long frameFlushTimer;

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
                        // FIXME remove when deploying
                        frameFlushTimer = System.nanoTime();
                        FrameUtils.flushFrame(outputBuffer, outputWriter);
                        residentFrameFlushTimer += System.nanoTime() - frameFlushTimer;

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
            partitionAggregateStates[i].reset();

            partitionUsedHashEntries[i] = 0;
            partitionRecordsInMemory[i] = 0;
            partitionRecordsInserted[i] = 0;
        }

        if (outputAppender.getTupleCount() > 0) {
            // FIXME remove when deploying
            frameFlushTimer = System.nanoTime();
            FrameUtils.flushFrame(outputBuffer, outputWriter);
            residentFrameFlushTimer += System.nanoTime() - frameFlushTimer;

            outputAppender.reset(outputBuffer, true);
        }

        flushHashtableToOutputTimer += System.nanoTime() - methodTimer;
    }

    /**
     * Finish up the hash table operations by:<br/>
     * - flush all resident partitions, if any into the output writers;<br/>
     * - flush all spilled partitions into their runs; <br/>
     * - collect run file information.
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#finishup()
     */
    @Override
    public void finishup() throws HyracksDataException {

        // FIXME
        long flushResPartTimer, frameFlushTimer;

        // flush all spilled partitions
        partitionRawRecords = new LinkedList<Integer>();
        runReaders = new LinkedList<RunFileReader>();
        for (int i = partitionSpillFlags.nextSetBit(0); i >= 0; i = partitionSpillFlags.nextSetBit(i + 1)) {
            spillPartition(i);
            runReaders.add(runWriters[i].createReader());
            runWriters[i].close();
            partitionRawRecords.add(partitionRawRecordCounts[i]);
        }

        // flush resident partitions
        outputAppender.reset(outputBuffer, true);
        for (int i = partitionSpillFlags.nextClearBit(0); i >= 0 && i < numOfPartitions; i = partitionSpillFlags
                .nextClearBit(i + 1)) {

            // FIXME
            flushResPartTimer = System.nanoTime();

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
                        // FIXME
                        frameFlushTimer = System.nanoTime();
                        FrameUtils.flushFrame(outputBuffer, outputWriter);
                        residentFrameFlushTimer += System.nanoTime() - frameFlushTimer;

                        outputAppender.reset(outputBuffer, true);
                        if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to flush the hash table to the final output");
                        }
                    }

                    totalTupleCount--;
                    directOutputGroups++;
                }
                int currentFrame = frameToFlush;
                frameToFlush = frameNext[currentFrame];
                recycleFrame(currentFrame);
            }
            if (partitionAggregateStates[i] != null)
                partitionAggregateStates[i].reset();

            // FIXME
            partitionRecordsInMemory[i] = 0;
            partitionUsedHashEntries[i] = 0;
            partitionRecordsInserted[i] = 0;

            flushHashtableToOutputTimer += System.nanoTime() - flushResPartTimer;
        }

        if (totalTupleCount != 0) {
            LOGGER.severe("wrong tuple counter!");
        }

        ctx.getCounterContext().getCounter("must.hash.succ.comps", true).update(hashSuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.unsucc.comps", true).update(hashUnsuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.slot.init", true).update(hashInitCount);

        hashSuccComparisonCount = 0;
        hashUnsuccComparisonCount = 0;
        hashInitCount = 0;

        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
            outputAppender.reset(outputBuffer, true);
        }
        return;
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
        headers = null;
        for (int i = 0; i < contents.length; i++) {
            contents[i] = null;
        }
        contents = null;
        outputBuffer = null;

        partitionCurrentFrameIndex = null;
        partitionRecordsInMemory = null;
        partitionSizeInFrame = null;
        runSizeInTuples = null;
        partitionUsedHashEntries = null;
        partitionRecordsInserted = null;

        partitionAggregateStates = null;
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

    protected int allocateFrame() throws HyracksDataException {
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

    public static int getHeaderSize(int tableSize, int frameSize) {
        int residual = ((tableSize % frameSize) * INT_SIZE * 2) % frameSize == 0 ? 0 : 1;
        return (tableSize / frameSize * 2 * INT_SIZE) + ((tableSize % frameSize) * 2 * INT_SIZE / frameSize) + residual;
    }

    private void printHashtableStatistics() {
        int recInHashTable = 0;
        int usedHashSlots = 0;
        int recordInserted = 0;
        if (hashSpilled) {
            for (int i = 0; i < numOfPartitions; i++) {
                recInHashTable += partitionRecordsInMemory[i];
                usedHashSlots += partitionUsedHashEntries[i];
                recordInserted += partitionRecordsInserted[i];
            }
        } else {
            for (int i = partitionSpillFlags.nextClearBit(0); i >= 0 && i < numOfPartitions; i = partitionSpillFlags
                    .nextClearBit(i + 1)) {
                recInHashTable += partitionRecordsInMemory[i];
                usedHashSlots += partitionUsedHashEntries[i];
                recordInserted += partitionRecordsInserted[i];
            }
        }
        int actualTableSize = (tableSize * (numOfPartitions - partitionSpillFlags.cardinality()) >= 0) ? (tableSize
                * (numOfPartitions - partitionSpillFlags.cardinality()) / numOfPartitions) : (tableSize
                / numOfPartitions * (numOfPartitions - partitionSpillFlags.cardinality()));
        LOGGER.warning("HybridHashGroupHashTable-HashtableStatistics\t" + actualTableSize + "\t" + usedHashSlots + "\t"
                + recInHashTable + "\t" + recordInserted);
        LOGGER.warning(getHistogram());
    }

    private void printHashtableStatistics(int actualTableSize, int pid) {
        LOGGER.warning("HybridHashGroupHashTable-HashtableStatistics\t" + actualTableSize + "\t"
                + partitionUsedHashEntries[pid] + "\t" + partitionRecordsInMemory[pid] + "\t"
                + partitionRecordsInserted[pid]);
    }

    private String getHistogram() {

        int len;
        int[] histogram = new int[20];
        int spilledEntry = 0;
        for (int i = 0; i < tableSize; i++) {
            if (partitionSpillFlags.get(i % numOfPartitions)) {
                spilledEntry++;
                continue;
            }
            len = getEntryLength(i);
            while (len >= histogram.length) {
                int[] newHistogram = new int[histogram.length * 2];
                System.arraycopy(histogram, 0, newHistogram, 0, histogram.length);
                histogram = newHistogram;
            }
            histogram[len]++;
        }
        StringBuilder sbder = new StringBuilder();
        for (int i = 0; i < histogram.length; i++) {
            sbder.append(histogram[i]).append("\t");
        }
        sbder.append("S,").append(spilledEntry);
        return sbder.toString();
    }

    private int getEntryLength(int entry) {
        int cnt = 0;
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);
        if (headers[headerFrameIndex] == null) {
            return cnt;
        }
        FrameTupleAccessorForGroupHashtable debugAccessor = new FrameTupleAccessorForGroupHashtable(ctx.getFrameSize(),
                outRecDesc);
        int frameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int tupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);
        while (frameIndex >= 0) {
            debugAccessor.reset(contents[frameIndex]);
            frameIndex = debugAccessor.getHashReferenceNextFrameIndex(tupleIndex);
            tupleIndex = debugAccessor.getHashReferenceNextTupleIndex(tupleIndex);
            cnt++;
        }

        return cnt;
    }

    @Override
    public int getHashedRawRecords() {
        return hashedRawRecords;
    }

    @Override
    public int getHashedKeys() {
        return hashedKeys;
    }

    @Override
    public LinkedList<Integer> getSpilledPartitionRawRecordCount() throws HyracksDataException {
        return partitionRawRecords;
    }

    public static double getHashTableFudgeFactor(int hashtableSlots, int estimatedRecordSize, int frameSize,
            int memorySizeInFrames) {
        int pagesForRecord = memorySizeInFrames - getHeaderPages(hashtableSlots, frameSize);
        int recordsInHashtable = (pagesForRecord - 1) * ((int) (frameSize / (estimatedRecordSize + 2 * INT_SIZE)));

        return (double) memorySizeInFrames * frameSize / recordsInHashtable / estimatedRecordSize;
    }

    public static int getHeaderPages(int hashtableSlots, int frameSize) {
        return (int) Math.ceil((double) hashtableSlots * 2 * INT_SIZE / frameSize);
    }
}
