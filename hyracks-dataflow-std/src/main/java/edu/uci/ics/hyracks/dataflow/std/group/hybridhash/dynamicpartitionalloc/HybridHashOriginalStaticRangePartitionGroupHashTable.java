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
import java.util.HashMap;
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
import edu.uci.ics.hyracks.dataflow.std.group.struct.HashArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTableForHashHash;
import edu.uci.ics.hyracks.dataflow.std.group.struct.TupleAccessHelper;
import edu.uci.ics.hyracks.dataflow.std.sort.BSTMemMgr;
import edu.uci.ics.hyracks.dataflow.std.sort.IMemoryManager;
import edu.uci.ics.hyracks.dataflow.std.sort.Slot;
import edu.uci.ics.hyracks.dataflow.std.util.FrameMemManager;

public class HybridHashOriginalStaticRangePartitionGroupHashTable implements ISerializableGroupHashTableForHashHash {

    protected static final int INT_SIZE = 4;
    protected static final int END_REF = -1;

    protected final int tableSize, framesLimit, frameSize;

    protected int[] headers, inMemHTContents, spillOutputBuffers;

    protected boolean[] fullSpillOutputBufferFlags;

    protected HashMap<Integer, Integer> frameMgrIdxToHTContentFrameIndexMap;

    /**
     * memory manager for hash table content
     */
    protected IMemoryManager hashtableContent;

    protected FrameMemManager frameManager;

    protected final IHyracksTaskContext ctx;

    protected final IAggregatorDescriptor aggregator;

    protected final int[] keys, internalKeys;

    protected final IBinaryComparator[] comparators;

    protected final ITuplePartitionComputer inputTpc, internalTpc;

    protected int outputBuffer;

    protected RunFileWriter[] runWriters;

    protected int[] runSizeInFrames;

    private Slot allocationPtr, unallocationPtr;

    /**
     * run size size in tuples.
     */
    protected int[] runSizeInTuples;

    protected int[] matchPointer;

    protected final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    protected final FrameTupleAppenderForGroupHashtable internalAppender;

    protected final TupleAccessHelper tupleAccessorHelper;

    protected final FrameTupleAppender spilledPartitionAppender;

    protected final FrameTupleAccessor spilledPartitionFrameAccessor;

    protected final FrameTupleAppender outputAppender;

    /**
     * Tuple builder for hash table insertion
     */
    protected final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;

    protected final HashArrayTupleBuilder internalHashArrayTupleBuilder;

    protected final int numOfPartitions;

    /**
     * Partition size (in memory) in frames.
     */
    protected int[] partitionSizeInFrame;

    /**
     * Raw records inserted into the partition.
     */
    protected int[] partitionRawRecordCounts;

    protected boolean[] spilledFlags;

    protected LinkedList<Integer> partitionRawRecords;
    protected LinkedList<RunFileReader> runReaders;

    protected AggregateState[] partitionAggregateStates;

    protected final IFrameWriter outputWriter;

    protected int totalTupleCount;

    protected final RecordDescriptor outRecDesc;

    protected final int tupleCountOffsetInFrame;

    protected int familyGenerateSeed;

    /**
     * Used for the statistics about the key distribution collected during the insertion
     */
    protected int hashedRawRecords, hashedKeys;

    /**
     * For debugging
     */
    private static final Logger LOGGER = Logger.getLogger(HybridHashOriginalStaticRangePartitionGroupHashTable.class
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

    public HybridHashOriginalStaticRangePartitionGroupHashTable(IHyracksTaskContext ctx, int frameLimits,
            int tableSize, int numberOfPartitions, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily inputTpcf, ITuplePartitionComputerFamily internalTpcf,
            IAggregatorDescriptor aggregator, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.tableSize = tableSize;
        this.framesLimit = frameLimits;
        this.frameSize = ctx.getFrameSize();
        this.tupleCountOffsetInFrame = FrameHelper.getTupleCountOffset(frameSize);

        this.frameManager = new FrameMemManager(framesLimit, ctx);

        this.keys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.outputWriter = outputWriter;

        this.aggregator = aggregator;

        this.familyGenerateSeed = procLevel;
        this.inputTpc = inputTpcf.createPartitioner(familyGenerateSeed);
        this.internalTpc = internalTpcf.createPartitioner(familyGenerateSeed);
        this.comparators = comparators;

        this.hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outRecDesc);
        this.spilledPartitionFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        this.internalTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.internalHashArrayTupleBuilder = new HashArrayTupleBuilder(outRecDesc.getFieldCount());
        this.outputTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
        this.internalAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
        this.spilledPartitionAppender = new FrameTupleAppender(frameSize);
        this.outputAppender = new FrameTupleAppender(frameSize);

        this.tupleAccessorHelper = new TupleAccessHelper(outRecDesc);

        this.matchPointer = new int[2];
        this.frameMgrIdxToHTContentFrameIndexMap = new HashMap<Integer, Integer>();

        this.outputBuffer = frameManager.allocateFrame();
        if (this.outputBuffer < 0) {
            throw new HyracksDataException("Failed to allocate a frame as the output buffer");
        }

        // initialize the hash table
        int possibleHeaderSize = getHeaderSize(tableSize, frameSize);
        if (numberOfPartitions - 1 >= framesLimit - 1 - possibleHeaderSize) {
            LOGGER.warning("Input partition " + numberOfPartitions
                    + " count is too large for an in-memory hash table with " + tableSize + " slots and " + framesLimit
                    + " frames. Do pure partitioning instead.");
            this.headers = new int[0];
            this.spilledFlags = new boolean[] { true, true };
            this.numOfPartitions = framesLimit - 1;

            this.inMemHTContents = new int[0];
            this.hashtableContent = null;

            // set the output buffers
            this.spillOutputBuffers = new int[numOfPartitions];
            for (int i = 0; i < numOfPartitions; i++) {
                this.spillOutputBuffers[i] = frameManager.allocateFrame();
                if (this.spillOutputBuffers[i] < 0) {
                    throw new HyracksDataException("Failed to allocate an output buffer for partition " + i);
                }
            }
        } else {
            this.headers = new int[possibleHeaderSize];
            // reserve space for headers
            int allocatedFrameIdx = frameManager.bulkAllocate(headers.length);

            if (allocatedFrameIdx < 0) {
                throw new HyracksDataException("Failed to allocate frames for the hash table header");
            }

            for (int i = 0; i < headers.length; i++) {
                headers[i] = allocatedFrameIdx;
                resetHeader(headers[i]);
                allocatedFrameIdx = frameManager.getNextFrame(allocatedFrameIdx);
            }

            this.spilledFlags = new boolean[] { false, false };
            this.numOfPartitions = numberOfPartitions;

            allocatedFrameIdx = frameManager.bulkAllocate(framesLimit - 1 - headers.length - (numOfPartitions - 1));

            this.inMemHTContents = new int[framesLimit - 1 - headers.length - (numOfPartitions - 1)];
            for (int i = 0; i < inMemHTContents.length; i++) {
                if (allocatedFrameIdx < 0) {
                    throw new HyracksDataException("Failed to allocate frames for the hash table content");
                }
                this.inMemHTContents[i] = allocatedFrameIdx;
                this.frameMgrIdxToHTContentFrameIndexMap.put(allocatedFrameIdx, i);
                allocatedFrameIdx = frameManager.getNextFrame(allocatedFrameIdx);
            }
            this.hashtableContent = new BSTMemMgr(ctx, framesLimit - 1 - headers.length - (numOfPartitions - 1));

            // set the output buffers
            this.spillOutputBuffers = new int[numOfPartitions];
            this.fullSpillOutputBufferFlags = new boolean[numOfPartitions];
            this.spillOutputBuffers[0] = this.outputBuffer;
            for (int i = 1; i < numOfPartitions; i++) {
                this.spillOutputBuffers[i] = frameManager.allocateFrame();
                if (this.spillOutputBuffers[i] < 0) {
                    throw new HyracksDataException("Failed to allocate an output buffer for partition " + i);
                }
                this.fullSpillOutputBufferFlags[i] = false;
            }
        }

        this.maxHeaderCount = headers.length;
        this.minHeaderCount = headers.length;

        this.partitionSizeInFrame = new int[this.numOfPartitions];
        this.runWriters = new RunFileWriter[this.numOfPartitions];
        this.runSizeInFrames = new int[this.numOfPartitions];
        this.partitionAggregateStates = new AggregateState[this.numOfPartitions];
        this.runSizeInTuples = new int[this.numOfPartitions];
        this.partitionRawRecordCounts = new int[this.numOfPartitions];

        // if only for partitioning: mark all partitions as spilled
        if (this.spilledFlags[0]) {
            for (int i = 0; i < this.numOfPartitions; i++) {
                FileReference runFile;
                try {
                    runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                            HybridHashOriginalStaticRangePartitionGroupHashTable.class.getSimpleName());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                runWriters[i] = new RunFileWriter(runFile, ctx.getIOManager());
                runSizeInFrames[i] = 0;

                runWriters[i].open();
            }
        }

        this.totalTupleCount = 0;

        this.outRecDesc = outRecDesc;

        this.hashedRawRecords = 0;
        this.hashedKeys = 0;

        // FIXME for debugging hash table
        this.partitionRecordsInMemory = new int[this.numOfPartitions];
        this.partitionUsedHashEntries = new int[this.numOfPartitions];
        this.partitionRecordsInserted = new int[this.numOfPartitions];

        this.allocationPtr = new Slot();
        this.unallocationPtr = new Slot();

        totalTimer = System.nanoTime();
    }

    private ByteBuffer getFrame(int frameIndex) {
        if (frameMgrIdxToHTContentFrameIndexMap.containsKey(frameIndex)) {
            return hashtableContent.getFrame(frameMgrIdxToHTContentFrameIndexMap.get(frameIndex));
        } else {
            return frameManager.getFrame(frameIndex);
        }
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

        int entry = inputTpc.partition(accessor, tupleIndex, tableSize);
        int pid = partition(entry);

        partitionRawRecordCounts[pid]++;

        if (spilledFlags[((pid > 0) ? 1 : 0)]) {
            // for spilled partition: direct insertion
            insertSpilledPartition(pid, accessor, tupleIndex);
            totalTupleCount++;
            runSizeInTuples[pid]++;
            partitionRecordsInMemory[pid]++;
        } else {
            hashedRawRecords++;
            if (findMatch(entry, accessor, tupleIndex)) {
                // find match; do aggregation
                aggregator.aggregate(accessor, tupleIndex, getFrame(matchPointer[0]).array(), matchPointer[1],
                        tupleAccessorHelper.getTupleLength(getFrame(matchPointer[0]), matchPointer[1]),
                        partitionAggregateStates[pid]);
            } else {

                int[] insertedPosition = new int[] { -1, -1 };

                if (partitionAggregateStates[pid] == null) {
                    partitionAggregateStates[pid] = aggregator.createAggregateStates();
                }

                internalTupleBuilder.reset();
                for (int k = 0; k < keys.length; k++) {
                    internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
                }
                aggregator.init(internalTupleBuilder, accessor, tupleIndex, partitionAggregateStates[pid]);

                boolean isInsertedIntoOutputBuffers = false;

                // try to insert into the output buffer first
                if (pid > 0 && !fullSpillOutputBufferFlags[pid]) {
                    internalAppender.reset(getFrame(spillOutputBuffers[pid]), false);
                    if (internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                            internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                        isInsertedIntoOutputBuffers = true;
                        insertedPosition[0] = spillOutputBuffers[pid];
                        // compute the tuple start offset
                        int insertedTupleIndex = internalAppender.getTupleCount() - 1;
                        insertedPosition[1] = getTupleStartOffset(spillOutputBuffers[pid], insertedTupleIndex);
                    } else {
                        fullSpillOutputBufferFlags[pid] = true;
                    }
                }

                if (!isInsertedIntoOutputBuffers) {
                    int slotRequirement = internalTupleBuilder.getSize()
                            + internalTupleBuilder.getFieldEndOffsets().length * INT_SIZE + 2 * INT_SIZE;
                    // reset the allocation pointer
                    allocationPtr.set(-1, -1);
                    hashtableContent.allocate(slotRequirement, allocationPtr);

                    while (allocationPtr.isNull()) {
                        chooseResidentPartitionToFlush();
                        // stop the attempt to spill: if the inserting partition 
                        // is spilled, or partition 0 is spilled (so all partitions are spilled)
                        if (spilledFlags[0] || spilledFlags[((pid > 0) ? 1 : 0)]) {
                            break;
                        }
                        hashtableContent.allocate(slotRequirement, allocationPtr);
                    }

                    if (allocationPtr.isNull()) {
                        // Time to spill the record
                        if (spilledFlags[((pid > 0) ? 1 : 0)]) {
                            // the partition is spilled
                            insertSpilledPartition(pid, accessor, tupleIndex);
                        } else {
                            throw new HyracksDataException(
                                    "Failed to allocate memory for an insertion into the hash table");
                        }
                    } else {

                        try {
                            internalTupleBuilder.getDataOutput().writeInt(-1);
                            internalTupleBuilder.getDataOutput().writeInt(-1);
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        hashtableContent.writeTuple(allocationPtr.getFrameIx(), allocationPtr.getOffset(),
                                internalTupleBuilder.getFieldEndOffsets(), internalTupleBuilder.getByteArray(), 0,
                                internalTupleBuilder.getSize());
                        insertedPosition[0] = inMemHTContents[allocationPtr.getFrameIx()];
                        insertedPosition[1] = hashtableContent.getTupleStartOffset(allocationPtr.getFrameIx(),
                                allocationPtr.getOffset());
                    }
                }

                // update the hash table reference
                if (insertedPosition[0] >= 0) {
                    hashedKeys++;
                    if (matchPointer[0] < 0) {
                        // need to initialize a new slot into the hash table
                        int headerFrameIndex = getHeaderFrameIndex(entry);
                        int headerFrameOffset = getHeaderTupleIndex(entry);
                        if (!frameManager.isFrameInitialized(headers[headerFrameIndex])) {
                            resetHeader(headers[headerFrameIndex]);
                        }
                        getFrame(headers[headerFrameIndex]).putInt(headerFrameOffset, insertedPosition[0]);
                        getFrame(headers[headerFrameIndex]).putInt(headerFrameOffset + INT_SIZE, insertedPosition[1]);
                    } else {
                        int refOffset = tupleAccessorHelper.getTupleEndOffset(getFrame(matchPointer[0]),
                                matchPointer[1]);

                        getFrame(matchPointer[0]).putInt(refOffset, insertedPosition[0]);
                        getFrame(matchPointer[0]).putInt(refOffset + INT_SIZE, insertedPosition[1]);
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

    private int getTupleStartOffset(int frameIndex, int tupleIndex) {
        if (frameMgrIdxToHTContentFrameIndexMap.containsKey(frameIndex)) {
            LOGGER.severe("Wrong method is called: cannot find start offset using this method when the frame is in the BSTManager");
        }
        if (tupleIndex == 0) {
            return 0;
        } else {
            return getFrame(frameIndex).getInt(frameSize - INT_SIZE - tupleIndex * INT_SIZE);
        }
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

        spilledPartitionAppender.reset(getFrame(spillOutputBuffers[pid]), false);
        if (!spilledPartitionAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
            // partition is a spilled one; 
            if (spilledFlags[0]) {
                // if all partitions are spilled, do dynamic allocation
                int newFrameIdx = frameManager.allocateFrame();
                if (newFrameIdx >= 0) {
                    frameManager.setNextFrame(newFrameIdx, spillOutputBuffers[pid]);
                    spillOutputBuffers[pid] = newFrameIdx;
                    partitionSizeInFrame[pid]++;
                } else {
                    spillPartition(pid, false);
                    if (partitionAggregateStates[pid] != null)
                        partitionAggregateStates[pid].reset();
                }
            } else {
                //flush and reinsert
                spillPartition(pid, false);
                if (partitionAggregateStates[pid] != null)
                    partitionAggregateStates[pid].reset();
            }

            spilledPartitionAppender.reset(getFrame(spillOutputBuffers[pid]), true);

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
        ByteBuffer buf = getFrame(headerFrameIndex);
        for (int i = 0; i < frameSize; i += INT_SIZE) {
            buf.putInt(i, -1);
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

        int tupleStartOffset = accessor.getTupleStartOffset(tupleIndex);

        // reset the match pointer
        matchPointer[0] = -1;
        matchPointer[1] = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (!frameManager.isFrameInitialized(headers[headerFrameIndex])) {
            // FIXME
            findMatchTimer += System.nanoTime() - timer;
            hashInitCount++;

            return false;
        }

        // initialize the pointer to the first record 
        int entryFrameIndex = getFrame(headers[headerFrameIndex]).getInt(headerFrameOffset);
        int entryTupleStartOffset = getFrame(headers[headerFrameIndex]).getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {

            matchPointer[0] = entryFrameIndex;
            matchPointer[1] = entryTupleStartOffset;

            hashtableRecordAccessor.reset(getFrame(entryFrameIndex));
            comps++;
            if (compare(accessor, tupleStartOffset, hashtableRecordAccessor, entryTupleStartOffset) == 0) {
                // FIXME
                findMatchTimer += System.nanoTime() - timer;
                hashSuccComparisonCount += comps;

                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = tupleAccessorHelper.getTupleEndOffset(getFrame(entryFrameIndex), entryTupleStartOffset);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = getFrame(prevFrameIndex).getInt(refOffset);
            entryTupleStartOffset = getFrame(prevFrameIndex).getInt(refOffset + INT_SIZE);
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
     * Pick a resident partition to flush. This partition will be marked as a spilled partition, and
     * only one frame is assigned to it after flushing.
     * 
     * @throws HyracksDataException
     */
    protected void chooseResidentPartitionToFlush() throws HyracksDataException {

        if (!spilledFlags[1]) {
            // spill spilling partitions
            // - spill its output buffers first
            for (int i = 1; i < spillOutputBuffers.length; i++) {
                spillPartition(i, false);
                outputAppender.reset(getFrame(spillOutputBuffers[i]), true);
            }
            // - spill keys in the hash table content
            spillHashtableContent(1, false);

            spilledFlags[1] = true;
            if (partitionAggregateStates[1] != null)
                partitionAggregateStates[1].reset();
            return;
        }

        if (!spilledFlags[0]) {
            // spill partition zero
            spillHashtableContent(0, false);
            spilledFlags[0] = true;
            if (partitionAggregateStates[0] != null)
                partitionAggregateStates[0].reset();

            // recycle frames occupied by the hash table

            for (int i : inMemHTContents) {
                frameManager.recycleFrame(i);
            }
            hashtableContent.close();

            // recycle hash headers
            for (int i : headers) {
                frameManager.recycleFrame(i);
            }
            headers = null;

            frameMgrIdxToHTContentFrameIndexMap.clear();
            inMemHTContents = null;

            // assign a new output buffer for partition 0
            spillOutputBuffers[0] = frameManager.allocateFrame();

            if (spillOutputBuffers[0] < 0) {
                throw new HyracksDataException("Failed to allocate an output buffer for partition 0");
            }

            return;
        }
    }

    private void spillPartition(int partToSpill, boolean isFinalOutput) throws HyracksDataException {

        ByteBuffer buf;
        long frameFlushTimer;

        if (spillOutputBuffers[partToSpill] < 0) {
            return;
        }

        int frameToSpill = spillOutputBuffers[partToSpill];

        outputAppender.reset(getFrame(outputBuffer), true);

        while (frameToSpill >= 0) {
            buf = getFrame(frameToSpill);
            spilledPartitionFrameAccessor.reset(buf);
            for (int j = 0; j < spilledPartitionFrameAccessor.getTupleCount(); j++) {
                outputTupleBuilder.reset();
                int tupleOffset = spilledPartitionFrameAccessor.getTupleStartOffset(j);
                int fieldOffset = spilledPartitionFrameAccessor.getFieldCount() * INT_SIZE;
                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(spilledPartitionFrameAccessor.getBuffer().array(), tupleOffset
                            + fieldOffset + spilledPartitionFrameAccessor.getFieldStartOffset(j, k),
                            spilledPartitionFrameAccessor.getFieldLength(j, k));
                }

                if (isFinalOutput) {
                    aggregator.outputFinalResult(outputTupleBuilder, spilledPartitionFrameAccessor, j,
                            partitionAggregateStates[partToSpill]);
                } else {
                    aggregator.outputPartialResult(outputTupleBuilder, spilledPartitionFrameAccessor, j,
                            partitionAggregateStates[partToSpill]);
                }

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    // FIXME
                    frameFlushTimer = System.nanoTime();
                    if (isFinalOutput)
                        FrameUtils.flushFrame(getFrame(outputBuffer), outputWriter);
                    else {
                        // initialize the run file if it is null
                        if (runWriters[partToSpill] == null) {
                            FileReference runFile;
                            try {
                                runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                        HybridHashDynamicDestagingGroupHashTable.class.getSimpleName());
                            } catch (IOException e) {
                                throw new HyracksDataException(e);
                            }
                            runWriters[partToSpill] = new RunFileWriter(runFile, ctx.getIOManager());
                            runWriters[partToSpill].open();
                            runSizeInFrames[partToSpill] = 0;
                        }
                        FrameUtils.flushFrame(getFrame(outputBuffer), runWriters[partToSpill]);
                        runSizeInFrames[partToSpill]++;
                    }

                    spilledFrameFlushTimer += System.nanoTime() - frameFlushTimer;

                    outputAppender.reset(getFrame(outputBuffer), true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }

                // if the tuple has hash reference to a tuple inside of the hash table, then update the hash table reference
                if (!spilledFlags[partToSpill == 0 ? 0 : 1]) {
                    int hashRefStartoffset = spilledPartitionFrameAccessor.getTupleEndOffset(j) - 2 * INT_SIZE;
                    int nextHashFrameIndex = buf.getInt(hashRefStartoffset);
                    int nextHashTupleStartOffset = buf.getInt(hashRefStartoffset + INT_SIZE);
                    if (frameMgrIdxToHTContentFrameIndexMap.containsKey(nextHashFrameIndex)) {
                        int entryID = internalTpc.partition(spilledPartitionFrameAccessor, j, tableSize);
                        int hashHeaderRef = getHeaderFrameIndex(entryID);
                        int hashHeaderTupleRef = getHeaderTupleIndex(entryID);
                        getFrame(headers[hashHeaderRef]).putInt(hashHeaderTupleRef, nextHashFrameIndex);
                        getFrame(headers[hashHeaderRef])
                                .putInt(hashHeaderTupleRef + INT_SIZE, nextHashTupleStartOffset);
                    }
                }
            }

            int nextFrameToSpill = frameManager.getNextFrame(frameToSpill);
            if (nextFrameToSpill >= 0) {
                frameManager.recycleFrame(frameToSpill);
                spillOutputBuffers[partToSpill] = nextFrameToSpill;
            }
            frameToSpill = nextFrameToSpill;
        }

        getFrame(spillOutputBuffers[partToSpill]).putInt(frameSize - INT_SIZE, 0);

        if (outputAppender.getTupleCount() > 0) {
            frameFlushTimer = System.nanoTime();
            if (isFinalOutput)
                FrameUtils.flushFrame(getFrame(outputBuffer), outputWriter);
            else {
                // initialize the run file if it is null
                if (runWriters[partToSpill] == null) {
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                HybridHashDynamicDestagingGroupHashTable.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    runWriters[partToSpill] = new RunFileWriter(runFile, ctx.getIOManager());
                    runWriters[partToSpill].open();
                    runSizeInFrames[partToSpill] = 0;
                }
                FrameUtils.flushFrame(getFrame(outputBuffer), runWriters[partToSpill]);
                runSizeInFrames[partToSpill]++;
            }
            spilledFrameFlushTimer += System.nanoTime() - frameFlushTimer;
            outputAppender.reset(getFrame(outputBuffer), true);
        }
    }

    private void spillHashtableContent(int partToSpill, boolean isFinalOutput) throws HyracksDataException {
        ByteBuffer buf;
        long frameFlushTimer;
        // - spill slots in the hash table belonging to spilling partitions.
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] < 0 || !frameManager.isFrameInitialized(headers[i])) {
                // the header page is not initialized, then skip
                continue;
            }
            buf = getFrame(headers[i]);
            // scan all references in the page
            int hashRefOffset = 0;
            while (hashRefOffset < frameSize) {
                int entryIdx = i * (frameSize / (INT_SIZE * 2)) + hashRefOffset / (INT_SIZE * 2);
                int pid = entryIdx % numOfPartitions;
                boolean spillSlot = true;
                if (partToSpill >= 0) {
                    if (partToSpill == 0) {
                        spillSlot = (pid == 0);
                    } else {
                        spillSlot = (pid > 0);
                    }
                }
                if (spillSlot) {
                    // follow the hash reference and spill the records
                    int nextFrameIndex = buf.getInt(hashRefOffset);
                    int nextTupleStartOffset = buf.getInt(hashRefOffset + INT_SIZE);
                    ByteBuffer workingBuf;
                    while (nextFrameIndex >= 0 && frameMgrIdxToHTContentFrameIndexMap.containsKey(nextFrameIndex)) {

                        workingBuf = getFrame(nextFrameIndex);

                        outputTupleBuilder.reset();

                        for (int k : internalKeys) {
                            outputTupleBuilder.addField(
                                    workingBuf.array(),
                                    nextTupleStartOffset
                                            + tupleAccessorHelper.getFieldSlotLength()
                                            + tupleAccessorHelper.getFieldStartOffset(workingBuf, nextTupleStartOffset,
                                                    k),
                                    tupleAccessorHelper.getFieldLength(workingBuf, nextTupleStartOffset, k));
                        }

                        if (isFinalOutput)
                            aggregator.outputFinalResult(outputTupleBuilder, workingBuf.array(), nextTupleStartOffset,
                                    tupleAccessorHelper.getTupleLength(workingBuf, nextTupleStartOffset),
                                    tupleAccessorHelper.getFieldCount(), INT_SIZE, partitionAggregateStates[pid]);
                        else
                            aggregator.outputPartialResult(outputTupleBuilder, workingBuf.array(),
                                    nextTupleStartOffset,
                                    tupleAccessorHelper.getTupleLength(workingBuf, nextTupleStartOffset),
                                    tupleAccessorHelper.getFieldCount(), INT_SIZE, partitionAggregateStates[pid]);

                        outputAppender.reset(getFrame(spillOutputBuffers[pid]), false);
                        if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            // FIXME
                            frameFlushTimer = System.nanoTime();
                            if (isFinalOutput)
                                FrameUtils.flushFrame(getFrame(spillOutputBuffers[pid]), outputWriter);
                            else {
                                // initialize the run file if it is null
                                if (runWriters[pid] == null) {
                                    FileReference runFile;
                                    try {
                                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                                HybridHashDynamicDestagingGroupHashTable.class.getSimpleName());
                                    } catch (IOException e) {
                                        throw new HyracksDataException(e);
                                    }
                                    runWriters[pid] = new RunFileWriter(runFile, ctx.getIOManager());
                                    runWriters[pid].open();
                                    runSizeInFrames[pid] = 0;
                                }
                                FrameUtils.flushFrame(getFrame(spillOutputBuffers[pid]), runWriters[pid]);
                            }
                            runSizeInFrames[pid]++;
                            spilledFrameFlushTimer += System.nanoTime() - frameFlushTimer;

                            outputAppender.reset(getFrame(spillOutputBuffers[pid]), true);
                            if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                                    outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                                throw new HyracksDataException("Failed to flush the hash table to the final output");
                            }
                        }

                        // move to the next hash reference along the slot
                        int hashRefFrameIndex = workingBuf.getInt(tupleAccessorHelper.getTupleEndOffset(workingBuf,
                                nextTupleStartOffset));
                        int hashRefTupleStartOffset = workingBuf.getInt(tupleAccessorHelper.getTupleEndOffset(
                                workingBuf, nextTupleStartOffset) + INT_SIZE);

                        // unallocate the space in hash table content, if necessary
                        if (frameMgrIdxToHTContentFrameIndexMap.containsKey(nextFrameIndex)) {
                            unallocationPtr.set(frameMgrIdxToHTContentFrameIndexMap.get(nextFrameIndex),
                                    hashtableContent.getSlotStartOffset(
                                            frameMgrIdxToHTContentFrameIndexMap.get(nextFrameIndex),
                                            nextTupleStartOffset));
                            hashtableContent.unallocate(unallocationPtr);
                        }

                        nextFrameIndex = hashRefFrameIndex;
                        nextTupleStartOffset = hashRefTupleStartOffset;
                    }

                    // reset the hash reference
                    buf.putInt(hashRefOffset, -1);
                    buf.putInt(hashRefOffset + INT_SIZE, -1);
                }
                // move to the next hash reference slot
                hashRefOffset += INT_SIZE * 2;
            }
        }

        for (int i = 0; i < numOfPartitions; i++) {
            outputAppender.reset(getFrame(spillOutputBuffers[i]), false);
            if (outputAppender.getTupleCount() > 0) {
                frameFlushTimer = System.nanoTime();
                if (isFinalOutput)
                    FrameUtils.flushFrame(getFrame(spillOutputBuffers[i]), outputWriter);
                else {
                    if (runWriters[i] == null) {
                        FileReference runFile;
                        try {
                            runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                    HybridHashDynamicDestagingGroupHashTable.class.getSimpleName());
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        runWriters[i] = new RunFileWriter(runFile, ctx.getIOManager());
                        runWriters[i].open();
                        runSizeInFrames[i] = 0;
                    }
                    FrameUtils.flushFrame(getFrame(spillOutputBuffers[i]), runWriters[i]);
                    runSizeInFrames[i]++;
                }
                spilledFrameFlushTimer += System.nanoTime() - frameFlushTimer;
                outputAppender.reset(getFrame(spillOutputBuffers[i]), true);
            }
        }

    }

    private int compare(FrameTupleAccessor accessor, int tupleStartOffset,
            FrameTupleAccessorForGroupHashtable hashAccessor, int hashTupleStartOffset) {
        comparsionCount++;
        int fStartOffset0 = accessor.getFieldSlotsLength() + tupleStartOffset;

        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + hashTupleStartOffset;

        for (int i = 0; i < keys.length; ++i) {
            int fStart0 = keys[i] == 0 ? 0 : accessor.getBuffer().getInt(tupleStartOffset + (keys[i] - 1) * 4);
            int fEnd0 = accessor.getBuffer().getInt(tupleStartOffset + keys[i] * 4);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = internalKeys[i] == 0 ? 0 : hashAccessor.getBuffer().getInt(
                    hashTupleStartOffset + (internalKeys[i] - 1) * 4);
            int fEnd1 = hashAccessor.getBuffer().getInt(hashTupleStartOffset + internalKeys[i] * 4);
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
        return (pid > 0) ? spilledFlags[1] : spilledFlags[0];
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
        throw new HyracksDataException("Unsupported reset functionality for "
                + HybridHashOriginalStaticRangePartitionGroupHashTable.class.getSimpleName());
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

        // spill spilling partitions
        // - spill its output buffers first
        for (int i = 1; i < spillOutputBuffers.length; i++) {
            spillPartition(i, true);
            outputAppender.reset(getFrame(spillOutputBuffers[i]), true);
        }
        // - spill keys in the hash table content
        spillHashtableContent(-1, true);

        // - spill its output buffers first
        for (int i = 1; i < spillOutputBuffers.length; i++) {
            spillPartition(i, true);
            outputAppender.reset(getFrame(spillOutputBuffers[i]), true);
        }

        flushHashtableToOutputTimer += System.nanoTime() - methodTimer;
    }

    /**
     * Finish up the hash table operations by:<br/>
     * - flush all resident partitions, if any, into the output writers;<br/>
     * - flush all spilled partitions into their runs; <br/>
     * - collect run file information.
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#finishup()
     */
    @Override
    public void finishup() throws HyracksDataException {

        // flush all spilled partitions
        partitionRawRecords = new LinkedList<Integer>();
        runReaders = new LinkedList<RunFileReader>();
        for (int i = 0; i < numOfPartitions; i++) {
            if (!isPartitionSpilled(i)) {
                continue;
            }
            spillPartition(i, false);
            if (runWriters[i] != null) {
                runReaders.add(runWriters[i].createReader());
                runWriters[i].close();
            }
            partitionRawRecords.add(partitionRawRecordCounts[i]);
        }

        // flush resident partitions, if any
        if (!spilledFlags[1]) {
            spillPartition(1, true);
        }
        if (!spilledFlags[0]) {
            spillHashtableContent(-1, true);
        }

        ctx.getCounterContext().getCounter("must.hash.succ.comps", true).update(hashSuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.unsucc.comps", true).update(hashUnsuccComparisonCount);

        ctx.getCounterContext().getCounter("must.hash.slot.init", true).update(hashInitCount);

        hashSuccComparisonCount = 0;
        hashUnsuccComparisonCount = 0;
        hashInitCount = 0;

        return;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#close()
     */
    @Override
    public void close() throws HyracksDataException {
        if (!spilledFlags[0]) {
            hashtableContent.close();
        }
        frameManager.close();
        for (int i = 0; i < numOfPartitions; i++) {
            if (partitionAggregateStates[i] != null)
                partitionAggregateStates[i].close();
        }
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

    public static int getHeaderSize(int tableSize, int frameSize) {
        int residual = ((tableSize % frameSize) * INT_SIZE * 2) % frameSize == 0 ? 0 : 1;
        return (tableSize / frameSize * 2 * INT_SIZE) + ((tableSize % frameSize) * 2 * INT_SIZE / frameSize) + residual;
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
