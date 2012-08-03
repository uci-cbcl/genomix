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
package edu.uci.ics.hyracks.dataflow.std.group.struct;

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
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HybridHashGroupHashTable implements ISerializableGroupHashTable {

    private static final int INT_SIZE = 4;
    private static final int END_REF = -1;

    private final int tableSize, framesLimit, frameSize;

    private ByteBuffer[] headers, contents;
    private int[] frameNext;
    private int freeFramesCounter;

    private final IHyracksTaskContext ctx;

    private final IAggregatorDescriptor aggregator, merger;

    private final int[] keys, internalKeys;

    private final IBinaryComparator[] comparators;

    private final ITuplePartitionComputer tpc;

    private ByteBuffer outputBuffer;

    private RunFileWriter[] runWriters;

    private TuplePointer matchPointer;

    private final FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    private final FrameTupleAppenderForGroupHashtable internalAppender;

    private final FrameTupleAppender spilledPartitionAppender;

    private final FrameTupleAccessor spilledPartitionFrameAccessor;

    private final FrameTupleAppender outputAppender;

    /**
     * Tuple builder for hash table insertion
     */
    private final ArrayTupleBuilder internalTupleBuilder, outputTupleBuilder;

    private final int numOfPartitions;

    private int[] partitionBeginningFrame, partitionCurrentFrameIndex, partitionSizeInFrame, partitionSizeInTuple;

    private AggregateState[] partitionAggregateStates, partitionMergeStates;

    private BitSet partitionSpillFlags;

    private final IFrameWriter outputWriter;

    private int nextFreeFrameIndex;

    private int totalTupleCount;

    private final boolean pickLargestPartitionForFlush;

    private final RecordDescriptor outRecDesc;

    private final ITuplePartitionComputerFamily tpcf;

    private final int tupleCountOffsetInFrame;

    private int familyGenerateSeed;

    private final boolean hashSpilled = false;

    private final boolean allowReload;

    /**
     * For debugging
     */
    private static final Logger LOGGER = Logger.getLogger(HybridHashGroupHashTable.class.getSimpleName());

    int spilledGroups = 0, directOutputGroups = 0, reloadedSpilledGroups = 0, spilledFrames = 0, reloadedFrames = 0,
            spilledRuns = 0, reloadedRuns = 0;

    int maxHeaderCount, minHeaderCount;

    long comparsionCount = 0;

    long totalTimer = 0, insertTimer = 0, insertSpilledPartitionTimer = 0, findMatchTimerInNS = 0,
            flushSpilledPartitionTimer = 0, spillResidentPartitionTimer = 0, flushHashtableToOutputTimer = 0,
            finishupTimer = 0;
    long spilledFrameFlushTimerInNS = 0, residentFrameFlushTimerInNS = 0;

    int[] partitionRecordsInMemory, partitionUsedHashEntries, partitionRecordsInserted;

    public HybridHashGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize, int numOfPartitions,
            int procLevel, int[] keys, IBinaryComparator[] comparators, ITuplePartitionComputerFamily tpcf,
            IAggregatorDescriptor aggregator, IAggregatorDescriptor merger, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter, boolean allowReload) throws HyracksDataException {
        this(ctx, frameLimits, tableSize, numOfPartitions, procLevel, keys, comparators, tpcf, aggregator, merger,
                inRecDesc, outRecDesc, outputWriter, true, allowReload);
    }

    public HybridHashGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize, int numOfPartitions,
            int procLevel, int[] keys, IBinaryComparator[] comparators, ITuplePartitionComputerFamily tpcf,
            IAggregatorDescriptor aggregator, IAggregatorDescriptor merger, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter) throws HyracksDataException {
        this(ctx, frameLimits, tableSize, numOfPartitions, procLevel, keys, comparators, tpcf, aggregator, merger,
                inRecDesc, outRecDesc, outputWriter, true, true);
    }

    public HybridHashGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize, int numOfPartitions,
            int procLevel, int[] keys, IBinaryComparator[] comparators, ITuplePartitionComputerFamily tpcf,
            IAggregatorDescriptor aggregator, IAggregatorDescriptor merger, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter, boolean pickMostToFlush, boolean allowReload)
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
        this.merger = merger;

        this.familyGenerateSeed = procLevel;
        this.tpcf = tpcf;
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
        int possibleHeaderSize = HybridHashGroupHashTable.getHeaderSize(tableSize, frameSize);
        if (numOfPartitions > framesLimit - 1 - possibleHeaderSize) {
            this.headers = new ByteBuffer[0];
        } else {
            this.headers = new ByteBuffer[possibleHeaderSize];
        }

        this.numOfPartitions = numOfPartitions;

        this.maxHeaderCount = headers.length;
        this.minHeaderCount = headers.length;

        this.outputBuffer = ctx.allocateFrame();

        this.contents = new ByteBuffer[framesLimit - 1 - headers.length];
        this.frameNext = new int[this.contents.length];

        // reset the memory
        for (int i = 0; i < frameNext.length - 1; i++) {
            frameNext[i] = i + 1;
        }
        frameNext[frameNext.length - 1] = END_REF;
        this.nextFreeFrameIndex = 0;
        this.freeFramesCounter = frameNext.length;

        this.partitionCurrentFrameIndex = new int[this.numOfPartitions];
        this.partitionBeginningFrame = new int[this.numOfPartitions];
        this.partitionSizeInFrame = new int[this.numOfPartitions];
        this.runWriters = new RunFileWriter[this.numOfPartitions];
        this.partitionAggregateStates = new AggregateState[this.numOfPartitions];
        this.partitionMergeStates = new AggregateState[this.numOfPartitions];
        this.partitionSizeInTuple = new int[this.numOfPartitions];

        for (int i = 0; i < this.numOfPartitions; i++) {
            int newFrame = allocateFrame();
            partitionBeginningFrame[i] = newFrame;
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
                                HybridHashGroupHashTable.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    runWriters[i] = new RunFileWriter(runFile, ctx.getIOManager());
                }
                runWriters[i].open();
            }
        }

        this.totalTupleCount = 0;

        this.pickLargestPartitionForFlush = pickMostToFlush;

        this.outRecDesc = outRecDesc;

        this.allowReload = allowReload;

        // FIXME for debugging hash table
        this.partitionRecordsInMemory = new int[this.numOfPartitions];
        this.partitionUsedHashEntries = new int[this.numOfPartitions];
        this.partitionRecordsInserted = new int[this.numOfPartitions];

        totalTimer = System.currentTimeMillis();
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
        long timer = System.currentTimeMillis();

        int entry = tpc.partition(accessor, tupleIndex, tableSize);
        int pid = entry % numOfPartitions;

        if (partitionSpillFlags.get(pid)) {
            insertSpilledPartition(pid, accessor, tupleIndex);
            totalTupleCount++;
            partitionSizeInTuple[pid]++;
            partitionRecordsInMemory[pid]++;
        } else {
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
                    boolean needReinit = false;

                    // try to allocate a new frame
                    while (true) {
                        int newFrame = allocateFrame();
                        if (newFrame != END_REF) {
                            frameNext[newFrame] = partitionCurrentFrameIndex[pid];
                            partitionCurrentFrameIndex[pid] = newFrame;
                            partitionSizeInFrame[pid]++;
                            break;
                        } else {

                            // FIXME print hash table statistics
                            printHashtableStatistics();

                            // choose a partition to spill to make space
                            chooseResidentPartitionToFlush();
                            if (partitionSpillFlags.get(pid)) {
                                // in case that the current partition is spilled
                                needReinit = true;
                                break;
                            }
                        }
                    }

                    if (needReinit) {
                        insertSpilledPartition(pid, accessor, tupleIndex);
                    } else {
                        internalAppender.reset(contents[partitionCurrentFrameIndex[pid]], true);
                        if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to insert an aggregation value to the hash table.");
                        }
                    }
                }

                if (!partitionSpillFlags.get(pid)) {

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
                partitionSizeInTuple[pid]++;
                partitionRecordsInMemory[pid]++;
            }
        }
        partitionRecordsInserted[pid]++;

        // FIXME
        insertTimer += System.currentTimeMillis() - timer;
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
    private void insertSpilledPartition(int pid, FrameTupleAccessor accessor, int tupleIndex)
            throws HyracksDataException {
        // FIXME
        long timer = System.currentTimeMillis();

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
                    flushSpilledPartition(pid);
                }
            } else {
                //flush and reinsert
                flushSpilledPartition(pid);
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
        insertSpilledPartitionTimer += System.currentTimeMillis() - timer;
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
        int frameIndex = entry * 2 * INT_SIZE / frameSize;
        return frameIndex;
    }

    /**
     * Get the tuple index of the given hash table entry
     * 
     * @param entry
     * @return
     */
    private int getHeaderTupleIndex(int entry) {
        int offset = entry * 2 * INT_SIZE % frameSize;
        return offset;
    }

    private boolean findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex) {
        // FIXME
        long timer = System.nanoTime();

        // reset the match pointer
        matchPointer.frameIndex = -1;
        matchPointer.tupleIndex = -1;

        // get reference in the header
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderTupleIndex(entry);

        if (headers[headerFrameIndex] == null) {
            // FIXME
            findMatchTimerInNS += System.nanoTime() - timer;

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
                // FIXME
                findMatchTimerInNS += System.nanoTime() - timer;

                return true;
            }
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
        }
        // FIXME
        findMatchTimerInNS += System.nanoTime() - timer;

        return false;
    }

    /**
     * Flush a spilled partition. Records are directly flushed and appended
     * to the run file for this partition.
     * 
     * @param pid
     * @throws HyracksDataException
     */
    private void flushSpilledPartition(int pid) throws HyracksDataException {
        // FIXME
        long methodTimer = System.currentTimeMillis();
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

                for (int k = 0; k < internalKeys.length; k++) {
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
                    spilledFrameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

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
                partitionSizeInFrame[pid]++;
                break;
            }

        }
        if (outputAppender.getTupleCount() > 0) {
            // FIXME
            frameFlushTimer = System.nanoTime();
            FrameUtils.flushFrame(outputBuffer, runWriters[pid]);
            spilledFrameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

            outputAppender.reset(outputBuffer, true);
        }

        if (hashSpilled) {
            // reset hash headers belonging to this partition
            for (int i = pid; i < tableSize; i += numOfPartitions) {
                int headerFrameIndex = getHeaderFrameIndex(i);
                int headerFrameOffset = getHeaderTupleIndex(i);
                if (headers[headerFrameIndex] != null) {
                    headers[headerFrameIndex].putInt(headerFrameOffset, -1);
                    headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, -1);
                }
            }
        }
        if (partitionAggregateStates[pid] != null)
            partitionAggregateStates[pid].reset();

        // FIXME
        partitionRecordsInMemory[pid] = 0;
        partitionUsedHashEntries[pid] = 0;
        partitionRecordsInserted[pid] = 0;

        flushSpilledPartitionTimer += System.currentTimeMillis() - methodTimer;
    }

    /**
     * Spill a resident partition.
     * 
     * @param pid
     * @throws HyracksDataException
     */
    private void spillResidentPartition(int pid, boolean makeSpilledFrame) throws HyracksDataException {
        // FIXME
        long methodTimer = System.currentTimeMillis();
        long frameFlushTimer;

        outputAppender.reset(outputBuffer, true);
        int frameToFlush = partitionCurrentFrameIndex[pid];
        while (true) {
            hashtableRecordAccessor.reset(contents[frameToFlush]);
            int tupleCount = hashtableRecordAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                outputTupleBuilder.reset();

                int tupleOffset = hashtableRecordAccessor.getTupleStartOffset(i);
                int fieldOffset = hashtableRecordAccessor.getFieldCount() * INT_SIZE;

                for (int k = 0; k < internalKeys.length; k++) {
                    outputTupleBuilder.addField(hashtableRecordAccessor.getBuffer().array(), tupleOffset + fieldOffset
                            + hashtableRecordAccessor.getFieldStartOffset(i, k),
                            hashtableRecordAccessor.getFieldLength(i, k));
                }

                aggregator.outputPartialResult(outputTupleBuilder, hashtableRecordAccessor, i,
                        partitionAggregateStates[pid]);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    // FIXME
                    frameFlushTimer = System.nanoTime();
                    FrameUtils.flushFrame(outputBuffer, runWriters[pid]);
                    spilledFrameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

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
            if (!makeSpilledFrame || frameToFlush != END_REF) {
                recycleFrame(currentFrame);
                if (!makeSpilledFrame && frameToFlush == END_REF) {
                    break;
                }
            } else {
                partitionCurrentFrameIndex[pid] = currentFrame;
                contents[partitionCurrentFrameIndex[pid]].putInt(tupleCountOffsetInFrame, 0);
                partitionSizeInFrame[pid]++;
                break;
            }

        }
        if (outputAppender.getTupleCount() > 0) {
            // FIXME
            frameFlushTimer = System.nanoTime();
            FrameUtils.flushFrame(outputBuffer, runWriters[pid]);
            spilledFrameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

            outputAppender.reset(outputBuffer, true);
        }

        if (hashSpilled) {
            // reset hash headers belonging to this partition
            for (int i = pid; i < tableSize; i += numOfPartitions) {
                int headerFrameIndex = getHeaderFrameIndex(i);
                int headerFrameOffset = getHeaderTupleIndex(i);
                if (headers[headerFrameIndex] != null) {
                    headers[headerFrameIndex].putInt(headerFrameOffset, -1);
                    headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, -1);
                }
            }
        }
        if (partitionAggregateStates[pid] != null)
            partitionAggregateStates[pid].reset();

        // FIXME
        partitionRecordsInMemory[pid] = 0;
        partitionUsedHashEntries[pid] = 0;
        partitionRecordsInserted[pid] = 0;

        spillResidentPartitionTimer += System.currentTimeMillis() - methodTimer;
    }

    /**
     * Pick a resident partition to flush. This partition will be marked as a spilled partition, and
     * only one frame is assigned to it after flushing.
     * 
     * @throws HyracksDataException
     */
    private void chooseResidentPartitionToFlush() throws HyracksDataException {
        int partitionToPick = -1;
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
                if (partitionSizeInFrame[i] < partitionSizeInFrame[partitionToPick]) {
                    partitionToPick = i;
                }
            }
        }

        if (partitionToPick < 0 || partitionToPick >= numOfPartitions) {
            // no resident partition to spill
            return;
        }

        spilledRuns++;
        LOGGER.warning("HybridHashGroupHT-FlushSpill\t" + partitionToPick + "\t"
                + partitionSizeInFrame[partitionToPick] + "\t" + partitionSizeInTuple[partitionToPick] + "\t"
                + partitionRecordsInserted[partitionToPick]);

        // spill the picked partition
        int frameToFlush = partitionCurrentFrameIndex[partitionToPick];
        if (frameToFlush != END_REF) {
            if (runWriters[partitionToPick] == null) {
                FileReference runFile;
                try {
                    runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                            HybridHashGroupHashTable.class.getSimpleName());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                runWriters[partitionToPick] = new RunFileWriter(runFile, ctx.getIOManager());
            }
        }
        runWriters[partitionToPick].open();

        // flush the partition picked
        spillResidentPartition(partitionToPick, true);

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
        LinkedList<RunFileReader> runReaders = new LinkedList<RunFileReader>();

        for (int i = 0; i < numOfPartitions; i++) {
            if (runWriters[i] == null) {
                continue;
            }
            runReaders.add(runWriters[i].createReader());
        }

        return runReaders;
    }

    public LinkedList<Integer> getRunFileSizesInTuple() throws HyracksDataException {
        LinkedList<Integer> runSizesInTuple = new LinkedList<Integer>();

        for (int i = 0; i < numOfPartitions; i++) {
            if (runWriters[i] == null) {
                continue;
            }
            runSizesInTuple.add(partitionSizeInTuple[i]);
        }

        return runSizesInTuple;
    }

    public LinkedList<Integer> getRunFileSizesInFrame() throws HyracksDataException {
        LinkedList<Integer> runSizesInFrame = new LinkedList<Integer>();

        for (int i = 0; i < numOfPartitions; i++) {
            if (runWriters[i] == null) {
                continue;
            }
            runSizesInFrame.add(partitionSizeInFrame[i]);
        }

        return runSizesInFrame;
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
        return partitionSizeInTuple[pid];
    }

    public int getSpilledPartitionSizeInFrame(int pid) {
        return partitionBeginningFrame[pid];
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

            partitionBeginningFrame[i] = partitionCurrentFrameIndex[i];
            partitionSizeInTuple[i] = 0;

            if (partitionAggregateStates != null && partitionAggregateStates[i] != null) {
                partitionAggregateStates[i].reset();
            }

            partitionRecordsInMemory[i] = 0;
            partitionUsedHashEntries[i] = 0;
            partitionRecordsInserted[i] = 0;
        }

        partitionSpillFlags.clear();
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

        // FIXME print hash table statistics
        printHashtableStatistics();

        long methodTimer = System.currentTimeMillis();
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
                        residentFrameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

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
            residentFrameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

            outputAppender.reset(outputBuffer, true);
        }

        flushHashtableToOutputTimer += System.currentTimeMillis() - methodTimer;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTable#finishup()
     */
    @Override
    public void finishup() throws HyracksDataException {

        printHashtableStatistics();

        // FIXME
        long flushResPartTimer, frameFlushTimer;
        boolean hasResidentPart = false;

        // flush all spilled partitions
        for (int i = partitionSpillFlags.nextSetBit(0); i >= 0; i = partitionSpillFlags.nextSetBit(i + 1)) {
            flushSpilledPartition(i);
            runWriters[i].close();
        }

        // flush resident partitions
        outputAppender.reset(outputBuffer, true);
        for (int i = partitionSpillFlags.nextClearBit(0); i >= 0 && i < numOfPartitions; i = partitionSpillFlags
                .nextClearBit(i + 1)) {
            hasResidentPart = true;

            // FIXME
            flushResPartTimer = System.currentTimeMillis();

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
                        residentFrameFlushTimerInNS += System.nanoTime() - frameFlushTimer;

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

            flushHashtableToOutputTimer += System.currentTimeMillis() - flushResPartTimer;
        }

        if (totalTupleCount != 0) {
            LOGGER.severe("wrong tuple counter!");
        }

        int insertGroups = 0;

        for (int i = 0; i < partitionSizeInTuple.length; i++) {
            insertGroups += partitionSizeInTuple[i];
        }

        LOGGER.warning((hasResidentPart ? "Phase2i\t" : "Phase1i\t") + (System.currentTimeMillis() - totalTimer) + "\t"
                + insertGroups + "\t" + directOutputGroups + "\t" + spilledGroups + "\t" + headers.length + "\t"
                + comparsionCount + "\t" + numOfPartitions + "\t" + spilledRuns + "\t" + insertTimer + "\t"
                + insertSpilledPartitionTimer + "\t" + findMatchTimerInNS + "\t" + flushSpilledPartitionTimer + "\t"
                + spillResidentPartitionTimer + "\t" + flushHashtableToOutputTimer + "\t"
                + (System.currentTimeMillis() - finishupTimer) + "\t" + spilledFrameFlushTimerInNS + "\t"
                + residentFrameFlushTimerInNS);

        if (!allowReload) {
            if (outputAppender.getTupleCount() > 0) {
                FrameUtils.flushFrame(outputBuffer, outputWriter);
                outputAppender.reset(outputBuffer, true);
            }
            return;
        }

        // Start reloading partition

        FrameTupleAccessor runFrameAccessor = new FrameTupleAccessor(frameSize, outRecDesc);

        ITuplePartitionComputer runTpc = tpcf.createPartitioner(familyGenerateSeed + 1);

        // resize the contents to have one frame for loading buffer
        ByteBuffer[] shorterContents = new ByteBuffer[contents.length - 1];
        System.arraycopy(contents, 0, shorterContents, 0, contents.length - 1);
        ByteBuffer loadBuffer;
        if (contents[contents.length - 1] != null) {
            loadBuffer = contents[contents.length - 1];
        } else {
            loadBuffer = ctx.allocateFrame();
        }
        contents = shorterContents;

        // try to reload run files
        for (int i = partitionSpillFlags.nextSetBit(0); i >= 0; i = partitionSpillFlags.nextSetBit(i + 1)) {

            comparsionCount = 0;

            int newTableSize = tableSize;

            if (newTableSize > 2 * partitionSizeInTuple[i]) {
                newTableSize = 2 * partitionSizeInTuple[i];
            }

            int newHeaderCount = HybridHashGroupHashTable.getHeaderSize(newTableSize, frameSize);

            if (partitionSizeInFrame[i] > framesLimit - 2 - newHeaderCount) {
                // note that beside the output buffer, now we need to have one more frame for load buffer
                continue;
            }
            reloadedRuns++;

            // resize the hash headers for the given partition size in tuples
            ByteBuffer[] newHeaders = new ByteBuffer[newHeaderCount];
            ByteBuffer[] newContents = new ByteBuffer[framesLimit - 2 - newHeaderCount];
            if (newHeaders.length > headers.length) {
                int reallocateCount = newHeaders.length - headers.length;
                System.arraycopy(headers, 0, newHeaders, 0, headers.length);
                System.arraycopy(contents, 0, newHeaders, headers.length, reallocateCount);
                System.arraycopy(contents, reallocateCount, newContents, 0, newContents.length);
            } else if (headers.length > newHeaders.length) {
                int reallocateCount = headers.length - newHeaders.length;
                System.arraycopy(headers, 0, newHeaders, 0, newHeaders.length);
                System.arraycopy(headers, newHeaders.length, newContents, 0, reallocateCount);
                System.arraycopy(contents, 0, newContents, reallocateCount, contents.length);
            }
            this.headers = newHeaders;

            if (this.headers.length > maxHeaderCount) {
                maxHeaderCount = this.headers.length;
            }

            if (this.headers.length < minHeaderCount) {
                minHeaderCount = this.headers.length;
            }

            this.contents = newContents;
            if (frameNext.length != contents.length) {
                frameNext = null;
                frameNext = new int[contents.length];
            }

            // reset all header pages
            for (int j = 0; j < headers.length; j++) {
                if (headers[j] != null) {
                    resetHeader(j);
                }
            }

            // recycle all content frames
            for (int j = 0; j < contents.length; j++) {
                if (contents[j] != null)
                    contents[j].putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
                if (j < contents.length - 1) {
                    frameNext[j] = j + 1;
                } else {
                    frameNext[j] = END_REF;
                }
            }
            this.nextFreeFrameIndex = 0;
            this.freeFramesCounter = contents.length;

            // assign the initial page
            int newFrame = allocateFrame();
            partitionCurrentFrameIndex[i] = newFrame;
            frameNext[newFrame] = END_REF;

            // reset load buffer
            loadBuffer.putInt(FrameHelper.getTupleCountOffset(frameSize), 0);

            // reload the run file
            RunFileReader runReader = runWriters[i].createReader();
            runReader.open();

            while (runReader.nextFrame(loadBuffer)) {

                runFrameAccessor.reset(loadBuffer);
                int tupleCount = runFrameAccessor.getTupleCount();
                for (int t = 0; t < tupleCount; t++) {
                    int entry = runTpc.partition(runFrameAccessor, t, newTableSize);
                    inMemInsert(runFrameAccessor, t, entry, i);
                }
            }

            runReader.close();

            // FIXME
            printHashtableStatistics(newTableSize, i);

            // flush the memory
            flushInMemPartition(i);

            // remove the run file from the list
            runWriters[i] = null;

        }

        if (outputAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, outputWriter);
            outputAppender.reset(outputBuffer, true);
        }
    }

    private void flushInMemPartition(int pid) throws HyracksDataException {
        outputAppender.reset(outputBuffer, false);
        int frameToFlush = partitionCurrentFrameIndex[pid];
        while (frameToFlush != END_REF) {
            hashtableRecordAccessor.reset(contents[frameToFlush]);
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

                merger.outputFinalResult(outputTupleBuilder, hashtableRecordAccessor, j, partitionMergeStates[pid]);

                if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(), outputTupleBuilder.getByteArray(),
                        0, outputTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);

                    outputAppender.reset(outputBuffer, true);
                    if (!outputAppender.append(outputTupleBuilder.getFieldEndOffsets(),
                            outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to flush the hash table to the final output");
                    }
                }
                totalTupleCount--;
                reloadedSpilledGroups++;
            }
            int currentFrame = frameToFlush;
            frameToFlush = frameNext[currentFrame];
            recycleFrame(currentFrame);
        }

        if (partitionMergeStates[pid] != null)
            partitionMergeStates[pid].reset();

        partitionRecordsInMemory[pid] = 0;
        partitionUsedHashEntries[pid] = 0;
        partitionRecordsInserted[pid] = 0;
    }

    public void inMemInsert(FrameTupleAccessor accessor, int tupleIndex, int entry, int pid)
            throws HyracksDataException {

        if (findMatch(entry, accessor, tupleIndex)) {
            // find match; do aggregation
            hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
            merger.aggregate(accessor, tupleIndex, hashtableRecordAccessor, matchPointer.tupleIndex,
                    partitionMergeStates[pid]);
        } else {

            if (partitionMergeStates[pid] == null) {
                partitionMergeStates[pid] = merger.createAggregateStates();
            }

            internalTupleBuilder.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
            }
            merger.init(internalTupleBuilder, accessor, tupleIndex, partitionMergeStates[pid]);

            internalAppender.reset(contents[partitionCurrentFrameIndex[pid]], false);
            if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                    internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {

                // try to allocate a new frame
                int newFrame = allocateFrame();
                if (newFrame != END_REF) {
                    frameNext[newFrame] = partitionCurrentFrameIndex[pid];
                    partitionCurrentFrameIndex[pid] = newFrame;
                    partitionBeginningFrame[pid]++;
                } else {
                    throw new HyracksDataException("Failed to reload a spilled partition for im-memory processing.");
                }
                internalAppender.reset(contents[partitionCurrentFrameIndex[pid]], true);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                    throw new HyracksDataException("Failed to insert an aggregation value to the hash table.");
                }
            }

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
                headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE, internalAppender.getTupleCount() - 1);
                partitionUsedHashEntries[pid]++;
            } else {
                // update the previous reference
                hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                contents[matchPointer.frameIndex].putInt(refOffset, partitionCurrentFrameIndex[pid]);
                contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE, internalAppender.getTupleCount() - 1);
            }

            totalTupleCount++;

            partitionRecordsInMemory[pid]++;
        }
        partitionRecordsInserted[pid]++;
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

        partitionBeginningFrame = null;
        partitionCurrentFrameIndex = null;
        partitionRecordsInMemory = null;
        partitionSizeInFrame = null;
        partitionSizeInTuple = null;
        partitionUsedHashEntries = null;
        partitionRecordsInserted = null;

        partitionAggregateStates = null;
        partitionMergeStates = null;
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

    public static int getHeaderSize(int tableSize, int frameSize) {
        int residual = tableSize * INT_SIZE * 2 % frameSize == 0 ? 0 : 1;
        return tableSize * INT_SIZE * 2 / frameSize + residual;
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
        int actualTableSize = tableSize * (numOfPartitions - partitionSpillFlags.cardinality()) / numOfPartitions;
        LOGGER.warning("HybridHashGroupHashTable-HashtableStatistics\t" + actualTableSize + "\t" + usedHashSlots + "\t"
                + recInHashTable + "\t" + recordInserted);
        //        if (usedHashSlots != 0 && recInHashTable / usedHashSlots >= 2) {
        //            // print the hash table distribution
        //            long startTimers = System.currentTimeMillis();
        //            StringBuilder sbder = new StringBuilder();
        //            for (int i = 0; i < tableSize; i++) {
        //                int pid = i % numOfPartitions;
        //                if (!hashSpilled && partitionSpillFlags.get(pid)) {
        //                    continue;
        //                }
        //                int cnt = getEntryLength(i);
        //                if (cnt > 0)
        //                    sbder.append(i).append(" ").append(cnt).append("\n");
        //            }
        //            LOGGER.warning("HashtableHistogram\t" + (System.currentTimeMillis() - startTimers) + "\n"
        //                    + sbder.toString());
        //        }
    }

    private void printHashtableStatistics(int actualTableSize, int pid) {
        LOGGER.warning("HybridHashGroupHashTable-HashtableStatistics\t" + actualTableSize + "\t"
                + partitionUsedHashEntries[pid] + "\t" + partitionRecordsInMemory[pid] + "\t"
                + partitionRecordsInserted[pid]);
        //        if (partitionUsedHashEntries[pid] != 0 && partitionRecordsInMemory[pid] / partitionUsedHashEntries[pid] >= 2) {
        //            // print the hash table distribution
        //            long startTimers = System.currentTimeMillis();
        //            StringBuilder sbder = new StringBuilder();
        //            for (int i = 0; i < tableSize; i++) {
        //                if (i % numOfPartitions != pid) {
        //                    continue;
        //                }
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
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            // Move to the next record in this entry following the linked list
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
            cnt++;
        }

        return cnt;
    }

}
