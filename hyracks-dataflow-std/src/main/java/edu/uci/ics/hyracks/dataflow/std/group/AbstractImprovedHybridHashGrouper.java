/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;

public abstract class AbstractImprovedHybridHashGrouper implements IFrameAllocator {

    public final int NO_MORE_FREE_BUFFER = -1;
    private final int END_OF_PARTITION = -1;
    private final int INVALID_BUFFER = -2;
    private final int UNALLOCATED_FRAME = -3;

    private final static int INT_SIZE = 4;

    private final static int HASH_HEADER_COUNT_CHECKER = Integer.MAX_VALUE / (INT_SIZE * 2);

    private RunFileWriter[] spilledPartitionWriters;

    private final IHyracksTaskContext ctx;

    private final int framesLimit;

    private final int frameSize;

    private final int[] keys, storedKeys;

    private final ITuplePartitionComputer aggregatePartitionComputer;

    private final IAggregatorDescriptor aggregator, merger;

    private AggregateState[] partitionAggregateStates;

    private AggregateState mergeState;

    private final IFrameWriter finalWriter;

    private final FrameTupleAccessor inputAccessor, partitionAccessor;

    private final TupleInFrameAccessor hashtableEntryAccessor;

    private final FrameTupleAppender appender;

    private ByteBuffer[] frames;

    protected final int numOfPartitions;

    protected final int hashtableSize;

    protected int[] partitionBeginningFrame, partitionCurrentFrame, framesNext, partitionSizeInTuple,
            partitionSizeInFrame, hashtableEntrySize, framesOffset;

    // hashtable headers
    private int[] hashtableHeaders;

    private int nextFreeFrameIndex, freeFramesCounter;

    /**
     * Status of the partition (0: resident partition, 1: spilled partition)
     */
    protected BitSet partitionStatus;

    /**
     * Tuple builder for aggregation
     */
    private ArrayTupleBuilder internalTupleBuilderForInsert, internalTupleBuilderForFlush;

    private int hashtableSizeForPartition;

    private ByteBuffer outputBuffer;

    private FrameTupleAccessor mergeAccessor;

    private RecordDescriptor internalRecordDescriptor;

    private ITuplePartitionComputer mergePartitionComputer;

    private final IBinaryComparator[] comparators;

    /*
     * For instrumenting
     */

    private int residentPartitionCount = 0, spilledPartitionCount = 0, reloadSpilledPartitionCount = 0;

    private static final Logger LOGGER = Logger.getLogger(AbstractImprovedHybridHashGrouper.class.getSimpleName());

    public AbstractImprovedHybridHashGrouper(IHyracksTaskContext ctx, int framesLimit, int[] keys, int[] storedKeys,
            int targetNumOfPartitions, int hashtableSize, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            IBinaryComparator[] comparators, ITuplePartitionComputer aggregateHashtablePartitionComputer,
            ITuplePartitionComputer mergeHashtablePartitionComputer, IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory, IFrameWriter finalOutputWriter) throws HyracksDataException {
        this.framesLimit = framesLimit;
        this.keys = keys;
        this.hashtableSize = hashtableSize;
        this.storedKeys = storedKeys;
        this.aggregatePartitionComputer = aggregateHashtablePartitionComputer;
        this.mergePartitionComputer = mergeHashtablePartitionComputer;
        this.finalWriter = finalOutputWriter;
        this.ctx = ctx;
        this.frameSize = ctx.getFrameSize();
        this.comparators = comparators;

        // initialize the accessors and appenders
        this.internalRecordDescriptor = outRecDesc;

        if (keys.length >= outRecDesc.getFields().length) {
            // for the case of zero-aggregations
            ISerializerDeserializer<?>[] fields = outRecDesc.getFields();
            ITypeTraits[] types = outRecDesc.getTypeTraits();
            ISerializerDeserializer<?>[] newFields = new ISerializerDeserializer[fields.length + 1];
            for (int i = 0; i < fields.length; i++)
                newFields[i] = fields[i];
            ITypeTraits[] newTypes = null;
            if (types != null) {
                newTypes = new ITypeTraits[types.length + 1];
                for (int i = 0; i < types.length; i++)
                    newTypes[i] = types[i];
            }
            internalRecordDescriptor = new RecordDescriptor(newFields, newTypes);
        }

        this.inputAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
        this.partitionAccessor = new FrameTupleAccessor(ctx.getFrameSize(), internalRecordDescriptor);
        this.hashtableEntryAccessor = new TupleInFrameAccessor(internalRecordDescriptor);
        this.appender = new FrameTupleAppender(ctx.getFrameSize());
        this.internalTupleBuilderForInsert = new ArrayTupleBuilder(internalRecordDescriptor.getFieldCount());
        this.internalTupleBuilderForFlush = new ArrayTupleBuilder(internalRecordDescriptor.getFieldCount());

        // initialize the aggregator and merger
        this.aggregator = aggregatorFactory.createAggregator(ctx, inRecDesc, outRecDesc, keys, storedKeys);
        this.merger = mergerFactory.createAggregator(ctx, outRecDesc, outRecDesc, storedKeys, storedKeys);

        // set the free frames linked list
        // - free frame starts from beginning
        nextFreeFrameIndex = 0;
        // - next links for frames
        frames = new ByteBuffer[framesLimit];
        framesNext = new int[framesLimit];
        framesOffset = new int[framesLimit];
        for (int i = 0; i < framesNext.length; i++) {
            framesNext[i] = UNALLOCATED_FRAME;
            framesOffset[i] = 0;
        }
        freeFramesCounter = framesLimit;

        int hashtableHeaderCount = getHeaderPageCount(hashtableSize, ctx.getFrameSize());
        if (hashtableHeaderCount > framesLimit) {
            throw new HyracksDataException("Not enough frames to initialize the hash table: " + hashtableHeaderCount
                    + " required, but only " + framesLimit + " available.");
        }

        this.numOfPartitions = getNumOfPartitions(targetNumOfPartitions, hashtableHeaderCount, ctx.getFrameSize());

        // initialize the aggregate state
        this.partitionAggregateStates = new AggregateState[numOfPartitions];
        for (int i = 0; i < numOfPartitions; i++) {
            partitionAggregateStates[i] = aggregator.createAggregateStates();
        }

        // initialize the header pages: assign a block of frames for hashtable headers
        this.hashtableHeaders = new int[hashtableHeaderCount];
        int headerFrameIndex = allocateFrame();
        this.hashtableHeaders[0] = headerFrameIndex;
        for (int i = 1; i < hashtableHeaderCount; i++) {
            int newHeaderFrameIndex = allocateFrameForOverflowing(headerFrameIndex);
            this.hashtableHeaders[i] = newHeaderFrameIndex;
            headerFrameIndex = newHeaderFrameIndex;
        }
        for (int i = 0; i < hashtableHeaderCount; i++) {
            for (int j = 0; j < frameSize; j += INT_SIZE) {
                frames[hashtableHeaders[i]].putInt(j, -1);
            }
        }

        // initialize frame for each partition

        hashtableSizeForPartition = hashtableSize / numOfPartitions;

        partitionStatus = new BitSet(numOfPartitions);
        partitionBeginningFrame = new int[numOfPartitions];
        partitionCurrentFrame = new int[numOfPartitions];
        partitionSizeInTuple = new int[numOfPartitions];
        partitionSizeInFrame = new int[numOfPartitions];
        hashtableEntrySize = new int[numOfPartitions];
        this.spilledPartitionWriters = new RunFileWriter[numOfPartitions];
        for (int i = 0; i < numOfPartitions; i++) {
            int newFrame = allocateFrame();
            if (newFrame == NO_MORE_FREE_BUFFER) {
                throw new HyracksDataException("Not enough memory for the partition plan");
            }
            partitionBeginningFrame[i] = newFrame;
            partitionCurrentFrame[i] = newFrame;
            partitionSizeInTuple[i] = 0;
            partitionSizeInFrame[i] = 1;
            framesNext[newFrame] = END_OF_PARTITION;
            hashtableEntrySize[i] = hashtableSizeForPartition;
        }

        // for output
        outputBuffer = frames[allocateFrame()];
        LOGGER.info("Initialized Hybrid-hash-grouper: " + numOfPartitions + " Partitions, " + hashtableSize
                + " Hashvals, " + hashtableHeaderCount + " Header Pages");
    }

    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inputAccessor.reset(buffer);
        int tupleCount = inputAccessor.getTupleCount();
        for (int i = 0; i < tupleCount; i++) {
            int hid = aggregatePartitionComputer.partition(inputAccessor, i, hashtableSize);
            int pid = hid % numOfPartitions;
            insert(inputAccessor, i, pid, hid);
        }
    }

    private void insert(FrameTupleAccessor accessor, int tupleIndex, int pid, int hid) throws HyracksDataException {
        if (!partitionStatus.get(pid)) {
            insertResidentPartitionRecord(keys, accessor, tupleIndex, pid, hid, aggregator,
                    partitionAggregateStates[pid], true);
        } else {
            // spilled partition
            // Spilled partition: insert into the partition only, and flush when necessary
            appender.reset(frames[partitionCurrentFrame[pid]], false);
            // - build the aggregation value
            internalTupleBuilderForInsert.reset();
            for (int k = 0; k < keys.length; k++) {
                internalTupleBuilderForInsert.addField(accessor, tupleIndex, keys[k]);
            }
            aggregator.init(internalTupleBuilderForInsert, accessor, tupleIndex, partitionAggregateStates[pid]);
            if (!appender.appendSkipEmptyField(internalTupleBuilderForInsert.getFieldEndOffsets(),
                    internalTupleBuilderForInsert.getByteArray(), 0, internalTupleBuilderForInsert.getSize())) {
                flushSpilledPartition(pid);
                partitionSizeInFrame[pid]++;
                appender.reset(frames[partitionCurrentFrame[pid]], true);
                if (!appender.appendSkipEmptyField(internalTupleBuilderForInsert.getFieldEndOffsets(),
                        internalTupleBuilderForInsert.getByteArray(), 0, internalTupleBuilderForInsert.getSize())) {
                    throw new HyracksDataException("Failed to insert a tuple into a spilled partition.");
                }
            }

            partitionSizeInTuple[pid]++;
        }
    }

    private void insertResidentPartitionRecord(int[] recKeys, FrameTupleAccessor accessor, int tupleIndex, int pid,
            int hid, IAggregatorDescriptor grouper, AggregateState groupState, boolean allowSpill)
            throws HyracksDataException {
        // resident partition
        // - find the header record
        int headerFrameIndex = hid * 2 * INT_SIZE / frameSize;
        int headerFrameOffset = hid * 2 * INT_SIZE % frameSize;

        int pointerFrameIndex = hashtableHeaders[headerFrameIndex], pointerFrameOffset = headerFrameOffset;

        int entryFrameIndex = frames[pointerFrameIndex].getInt(headerFrameOffset);
        int entryFrameOffset = frames[pointerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        do {
            if (entryFrameIndex < 0) {
                break;
            }
            int actualFrameIndex = getHashtablePartitionFrame(pid, entryFrameIndex);
            ByteBuffer bufferContainsNextEntry = frames[actualFrameIndex];

            hashtableEntryAccessor.reset(bufferContainsNextEntry, entryFrameOffset);
            int c = compare(recKeys, accessor, tupleIndex, storedKeys, hashtableEntryAccessor);
            if (c == 0) {
                break;
            }
            // move to the next entry along the linked list
            pointerFrameIndex = actualFrameIndex;
            pointerFrameOffset = entryFrameOffset + hashtableEntryAccessor.getTupleLength();

            entryFrameIndex = bufferContainsNextEntry.getInt(pointerFrameOffset);
            entryFrameOffset = bufferContainsNextEntry.getInt(pointerFrameOffset + INT_SIZE);
        } while (true);

        if (entryFrameIndex >= 0) {
            // Found the entry; do aggregation
            grouper.aggregate(accessor, tupleIndex, hashtableEntryAccessor, groupState);

        } else {
            // No matching entry; do insertion
            int frameIndexToInsert = partitionSizeInFrame[pid] - 1;
            int frameOffsetToInsert = framesOffset[partitionCurrentFrame[pid]];
            // check whether there is enough space for the insertion
            // - build the aggregation value
            internalTupleBuilderForInsert.reset();
            for (int k = 0; k < recKeys.length; k++) {
                internalTupleBuilderForInsert.addField(accessor, tupleIndex, recKeys[k]);
            }

            int initLength = internalTupleBuilderForInsert.getSize() + grouper.getInitSize(accessor, tupleIndex);
            int fieldCount = recKeys.length + grouper.getFieldCount();

            if (frameSize - frameOffsetToInsert < fieldCount * INT_SIZE + initLength + INT_SIZE * 2) {

                int newPartitionFrameIndex = allocateFrameForOverflowing(partitionCurrentFrame[pid]);
                if (newPartitionFrameIndex == NO_MORE_FREE_BUFFER) {
                    if (allowSpill) {
                        // need to make some space
                        int pidToSpill = selectPartitionToSpill();
                        if (pidToSpill == -1) {
                            throw new HyracksDataException("Cannot allocate memory for a resident partition.");
                        }
                        spillResidentPartition(pidToSpill);
                        insert(accessor, tupleIndex, pid, hid);
                        return;
                    } else {
                        throw new HyracksDataException("Not enough space for processing a resident partition");
                    }
                }

                partitionCurrentFrame[pid] = newPartitionFrameIndex;
                framesOffset[newPartitionFrameIndex] = 0;
                partitionSizeInFrame[pid]++;

                frameIndexToInsert = partitionSizeInFrame[pid] - 1;
                frameOffsetToInsert = framesOffset[partitionCurrentFrame[pid]];
                if (frameSize - frameOffsetToInsert < fieldCount * INT_SIZE + initLength + INT_SIZE * 2) {
                    throw new HyracksDataException("Failed to allocate memory for an aggregation tuple.");
                }
            }

            frames[pointerFrameIndex].putInt(pointerFrameOffset, frameIndexToInsert);
            frames[pointerFrameIndex].putInt(pointerFrameOffset + INT_SIZE, frameOffsetToInsert);

            grouper.init(internalTupleBuilderForInsert, accessor, tupleIndex, groupState);

            for (int i = 0; i < internalTupleBuilderForInsert.getFieldEndOffsets().length; i++) {
                frames[partitionCurrentFrame[pid]].putInt(frameOffsetToInsert + i * INT_SIZE,
                        internalTupleBuilderForInsert.getFieldEndOffsets()[i]);
            }
            System.arraycopy(internalTupleBuilderForInsert.getByteArray(), 0,
                    frames[partitionCurrentFrame[pid]].array(),
                    frameOffsetToInsert + internalTupleBuilderForInsert.getFieldEndOffsets().length * INT_SIZE,
                    internalTupleBuilderForInsert.getSize());

            int linkedListRefOffset = frameOffsetToInsert + internalTupleBuilderForInsert.getFieldEndOffsets().length
                    * INT_SIZE + internalTupleBuilderForInsert.getSize();

            frames[partitionCurrentFrame[pid]].putInt(linkedListRefOffset, -1);
            frames[partitionCurrentFrame[pid]].putInt(linkedListRefOffset + INT_SIZE, -1);

            // update the frame offset
            framesOffset[partitionCurrentFrame[pid]] = linkedListRefOffset + INT_SIZE * 2;

            partitionSizeInTuple[pid]++;
        }
    }

    public void close() throws HyracksDataException {
        // flush all resident partitions
        for (int i = 0; i < numOfPartitions; i++) {
            if (!partitionStatus.get(i)) {
                flushResidentPartition(i, finalWriter, aggregator, partitionAggregateStates[i], false);
                residentPartitionCount++;
            } else {
                if (frames[partitionCurrentFrame[i]].getInt(frames[partitionCurrentFrame[i]].capacity() - 4) > 0)
                    flushSpilledPartition(i);
                recycleFrame(partitionCurrentFrame[i]);
                partitionCurrentFrame[i] = UNALLOCATED_FRAME;
                partitionSizeInFrame[i]++;
                if (spilledPartitionWriters[i] != null) {
                    spilledPartitionWriters[i].close();
                }
                spilledPartitionCount++;
            }
        }

        partitionAggregateStates = null;

        if (this.mergeState == null)
            this.mergeState = merger.createAggregateStates();

        // clean up the header frames
        for (int i = 0; i < hashtableHeaders.length; i++) {
            recycleFrame(hashtableHeaders[i]);
        }

        hashtableHeaders = null;

        // Select spilled partition to reload
        RunFileReader runReader;
        int runFileBufferIndex = allocateFrame();
        ByteBuffer runFileBuffer = frames[runFileBufferIndex];

        int newHeadPagesCount = getHeaderPageCount(hashtableSize / numOfPartitions, frameSize);

        for (int i = 0; i < numOfPartitions; i++) {
            if (partitionStatus.get(i)) {
                mergeState.reset();
                // check if the partition can be maintained in the main memory
                if (partitionSizeInFrame[i] > 0 && partitionSizeInFrame[i] + newHeadPagesCount < freeFramesCounter) {
                    reloadSpilledPartitionCount++;
                    LOGGER.warning("HybridHashGrouper-Merge " + partitionSizeInFrame[i] + " " + partitionSizeInTuple[i]);

                    partitionCurrentFrame[i] = allocateFrame();
                    if (partitionCurrentFrame[i] < 0) {
                        throw new HyracksDataException("Cannot allocate frame for merging.");
                    }
                    framesOffset[partitionCurrentFrame[i]] = 0;
                    // reset the size counter
                    partitionSizeInFrame[i] = 1;
                    partitionSizeInTuple[i] = 0;

                    framesNext[partitionCurrentFrame[i]] = END_OF_PARTITION;

                    // initialize the hash table header pages
                    hashtableHeaders = new int[newHeadPagesCount];
                    int headerFrameIndex = allocateFrame();
                    this.hashtableHeaders[0] = headerFrameIndex;
                    for (int k = 0; k < frameSize; k += INT_SIZE) {
                        frames[this.hashtableHeaders[0]].putInt(k, -1);
                    }
                    for (int j = 1; j < hashtableHeaders.length; j++) {
                        int newHeaderFrameIndex = allocateFrameForOverflowing(headerFrameIndex);
                        this.hashtableHeaders[j] = newHeaderFrameIndex;
                        headerFrameIndex = newHeaderFrameIndex;
                        for (int k = 0; k < frameSize; k += INT_SIZE) {
                            frames[newHeaderFrameIndex].putInt(k, -1);
                        }
                    }

                    runReader = spilledPartitionWriters[i].createReader();
                    runReader.open();
                    if (mergeAccessor == null) {
                        mergeAccessor = new FrameTupleAccessor(ctx.getFrameSize(), internalRecordDescriptor);
                    }
                    while (runReader.nextFrame(runFileBuffer)) {
                        mergeAccessor.reset(runFileBuffer);
                        int tupleCount = mergeAccessor.getTupleCount();
                        for (int j = 0; j < tupleCount; j++) {
                            merge(mergeAccessor, j, i);
                        }
                    }
                    // close the run file reader and writer
                    runReader.close();
                    spilledPartitionWriters[i].close();
                    spilledPartitionWriters[i] = null;
                    // flush the in-memory hash table into the output
                    flushResidentPartition(i, finalWriter, merger, mergeState, false);

                    // set the spilled flag to be empty
                    partitionStatus.clear(i);
                }
            }
        }
        LOGGER.warning("HybridHashGrouper " + residentPartitionCount + " " + spilledPartitionCount + " "
                + reloadSpilledPartitionCount);
    }

    private void merge(FrameTupleAccessor accessor, int tupleIndex, int pid) throws HyracksDataException {
        int hid = mergePartitionComputer.partition(accessor, tupleIndex, hashtableEntrySize[pid]);
        insertResidentPartitionRecord(storedKeys, accessor, tupleIndex, pid, hid, merger, mergeState, false);
    }

    protected void spillResidentPartition(int pid) throws HyracksDataException {
        // flush the data partition
        RunFileWriter writer = spilledPartitionWriters[pid];
        if (writer == null) {
            writer = new RunFileWriter(ctx.getJobletContext().createManagedWorkspaceFile(
                    AbstractImprovedHybridHashGrouper.class.getSimpleName()), ctx.getIOManager());
            writer.open();
            spilledPartitionWriters[pid] = writer;
        }
        flushResidentPartition(pid, writer, aggregator, partitionAggregateStates[pid], true);
        partitionStatus.set(pid);
    }

    protected void flushSpilledPartition(int pid) throws HyracksDataException {
        RunFileWriter writer = spilledPartitionWriters[pid];
        if (writer == null) {
            writer = new RunFileWriter(ctx.getJobletContext().createManagedWorkspaceFile(
                    AbstractImprovedHybridHashGrouper.class.getName()), ctx.getIOManager());
            writer.open();
            spilledPartitionWriters[pid] = writer;
        }
        flushSpilledPartitionToWriter(pid, writer);
    }

    private void flushSpilledPartitionToWriter(int pid, IFrameWriter writer) throws HyracksDataException {
        int spillPartFrame = partitionCurrentFrame[pid];
        int firstPartFrame = spillPartFrame;
        appender.reset(outputBuffer, true);
        while (spillPartFrame != END_OF_PARTITION) {
            partitionAccessor.reset(frames[spillPartFrame]);
            int tupleCount = partitionAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                internalTupleBuilderForFlush.reset();
                for (int k = 0; k < storedKeys.length; k++) {
                    internalTupleBuilderForFlush.addField(partitionAccessor, i, storedKeys[k]);
                }
                aggregator.outputPartialResult(internalTupleBuilderForFlush, partitionAccessor, i,
                        partitionAggregateStates[pid]);
                if (!appender.appendSkipEmptyField(internalTupleBuilderForFlush.getFieldEndOffsets(),
                        internalTupleBuilderForFlush.getByteArray(), 0, internalTupleBuilderForFlush.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, writer);
                    appender.reset(outputBuffer, true);
                    if (!appender.appendSkipEmptyField(internalTupleBuilderForFlush.getFieldEndOffsets(),
                            internalTupleBuilderForFlush.getByteArray(), 0, internalTupleBuilderForFlush.getSize())) {
                        throw new HyracksDataException("Failed to flush a spilled partition.");
                    }
                }
            }

            framesOffset[spillPartFrame] = 0;
            int nextFrame = framesNext[spillPartFrame];
            if (spillPartFrame != firstPartFrame) {
                recycleFrame(spillPartFrame);
            }
            spillPartFrame = nextFrame;
        }
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, writer);
        }
        frames[firstPartFrame].clear();
        frames[firstPartFrame].putInt(frames[firstPartFrame].capacity() - 4, 0);
        framesNext[firstPartFrame] = END_OF_PARTITION;
        // reset the aggregate state
        partitionAggregateStates[pid].reset();
    }

    private void flushResidentPartition(int pid, IFrameWriter writer, IAggregatorDescriptor grouper,
            AggregateState groupState, boolean isPartial) throws HyracksDataException {
        int residentPartFrame = partitionCurrentFrame[pid];
        int firstPartFrame = residentPartFrame;
        appender.reset(outputBuffer, true);
        while (residentPartFrame != END_OF_PARTITION) {
            int frameOffset = 0;

            while (frameOffset < framesOffset[residentPartFrame]) {
                hashtableEntryAccessor.reset(frames[residentPartFrame], frameOffset);
                int tupleLength = hashtableEntryAccessor.getTupleLength();
                int fieldOffsetLength = hashtableEntryAccessor.getFieldCount() * INT_SIZE;
                internalTupleBuilderForFlush.reset();

                for (int k = 0; k < storedKeys.length; k++) {
                    internalTupleBuilderForFlush.addField(frames[residentPartFrame].array(), frameOffset
                            + fieldOffsetLength + hashtableEntryAccessor.getFieldStartOffset(k),
                            hashtableEntryAccessor.getFieldLength(k));
                }

                if (isPartial)
                    grouper.outputPartialResult(internalTupleBuilderForFlush, frames[residentPartFrame].array(),
                            frameOffset, tupleLength, hashtableEntryAccessor.getFieldCount(), INT_SIZE, groupState);
                else
                    grouper.outputFinalResult(internalTupleBuilderForFlush, frames[residentPartFrame].array(),
                            frameOffset, tupleLength, hashtableEntryAccessor.getFieldCount(), INT_SIZE, groupState);

                if (!appender.append(internalTupleBuilderForFlush.getFieldEndOffsets(),
                        internalTupleBuilderForFlush.getByteArray(), 0, internalTupleBuilderForFlush.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, writer);
                    appender.reset(outputBuffer, true);
                    if (!appender.append(internalTupleBuilderForFlush.getFieldEndOffsets(),
                            internalTupleBuilderForFlush.getByteArray(), 0, internalTupleBuilderForFlush.getSize())) {
                        throw new HyracksDataException("Failed to flush a spilled partition.");
                    }
                }

                frameOffset += tupleLength + INT_SIZE * 2;
            }

            // release the frame
            framesOffset[residentPartFrame] = 0;
            int nextFrame = framesNext[residentPartFrame];
            if (residentPartFrame != firstPartFrame) {
                recycleFrame(residentPartFrame);
            }
            residentPartFrame = nextFrame;
        }
        if (appender.getTupleCount() > 0) {
            FrameUtils.flushFrame(outputBuffer, writer);
        }
        frames[firstPartFrame].clear();
        frames[firstPartFrame].putInt(frames[firstPartFrame].capacity() - 4, 0);
        framesNext[firstPartFrame] = END_OF_PARTITION;
    }

    /**
     * Get the number of header pages for the built-in hashtable.
     * <p/>
     * The number of header pages for the hashtable is calculated by dividing the total memory usage of all hashtable keys over the frame size. Each hashtable key takes two integer-length fields, indicating the frame index and the tuple index of the first entry with this hash key value.
     * <p/>
     * 
     * @param hashtableSize
     * @param frameSize
     * @return
     */
    private int getHeaderPageCount(int hashtableSize, int frameSize) {
        int hashtableHeaderPageResidual = (hashtableSize % frameSize) * (INT_SIZE * 2) % frameSize == 0 ? 0 : 1;
        if (hashtableSize <= HASH_HEADER_COUNT_CHECKER)
            return hashtableSize * (INT_SIZE * 2) / frameSize + hashtableHeaderPageResidual;
        else
            return hashtableSize / frameSize * (INT_SIZE * 2) + hashtableHeaderPageResidual;
    }

    /**
     * Get the number of partitions.
     * <p/>
     * The number of partition is decided by the target number of partitions, and also the size of the hashtable. The goal is to contain the whole hash table header, and for each partition at least one page can be assigned.
     * <p/>
     * In case that the target number of partitions is too large, or the available frame size is too small, the number of partition will be adjusted to be the available frames except for the headers and other reversed frames.
     * <p/>
     * 
     * @param targetNumOfPartitions
     * @param hashtableHeaderPageCount
     * @param frameSize
     * @return
     */
    private int getNumOfPartitions(int targetNumOfPartitions, int hashtableHeaderPageCount, int frameSize) {
        if (targetNumOfPartitions + hashtableHeaderPageCount + getReservedFramesCount() > framesLimit) {
            return framesLimit - getReservedFramesCount() - hashtableHeaderPageCount;
        } else {
            return targetNumOfPartitions;
        }
    }

    private void recycleFrame(int frameIdx) {
        frames[frameIdx].clear();
        framesOffset[frameIdx] = 0;
        frames[frameIdx].putInt(frames[frameIdx].capacity() - 4, 0);
        int oldFreeHead = nextFreeFrameIndex;
        nextFreeFrameIndex = frameIdx;
        framesNext[nextFreeFrameIndex] = oldFreeHead;
        freeFramesCounter++;
    }

    /**
     * Allocate frame in case of the given overflowing frame. The allocated frame
     * will become the head of the linked list, and its next pointer points to the
     * overflowing frame.
     */
    @Override
    public int allocateFrameForOverflowing(int frameIndex) throws HyracksDataException {
        int newFrameIndex = allocateFrame();
        if (newFrameIndex < 0)
            return newFrameIndex;
        framesNext[newFrameIndex] = frameIndex;
        return newFrameIndex;
    }

    /**
     * Allocate a free frame.
     * <p/>
     * If {@link #nextFreeFrameIndex} points to an unallocated frame slot, the frame is initialized and returned. Then the next frame of the free frame replaces the first free page {@link #nextFreeFrameIndex}.
     * <p/>
     * When there is no more free frame available, {@link #nextFreeFrameIndex} will point to {@link #NO_MORE_FREE_BUFFER}.
     * <p/>
     * 
     * @return
     * @throws HyracksDataException
     */
    @Override
    public int allocateFrame() throws HyracksDataException {
        if (nextFreeFrameIndex != NO_MORE_FREE_BUFFER) {
            int freeFrameIndex = nextFreeFrameIndex;
            if (frames[freeFrameIndex] == null) {
                frames[freeFrameIndex] = ctx.allocateFrame();
            }
            int oldNext = framesNext[freeFrameIndex];
            framesNext[freeFrameIndex] = INVALID_BUFFER;
            if (oldNext == UNALLOCATED_FRAME) {
                nextFreeFrameIndex++;
                if (nextFreeFrameIndex == framesLimit) { // No more free buffer
                    nextFreeFrameIndex = NO_MORE_FREE_BUFFER;
                }
            } else {
                nextFreeFrameIndex = oldNext;
            }
            (frames[freeFrameIndex]).clear();

            freeFramesCounter--;
            if (freeFramesCounter < 0) {
                throw new HyracksDataException("Memory underflow!");
            }
            return freeFrameIndex;
        } else {
            return NO_MORE_FREE_BUFFER; // A partitions needs to be spilled (if feasible)
        }
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IFrameAllocator#getFrame(int)
     */
    @Override
    public byte[] getFrame(int frameIndex) throws HyracksDataException {
        if (frameIndex < 0 || frameIndex > frames.length) {
            return null;
        }
        return frames[frameIndex].array();
    }

    private int compare(int[] keys0, FrameTupleAccessor accessor0, int tIndex0, int[] keys1,
            TupleInFrameAccessor accessor1) {
        int tStart0 = accessor0.getTupleStartOffset(tIndex0);
        int fStartOffset0 = accessor0.getFieldSlotsLength() + tStart0;

        int tStart1 = accessor1.getTupleStartOffset();
        int fStartOffset1 = accessor1.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < keys0.length; ++i) {
            int fIdx0 = keys0[i];
            int fStart0 = accessor0.getFieldStartOffset(tIndex0, fIdx0);
            int fEnd0 = accessor0.getFieldEndOffset(tIndex0, fIdx0);
            int fLen0 = fEnd0 - fStart0;

            int fIdx1 = keys1[i];
            int fStart1 = accessor1.getFieldStartOffset(fIdx1);
            int fEnd1 = accessor1.getFieldEndOffset(fIdx1);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor0.getBuffer().array(), fStart0 + fStartOffset0, fLen0, accessor1
                    .getBuffer().array(), fStart1 + fStartOffset1, fLen1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private int getHashtablePartitionFrame(int pid, int frameIndex) {
        if (frameIndex > partitionSizeInFrame[pid] - 1) {
            return -1;
        }
        int fIdx = partitionCurrentFrame[pid];
        for (int i = partitionSizeInFrame[pid] - 1; i > frameIndex; i--) {
            fIdx = framesNext[fIdx];
        }
        return fIdx;
    }

    public RunFileWriter getSpilledRunWriter(int pid) {
        if (pid >= 0 && pid < numOfPartitions) {
            return spilledPartitionWriters[pid];
        }
        return null;
    }

    public int getPartitionSizeInFrame(int pid) {
        if (pid >= 0 && pid < numOfPartitions) {
            return partitionSizeInFrame[pid];
        }
        return -1;
    }

    public boolean hasSpilledPartition() {
        boolean hasSpilledPart = false;
        for (int i = 0; i < numOfPartitions; i++) {
            if (spilledPartitionWriters[i] != null) {
                hasSpilledPart = true;
                break;
            }
        }
        return hasSpilledPart;
    }

    public int getPartitionMaxFrameSize() {
        int maxSize = 0;
        for (int i = partitionStatus.nextSetBit(0); i >= 0; i = partitionStatus.nextSetBit(i + 1)) {
            if (partitionSizeInFrame[i] > maxSize) {
                maxSize = partitionSizeInFrame[i];
            }
        }
        return maxSize;
    }

    public int getNumOfPartitions() {
        return numOfPartitions;
    }

    abstract public int getReservedFramesCount();

    abstract public int selectPartitionToSpill();

}
