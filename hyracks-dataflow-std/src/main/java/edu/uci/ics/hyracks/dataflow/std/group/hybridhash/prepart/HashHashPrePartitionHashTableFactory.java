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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.prepart;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAccessorForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.FrameTupleAppenderForGroupHashtable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;

public class HashHashPrePartitionHashTableFactory implements IHashHashSerializableHashTableFactory {

    private static final long serialVersionUID = 1L;

    private final static int HEADER_REF_EMPTY = -1;

    private final static int INT_SIZE = 4;

    public final static HashHashPrePartitionHashTableFactory INSTANCE = new HashHashPrePartitionHashTableFactory();

    private HashHashPrePartitionHashTableFactory() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.hashhash.IHashHashSerializableHashTableFactory#createHashTable(edu.uci
     * .ics.hyracks.api.context.IHyracksTaskContext, int, int,
     * edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory[], int,
     * edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily)
     */
    @Override
    public IHashHashSpillableFrameWriter createHashTable(final IHyracksTaskContext ctx, final int framesLimit,
            final int tableSize, final int numOfPartitions, final int[] keys, final IBinaryComparator[] comparators,
            final int hashSeedOffset, ITuplePartitionComputerFamily tpcFamily, final IAggregatorDescriptor aggregator,
            final IAggregatorDescriptor merger, final RecordDescriptor inputRecordDescriptor,
            final RecordDescriptor outputRecordDescriptor, final IFrameWriter outputWriter) {

        final int frameSize = ctx.getFrameSize();

        final int[] internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        final ITuplePartitionComputer hashComputer = tpcFamily.createPartitioner(hashSeedOffset * 2);

        final ITuplePartitionComputer partitionComputer = tpcFamily.createPartitioner(hashSeedOffset * 2 + 1);

        return new IHashHashSpillableFrameWriter() {

            /****************
             * For hash table
             ****************/

            /**
             * Hash table headers
             */
            private ByteBuffer[] headers;

            /**
             * Available buffers for allocation
             */
            private ByteBuffer[] contents;

            private int currentHashTableFrame;

            private AggregateState htAggregateState;

            /****************
             * For spilled partitions
             ****************/

            /**
             * output buffers for spilled partitions
             */
            private ByteBuffer[] spilledPartOutputBuffers;

            /**
             * run writers for spilled partitions
             */
            private RunFileWriter[] spilledPartRunWriters;
            private int[] spilledPartRunSizeArrayInFrames;
            private int[] spilledPartRunSizeArrayInTuples;

            private List<IFrameReader> spilledPartRunReaders = null;
            private List<Integer> spilledPartRunSizesInFrames = null;
            private List<Integer> spilledPartRunSizesInTuples = null;

            /**
             * records inserted into the in-memory hash table (for hashing and aggregation)
             */
            private int directFlushedRawSizeInTuples = 0;

            /**
             * in-memory part size in frames
             */
            private int directFlushedKeysInFrames = 0;

            /**
             * in-memory part size in tuples
             */
            private int directFlushedKeysInTuples = 0;

            /**
             * Hash table tuple pointer
             */
            private TuplePointer matchPointer;

            private FrameTupleAccessor inputFrameTupleAccessor;

            private boolean isHashtableFull;

            /**
             * Tuple accessor for hash table contents
             */
            private FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

            private ArrayTupleBuilder internalTupleBuilder;

            private FrameTupleAppender spilledPartInsertAppender;

            private FrameTupleAppenderForGroupHashtable htInsertAppender;

            /**********************************
             * for logging and debugging
             **********************************/
            private final Logger LOGGER = Logger.getLogger("HashHashDynamicPartitionHashTable");
            private long compCount = 0, hashSuccCompCount = 0, hashUnSuccCompCount = 0, hashSlotInitCount = 0;

            @Override
            public void open() throws HyracksDataException {

                // initialize hash headers
                int htHeaderCount = getHeaderPages(tableSize, frameSize);
                headers = new ByteBuffer[htHeaderCount];

                // initialize hash table contents
                contents = new ByteBuffer[framesLimit - htHeaderCount - numOfPartitions];
                currentHashTableFrame = 0;
                isHashtableFull = false;

                // initialize hash table aggregate state
                htAggregateState = aggregator.createAggregateStates();

                // initialize partition information
                spilledPartOutputBuffers = new ByteBuffer[numOfPartitions];
                spilledPartRunWriters = new RunFileWriter[numOfPartitions];
                spilledPartRunSizeArrayInFrames = new int[numOfPartitions];
                spilledPartRunSizeArrayInTuples = new int[numOfPartitions];

                // initialize other helper classes
                inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inputRecordDescriptor);
                internalTupleBuilder = new ArrayTupleBuilder(outputRecordDescriptor.getFieldCount());
                spilledPartInsertAppender = new FrameTupleAppender(frameSize);

                htInsertAppender = new FrameTupleAppenderForGroupHashtable(frameSize);
                matchPointer = new TuplePointer();
                hashtableRecordAccessor = new FrameTupleAccessorForGroupHashtable(frameSize, outputRecordDescriptor);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                inputFrameTupleAccessor.reset(buffer);
                int tupleCount = inputFrameTupleAccessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    insert(inputFrameTupleAccessor, i);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {

                for (int i = 0; i < numOfPartitions; i++) {
                    if (spilledPartRunWriters[i] != null) {
                        spilledPartRunWriters[i].close();
                    }
                }
                htAggregateState.close();

                ctx.getCounterContext().getCounter("must.hash.succ.comps", true).update(hashSuccCompCount);

                ctx.getCounterContext().getCounter("must.hash.unsucc.comps", true).update(hashUnSuccCompCount);

                ctx.getCounterContext().getCounter("must.hash.slot.init", true).update(hashSlotInitCount);
            }

            @Override
            public List<Integer> getSpilledRunsSizeInPages() throws HyracksDataException {
                return spilledPartRunSizesInFrames;
            }

            @Override
            public List<IFrameReader> getSpilledRuns() throws HyracksDataException {
                return spilledPartRunReaders;
            }

            @Override
            public void finishup() throws HyracksDataException {
                // spill all output buffers
                for (int i = 0; i < numOfPartitions; i++) {
                    if (spilledPartOutputBuffers[i] != null) {
                        flushSpilledPartitionOutputBuffer(i);
                    }
                }
                spilledPartOutputBuffers = null;

                // flush in-memory aggregation results
                ByteBuffer outputBuffer = ctx.allocateFrame();
                FrameTupleAppender outputBufferAppender = new FrameTupleAppender(frameSize);
                outputBufferAppender.reset(outputBuffer, true);

                ArrayTupleBuilder outFlushTupleBuilder = new ArrayTupleBuilder(outputRecordDescriptor.getFieldCount());

                for (ByteBuffer htFrame : contents) {
                    if (htFrame == null) {
                        continue;
                    }
                    hashtableRecordAccessor.reset(htFrame);
                    int tupleCount = hashtableRecordAccessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        outFlushTupleBuilder.reset();

                        for (int k = 0; k < internalKeys.length; k++) {
                            outFlushTupleBuilder.addField(hashtableRecordAccessor, i, internalKeys[k]);
                        }

                        aggregator
                                .outputFinalResult(outFlushTupleBuilder, hashtableRecordAccessor, i, htAggregateState);

                        if (!outputBufferAppender.append(outFlushTupleBuilder.getFieldEndOffsets(),
                                outFlushTupleBuilder.getByteArray(), 0, outFlushTupleBuilder.getSize())) {
                            FrameUtils.flushFrame(outputBuffer, outputWriter);
                            directFlushedKeysInFrames++;
                            outputBufferAppender.reset(outputBuffer, true);

                            if (!outputBufferAppender.append(outFlushTupleBuilder.getFieldEndOffsets(),
                                    outFlushTupleBuilder.getByteArray(), 0, outFlushTupleBuilder.getSize())) {
                                throw new HyracksDataException(
                                        "Failed to flush a record from in-memory hash table: record has length of "
                                                + outFlushTupleBuilder.getSize() + " and fields "
                                                + outFlushTupleBuilder.getFieldEndOffsets().length);
                            }
                        }
                    }
                }

                if (outputBufferAppender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
                    directFlushedKeysInFrames++;
                }

                // create run readers and statistic information for spilled runs
                spilledPartRunReaders = new LinkedList<IFrameReader>();
                spilledPartRunSizesInFrames = new LinkedList<Integer>();
                spilledPartRunSizesInTuples = new LinkedList<Integer>();
                for (int i = 0; i < numOfPartitions; i++) {
                    if (spilledPartRunWriters[i] != null) {
                        spilledPartRunReaders.add(spilledPartRunWriters[i].createReader());
                        spilledPartRunWriters[i].close();
                        spilledPartRunSizesInFrames.add(spilledPartRunSizeArrayInFrames[i]);
                        spilledPartRunSizesInTuples.add(spilledPartRunSizeArrayInTuples[i]);
                    }
                }
            }

            /**
             * Get the header frame index of the given hash table entry
             * 
             * @param entry
             * @return
             */
            private int getHeaderFrameIndex(int entry) {
                int frameIndex = (entry / frameSize * 2 * INT_SIZE) + (entry % frameSize * 2 * INT_SIZE / frameSize);
                return frameIndex;
            }

            /**
             * Get the tuple index of the given hash table entry
             * 
             * @param entry
             * @return
             */
            private int getHeaderTupleIndex(int entry) {
                int offset = (entry % frameSize) * 2 * INT_SIZE % frameSize;
                return offset;
            }

            private void insert(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {
                int hid = hashComputer.partition(accessor, tupleIndex, tableSize);

                if (findMatch(hid, accessor, tupleIndex)) {
                    // found a matching: do aggregation
                    hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                    aggregator.aggregate(accessor, tupleIndex, hashtableRecordAccessor, matchPointer.tupleIndex,
                            htAggregateState);
                    directFlushedRawSizeInTuples++;
                } else {
                    if (isHashtableFull) {
                        // when hash table is full: spill the record
                        int pid = partitionComputer.partition(accessor, tupleIndex, numOfPartitions);
                        insertSpilledPartition(accessor, tupleIndex, pid);
                        spilledPartRunSizeArrayInTuples[pid]++;
                    } else {
                        // insert a new entry into the hash table
                        internalTupleBuilder.reset();
                        for (int k = 0; k < keys.length; k++) {
                            internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
                        }

                        aggregator.init(internalTupleBuilder, accessor, tupleIndex, htAggregateState);

                        if (contents[currentHashTableFrame] == null) {
                            contents[currentHashTableFrame] = ctx.allocateFrame();
                        }

                        htInsertAppender.reset(contents[currentHashTableFrame], false);
                        if (!htInsertAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                            // hash table is full: try to allocate more frame
                            currentHashTableFrame++;
                            if (currentHashTableFrame >= contents.length) {
                                // no more frame to allocate: flush the hash table
                                isHashtableFull = true;

                                // reinsert the record
                                insert(accessor, tupleIndex);

                                return;
                            } else {
                                if (contents[currentHashTableFrame] == null) {
                                    contents[currentHashTableFrame] = ctx.allocateFrame();
                                }

                                htInsertAppender.reset(contents[currentHashTableFrame], true);

                                if (!htInsertAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                                    throw new HyracksDataException(
                                            "Failed to insert an aggregation partial result into the in-memory hash table: it has the length of "
                                                    + internalTupleBuilder.getSize() + " and fields "
                                                    + internalTupleBuilder.getFieldEndOffsets().length);
                                }

                            }
                        }

                        // update hash table reference
                        if (matchPointer.frameIndex < 0) {
                            // need to initialize the hash table header
                            int headerFrameIndex = getHeaderFrameIndex(hid);
                            int headerFrameOffset = getHeaderTupleIndex(hid);

                            if (headers[headerFrameIndex] == null) {
                                headers[headerFrameIndex] = ctx.allocateFrame();
                                resetHeader(headerFrameIndex);
                            }

                            headers[headerFrameIndex].putInt(headerFrameOffset, currentHashTableFrame);
                            headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE,
                                    htInsertAppender.getTupleCount() - 1);

                            hashSlotInitCount++;
                        } else {
                            // update the previous reference
                            hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                            int refOffset = hashtableRecordAccessor
                                    .getTupleHashReferenceOffset(matchPointer.tupleIndex);
                            contents[matchPointer.frameIndex].putInt(refOffset, currentHashTableFrame);
                            contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE,
                                    htInsertAppender.getTupleCount() - 1);
                        }

                        directFlushedKeysInTuples++;
                    }
                }
            }

            /**
             * Insert record into a spilled partition.
             * 
             * @param accessor
             * @param tupleIndex
             * @param pid
             */
            private void insertSpilledPartition(FrameTupleAccessor accessor, int tupleIndex, int pid)
                    throws HyracksDataException {

                if (spilledPartOutputBuffers[pid] == null) {
                    spilledPartOutputBuffers[pid] = ctx.allocateFrame();
                }

                spilledPartInsertAppender.reset(spilledPartOutputBuffers[pid], false);

                if (!spilledPartInsertAppender.append(accessor, tupleIndex)) {
                    // the output buffer is full: flush
                    flushSpilledPartitionOutputBuffer(pid);
                    // reset the output buffer
                    spilledPartInsertAppender.reset(spilledPartOutputBuffers[pid], true);

                    if (!spilledPartInsertAppender.append(accessor, tupleIndex)) {
                        throw new HyracksDataException("Failed to insert a record into a spilled partition!");
                    }
                }

            }

            /**
             * Flush a spilled partition's output buffer.
             * 
             * @param pid
             * @throws HyracksDataException
             */
            private void flushSpilledPartitionOutputBuffer(int pid) throws HyracksDataException {
                if (spilledPartRunWriters[pid] == null) {
                    spilledPartRunWriters[pid] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile("HashHashPrePartitionHashTable"), ctx.getIOManager());
                    spilledPartRunWriters[pid].open();
                }

                FrameUtils.flushFrame(spilledPartOutputBuffers[pid], spilledPartRunWriters[pid]);

                spilledPartRunSizeArrayInFrames[pid]++;
            }

            /**
             * Hash table lookup
             * 
             * @param hid
             * @param accessor
             * @param tupleIndex
             * @return
             */
            private boolean findMatch(int hid, FrameTupleAccessor accessor, int tupleIndex) {

                int comps = 0;

                matchPointer.frameIndex = -1;
                matchPointer.tupleIndex = -1;

                // get reference in the header
                int headerFrameIndex = getHeaderFrameIndex(hid);
                int headerFrameOffset = getHeaderTupleIndex(hid);

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
                    comps++;
                    if (compare(accessor, tupleIndex, hashtableRecordAccessor, entryTupleIndex) == 0) {

                        hashSuccCompCount += comps;

                        return true;
                    }
                    // Move to the next record in this entry following the linked list
                    int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
                    int prevFrameIndex = entryFrameIndex;
                    entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
                    entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);
                }

                hashUnSuccCompCount += comps;

                return false;
            }

            /**
             * Compare an input record with a hash table entry.
             * 
             * @param accessor
             * @param tupleIndex
             * @param hashAccessor
             * @param hashTupleIndex
             * @return
             */
            private int compare(FrameTupleAccessor accessor, int tupleIndex,
                    FrameTupleAccessorForGroupHashtable hashAccessor, int hashTupleIndex) {
                compCount++;
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

                    int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0,
                            hashAccessor.getBuffer().array(), fStart1 + fStartOffset1, fLen1);
                    if (c != 0) {
                        return c;
                    }
                }
                return 0;
            }

            /**
             * reset the header page.
             * 
             * @param headerFrameIndex
             */
            private void resetHeader(int headerFrameIndex) {
                for (int i = 0; i < frameSize; i += INT_SIZE) {
                    headers[headerFrameIndex].putInt(i, HEADER_REF_EMPTY);
                }
            }

            @Override
            public int getDirectFlushSizeInPages() throws HyracksDataException {
                return directFlushedKeysInFrames;
            }

            @Override
            public List<Integer> getSpilledRunsSizeInTuples() throws HyracksDataException {
                return spilledPartRunSizesInTuples;
            }

            @Override
            public int getDirectFlushSizeInTuples() throws HyracksDataException {
                return directFlushedKeysInTuples;
            }

            @Override
            public int getDirectFlushRawSizeInTuples() throws HyracksDataException {
                return directFlushedRawSizeInTuples;
            }
        };
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.group.hashhash.IHashHashSerializableHashTableFactory#getHashTableFudgeFactor
     * (int)
     */
    @Override
    public double getHashTableFudgeFactor(int hashtableSlots, int estimatedRecordSize, int frameSize,
            int memorySizeInFrames) {
        //return memorySizeInFrames / ((double) memorySizeInFrames - getHeaderPages(hashtableSlots, frameSize))
        //        * ((double) estimatedRecordSize + 2 * INT_SIZE) / estimatedRecordSize;
        int pagesForRecord = memorySizeInFrames - getHeaderPages(hashtableSlots, frameSize);
        int recordsInHashtable = (pagesForRecord - 1) * ((int) (frameSize / (estimatedRecordSize + 2 * INT_SIZE)));

        return (double) memorySizeInFrames * frameSize / recordsInHashtable / estimatedRecordSize;
    }

    @Override
    public int getHeaderPages(int hashtableSlots, int frameSize) {
        return (int) Math.ceil((double) hashtableSlots * 2 * INT_SIZE / frameSize);
    }
}
