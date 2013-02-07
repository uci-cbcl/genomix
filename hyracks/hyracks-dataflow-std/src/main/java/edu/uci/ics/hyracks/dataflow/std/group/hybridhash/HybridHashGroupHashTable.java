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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HybridHashGroupHashTable implements IFrameWriter {

    private final static int HEADER_REF_EMPTY = -1;

    private static final int INT_SIZE = 4;

    private IHyracksTaskContext ctx;

    private final int frameSize;

    private final int framesLimit;

    private final int tableSize;

    private final int numOfPartitions;

    private final IFrameWriter outputWriter;

    private final IBinaryComparator[] comparators;

    /**
     * index for keys
     */
    private final int[] inputKeys, internalKeys;

    private final RecordDescriptor inputRecordDescriptor, outputRecordDescriptor;

    /**
     * hash partitioner for hashing
     */
    private final ITuplePartitionComputer hashComputer;

    /**
     * hash partitioner for partitioning
     */
    private final ITuplePartitionComputer partitionComputer;

    /**
     * Hashtable haders
     */
    private ByteBuffer[] headers;

    /**
     * buffers for hash table
     */
    private ByteBuffer[] contents;

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
    private List<Integer> spilledRunAggregatedPages = null;
    private List<Integer> spilledPartRunSizesInFrames = null;
    private List<Integer> spilledPartRunSizesInTuples = null;

    /**
     * index of the current working buffer in hash table
     */
    private int currentHashTableFrame;

    /**
     * Aggregation state
     */
    private AggregateState htAggregateState;

    /**
     * the aggregator
     */
    private final IAggregatorDescriptor aggregator;

    /**
     * records inserted into the in-memory hash table (for hashing and aggregation)
     */
    private int hashedRawRecords = 0;

    /**
     * in-memory part size in tuples
     */
    private int hashedKeys = 0;

    /**
     * Hash table tuple pointer
     */
    private TuplePointer matchPointer;

    /**
     * Frame tuple accessor for input data frames
     */
    private FrameTupleAccessor inputFrameTupleAccessor;

    /**
     * flag for whether the hash table if full
     */
    private boolean isHashtableFull;

    /**
     * flag for only partition (no aggregation and hashing)
     */
    private boolean isPartitionOnly;

    /**
     * Tuple accessor for hash table contents
     */
    private FrameTupleAccessorForGroupHashtable hashtableRecordAccessor;

    private ArrayTupleBuilder internalTupleBuilder;

    private FrameTupleAppender spilledPartInsertAppender;

    private FrameTupleAppenderForGroupHashtable htInsertAppender;

    public HybridHashGroupHashTable(IHyracksTaskContext ctx, int framesLimit, int tableSize, int numOfPartitions,
            int[] keys, int hashSeedOffset, IBinaryComparator[] comparators, ITuplePartitionComputerFamily tpcFamily,
            IAggregatorDescriptor aggregator, RecordDescriptor inputRecordDescriptor,
            RecordDescriptor outputRecordDescriptor, IFrameWriter outputWriter) throws HyracksDataException {
        this.ctx = ctx;
        this.frameSize = ctx.getFrameSize();
        this.tableSize = tableSize;
        this.framesLimit = framesLimit;
        this.numOfPartitions = numOfPartitions;
        this.inputKeys = keys;
        this.internalKeys = new int[keys.length];
        for (int i = 0; i < internalKeys.length; i++) {
            internalKeys[i] = i;
        }

        this.comparators = comparators;

        this.inputRecordDescriptor = inputRecordDescriptor;
        this.outputRecordDescriptor = outputRecordDescriptor;

        this.outputWriter = outputWriter;

        this.hashComputer = tpcFamily.createPartitioner(hashSeedOffset * 2);
        this.partitionComputer = tpcFamily.createPartitioner(hashSeedOffset * 2 + 1);

        this.aggregator = aggregator;

    }

    public static double getHashtableOverheadRatio(int tableSize, int frameSize, int framesLimit, int recordSizeInByte) {
        int pagesForRecord = framesLimit - getHeaderPages(tableSize, frameSize);
        int recordsInHashtable = (pagesForRecord - 1) * ((int) (frameSize / (recordSizeInByte + 2 * INT_SIZE)));

        return (double) framesLimit * frameSize / recordsInHashtable / recordSizeInByte;
    }

    public static int getHeaderPages(int tableSize, int frameSize) {
        return (int) Math.ceil((double)tableSize * INT_SIZE * 2 / frameSize);
    }

    @Override
    public void open() throws HyracksDataException {
        // initialize hash headers
        int htHeaderCount = getHeaderPages(tableSize, frameSize);

        isPartitionOnly = false;
        if (numOfPartitions >= framesLimit - htHeaderCount) {
            isPartitionOnly = true;
        }

        if (isPartitionOnly) {
            htHeaderCount = 0;
        }

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
    }

    private void insert(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {

        if (isPartitionOnly) {
            // for partition only
            int pid = partitionComputer.partition(accessor, tupleIndex, tableSize) % numOfPartitions;
            insertSpilledPartition(accessor, tupleIndex, pid);
            spilledPartRunSizeArrayInTuples[pid]++;
            return;
        }

        int hid = hashComputer.partition(accessor, tupleIndex, tableSize);

        if (findMatch(hid, accessor, tupleIndex)) {
            // found a matching: do aggregation
            hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
            aggregator.aggregate(accessor, tupleIndex, hashtableRecordAccessor, matchPointer.tupleIndex,
                    htAggregateState);
            hashedRawRecords++;
        } else {
            if (isHashtableFull) {
                // when hash table is full: spill the record
                int pid = partitionComputer.partition(accessor, tupleIndex, tableSize) % numOfPartitions;
                insertSpilledPartition(accessor, tupleIndex, pid);
                spilledPartRunSizeArrayInTuples[pid]++;
            } else {
                // insert a new entry into the hash table
                internalTupleBuilder.reset();
                for (int k = 0; k < inputKeys.length; k++) {
                    internalTupleBuilder.addField(accessor, tupleIndex, inputKeys[k]);
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
                        // no more frame to allocate: stop expending the hash table
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
                    headers[headerFrameIndex]
                            .putInt(headerFrameOffset + INT_SIZE, htInsertAppender.getTupleCount() - 1);
                } else {
                    // update the previous reference
                    hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                    int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                    contents[matchPointer.frameIndex].putInt(refOffset, currentHashTableFrame);
                    contents[matchPointer.frameIndex]
                            .putInt(refOffset + INT_SIZE, htInsertAppender.getTupleCount() - 1);
                }

                hashedKeys++;
                hashedRawRecords++;
            }
        }
    }

    /**
     * Insert record into a spilled partition, by directly copying the tuple into the output buffer.
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

    public void finishup() throws HyracksDataException {
        // spill all output buffers
        for (int i = 0; i < numOfPartitions; i++) {
            if (spilledPartOutputBuffers[i] != null) {
                flushSpilledPartitionOutputBuffer(i);
            }
        }
        spilledPartOutputBuffers = null;

        // flush in-memory aggregation results: no more frame cost here as all output buffers are recycled
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

                aggregator.outputFinalResult(outFlushTupleBuilder, hashtableRecordAccessor, i, htAggregateState);

                if (!outputBufferAppender.append(outFlushTupleBuilder.getFieldEndOffsets(),
                        outFlushTupleBuilder.getByteArray(), 0, outFlushTupleBuilder.getSize())) {
                    FrameUtils.flushFrame(outputBuffer, outputWriter);
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
        }

        // create run readers and statistic information for spilled runs
        spilledPartRunReaders = new LinkedList<IFrameReader>();
        spilledRunAggregatedPages = new LinkedList<Integer>();
        spilledPartRunSizesInFrames = new LinkedList<Integer>();
        spilledPartRunSizesInTuples = new LinkedList<Integer>();
        for (int i = 0; i < numOfPartitions; i++) {
            if (spilledPartRunWriters[i] != null) {
                spilledPartRunReaders.add(spilledPartRunWriters[i].createReader());
                spilledRunAggregatedPages.add(0);
                spilledPartRunWriters[i].close();
                spilledPartRunSizesInFrames.add(spilledPartRunSizeArrayInFrames[i]);
                spilledPartRunSizesInTuples.add(spilledPartRunSizeArrayInTuples[i]);
            }
        }
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
    private int compare(FrameTupleAccessor accessor, int tupleIndex, FrameTupleAccessorForGroupHashtable hashAccessor,
            int hashTupleIndex) {
        int tStart0 = accessor.getTupleStartOffset(tupleIndex);
        int fStartOffset0 = accessor.getFieldSlotsLength() + tStart0;

        int tStart1 = hashAccessor.getTupleStartOffset(hashTupleIndex);
        int fStartOffset1 = hashAccessor.getFieldSlotsLength() + tStart1;

        for (int i = 0; i < internalKeys.length; ++i) {
            int fStart0 = accessor.getFieldStartOffset(tupleIndex, inputKeys[i]);
            int fEnd0 = accessor.getFieldEndOffset(tupleIndex, inputKeys[i]);
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

    public List<Integer> getSpilledRunsSizeInRawTuples() throws HyracksDataException {
        return spilledPartRunSizesInTuples;
    }

    public int getHashedUniqueKeys() throws HyracksDataException {
        return hashedKeys;
    }

    public int getHashedRawRecords() throws HyracksDataException {
        return hashedRawRecords;
    }

    public List<Integer> getSpilledRunsAggregatedPages() throws HyracksDataException {
        return spilledRunAggregatedPages;
    }

    public List<IFrameReader> getSpilledRuns() throws HyracksDataException {
        return spilledPartRunReaders;
    }

    public List<Integer> getSpilledRunsSizeInPages() throws HyracksDataException {
        return spilledPartRunSizesInFrames;
    }

}
