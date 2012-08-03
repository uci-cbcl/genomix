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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTuplePairComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HashSpillableBucketSortTableFactory implements ISpillableTableFactory {

    private static final long serialVersionUID = 1L;

    private static final int INIT_REF_COUNT = 4;

    private final ITuplePartitionComputerFactory tpcf;

    // FIXME
    private final static Logger LOGGER = Logger.getLogger(HashSpillableBucketSortTableFactory.class.getSimpleName());

    public HashSpillableBucketSortTableFactory(ITuplePartitionComputerFactory tpcf) {
        this.tpcf = tpcf;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.ISpillableTableFactory#buildSpillableTable(edu.uci.ics.hyracks.api.context.IHyracksTaskContext, int[], edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory[], edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory, edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int, int)
     */
    @Override
    public ISpillableTable buildSpillableTable(final IHyracksTaskContext ctx, final int[] keyFields,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory normalizedKeyComputerFactory,
            IAggregatorDescriptorFactory aggregateFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int tableSize, final int framesLimit)
            throws HyracksDataException {
        final int[] storedKeys = new int[keyFields.length];
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[keyFields[i]];
        }

        RecordDescriptor internalRecordDescriptor = outRecordDescriptor;

        if (keyFields.length >= outRecordDescriptor.getFields().length) {
            // for the case of zero-aggregations
            ISerializerDeserializer<?>[] fields = outRecordDescriptor.getFields();
            ITypeTraits[] types = outRecordDescriptor.getTypeTraits();
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

        final FrameTupleAccessor storedKeysAccessor = new FrameTupleAccessor(ctx.getFrameSize(),
                internalRecordDescriptor);
        final FrameTupleAccessor storedKeysAccessorForSort = new FrameTupleAccessor(ctx.getFrameSize(),
                internalRecordDescriptor);

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final FrameTuplePairComparator ftpcPartial = new FrameTuplePairComparator(keyFields, storedKeys, comparators);

        final FrameTuplePairComparator ftpcTuple = new FrameTuplePairComparator(storedKeys, storedKeys, comparators);

        final ITuplePartitionComputer tpc = tpcf.createPartitioner();

        final INormalizedKeyComputer nkc = normalizedKeyComputerFactory == null ? null : normalizedKeyComputerFactory
                .createNormalizedKeyComputer();

        int[] keyFieldsInPartialResults = new int[keyFields.length];
        for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
            keyFieldsInPartialResults[i] = i;
        }

        final IAggregatorDescriptor aggregator = aggregateFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, keyFields, keyFieldsInPartialResults);

        final AggregateState aggregateState = aggregator.createAggregateStates();

        final ArrayTupleBuilder stateTupleBuilder;
        if (keyFields.length < outRecordDescriptor.getFields().length) {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        } else {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length + 1);
        }

        final ArrayTupleBuilder outputTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);

        return new ISpillableTable() {

            private int lastBufIndex;

            private ByteBuffer outputFrame;
            private FrameTupleAppender outputAppender;

            private FrameTupleAppender stateAppender = new FrameTupleAppender(ctx.getFrameSize());

            private final ISerializableTable table = new SerializableHashTable(tableSize, ctx);
            private final TuplePointer storedTuplePointer = new TuplePointer();
            private final List<ByteBuffer> frames = new ArrayList<ByteBuffer>();

            /**
             * A tuple is "pointed" to by 2 entries in the tPointers array: <br/>
             * - [0] = Tuple offset in the hash bucket;<br/>
             * - [1] = Poor man's normalized key for the tuple.
             */
            private int[] tPointers;

            @Override
            public void close() {
                lastBufIndex = -1;
                tPointers = null;
                table.close();
                frames.clear();
                aggregateState.close();
            }

            @Override
            public void reset() {
                lastBufIndex = -1;
                tPointers = null;
                table.reset();
                aggregator.reset();
            }

            @Override
            public int getFrameCount() {
                return lastBufIndex;
            }

            @Override
            public void sortFrames() {
                // do nothing
            }

            /**
             * Sort the given hash bucket.
             * 
             * @param bucketID
             * @return the number of tuples in that bucket.
             */
            public int sortBucket(int bucketID) {
                // sort field index
                int sfIdx = storedKeys[0];
                if (tPointers == null)
                    tPointers = new int[INIT_REF_COUNT * 2];
                int ptr = 0;

                int offset = 0;
                do {
                    table.getTuplePointer(bucketID, offset, storedTuplePointer);
                    if (storedTuplePointer.frameIndex < 0)
                        break;

                    tPointers[ptr * 2] = offset;
                    int fIndex = storedTuplePointer.frameIndex;
                    int tIndex = storedTuplePointer.tupleIndex;
                    storedKeysAccessor.reset(frames.get(fIndex));
                    int tStart = storedKeysAccessor.getTupleStartOffset(tIndex);
                    int f0StartRel = storedKeysAccessor.getFieldStartOffset(tIndex, sfIdx);
                    int f0EndRel = storedKeysAccessor.getFieldEndOffset(tIndex, sfIdx);
                    int f0Start = f0StartRel + tStart + storedKeysAccessor.getFieldSlotsLength();
                    tPointers[ptr * 2 + 1] = nkc == null ? 0 : nkc.normalize(storedKeysAccessor.getBuffer().array(),
                            f0Start, f0EndRel - f0StartRel);
                    ptr++;

                    // Expand the tPointers
                    if (ptr * 2 >= tPointers.length) {
                        int[] newTPointers = new int[tPointers.length * 2];
                        System.arraycopy(tPointers, 0, newTPointers, 0, tPointers.length);
                        tPointers = newTPointers;
                    }

                    offset++;
                } while (true);
                /**
                 * Sort using quick sort
                 */
                if (offset > 0) {
                    sort(bucketID, tPointers, 0, offset);
                }
                return offset;
            }

            @Override
            public boolean insert(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                if (lastBufIndex < 0)
                    nextAvailableFrame();
                int entry = tpc.partition(accessor, tIndex, tableSize);
                boolean foundGroup = false;
                int offset = 0;
                do {
                    table.getTuplePointer(entry, offset++, storedTuplePointer);
                    if (storedTuplePointer.frameIndex < 0)
                        break;
                    storedKeysAccessor.reset(frames.get(storedTuplePointer.frameIndex));
                    int c = ftpcPartial.compare(accessor, tIndex, storedKeysAccessor, storedTuplePointer.tupleIndex);
                    if (c == 0) {
                        foundGroup = true;
                        break;
                    }
                } while (true);

                if (!foundGroup) {

                    stateTupleBuilder.reset();

                    for (int k = 0; k < keyFields.length; k++) {
                        stateTupleBuilder.addField(accessor, tIndex, keyFields[k]);
                    }

                    aggregator.init(stateTupleBuilder, accessor, tIndex, aggregateState);
                    if (!stateAppender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                            stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                        if (!nextAvailableFrame()) {
                            return false;
                        }
                        if (!stateAppender.appendSkipEmptyField(stateTupleBuilder.getFieldEndOffsets(),
                                stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                            throw new HyracksDataException("Cannot init external aggregate state in a frame.");
                        }
                    }

                    storedTuplePointer.frameIndex = lastBufIndex;
                    storedTuplePointer.tupleIndex = stateAppender.getTupleCount() - 1;
                    table.insert(entry, storedTuplePointer);
                } else {

                    aggregator.aggregate(accessor, tIndex, storedKeysAccessor, storedTuplePointer.tupleIndex,
                            aggregateState);

                }
                return true;
            }

            @Override
            public void flushFrames(IFrameWriter writer, boolean isPartial) throws HyracksDataException {

                // FIXME
                int recordsFlushed = 0;

                if (outputFrame == null) {
                    outputFrame = ctx.allocateFrame();
                }

                if (outputAppender == null) {
                    outputAppender = new FrameTupleAppender(outputFrame.capacity());
                }

                outputAppender.reset(outputFrame, true);

                writer.open();

                for (int i = 0; i < tableSize; i++) {
                    int tupleInBukcet = sortBucket(i);

                    // FIXME
                    recordsFlushed += tupleInBukcet;

                    for (int ptr = 0; ptr < tupleInBukcet; ptr++) {
                        int rowIndex = tPointers[ptr * 2];
                        table.getTuplePointer(i, rowIndex, storedTuplePointer);
                        int frameIndex = storedTuplePointer.frameIndex;
                        int tupleIndex = storedTuplePointer.tupleIndex;
                        // Get the frame containing the value
                        ByteBuffer buffer = frames.get(frameIndex);
                        storedKeysAccessor.reset(buffer);

                        outputTupleBuilder.reset();
                        for (int k = 0; k < storedKeys.length; k++) {
                            outputTupleBuilder.addField(storedKeysAccessor, tupleIndex, storedKeys[k]);
                        }

                        if (isPartial) {

                            aggregator.outputPartialResult(outputTupleBuilder, storedKeysAccessor, tupleIndex,
                                    aggregateState);

                        } else {

                            aggregator.outputFinalResult(outputTupleBuilder, storedKeysAccessor, tupleIndex,
                                    aggregateState);
                        }

                        if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                            FrameUtils.flushFrame(outputFrame, writer);

                            outputAppender.reset(outputFrame, true);
                            if (!outputAppender.appendSkipEmptyField(outputTupleBuilder.getFieldEndOffsets(),
                                    outputTupleBuilder.getByteArray(), 0, outputTupleBuilder.getSize())) {
                                throw new HyracksDataException("The output item is too large to be fit into a frame.");
                            }
                        }
                    }
                }
                if (outputAppender.getTupleCount() > 0) {
                    FrameUtils.flushFrame(outputFrame, writer);
                    outputAppender.reset(outputFrame, true);
                }
                // FIXME
                LOGGER.warning("HashSpillableBucketTableFactory-Flush\t" + recordsFlushed);
            }

            @Override
            public void finishup(boolean isSorted) {
                // do nothing
            }

            /**
             * Set the working frame to the next available frame in the frame
             * list. There are two cases:<br>
             * 1) If the next frame is not initialized, allocate a new frame. 2)
             * When frames are already created, they are recycled.
             * 
             * @return Whether a new frame is added successfully.
             */
            private boolean nextAvailableFrame() {
                // Return false if the number of frames is equal to the limit.
                if (lastBufIndex + 1 >= framesLimit - (table.getFrameCount()))
                    return false;

                if (frames.size() + table.getFrameCount() < framesLimit) {
                    // Insert a new frame
                    ByteBuffer frame = ctx.allocateFrame();
                    frame.position(0);
                    frame.limit(frame.capacity());
                    frames.add(frame);
                    stateAppender.reset(frame, true);
                    lastBufIndex = frames.size() - 1;
                } else {
                    // Reuse an old frame
                    lastBufIndex++;
                    ByteBuffer frame = frames.get(lastBufIndex);
                    frame.position(0);
                    frame.limit(frame.capacity());
                    stateAppender.reset(frame, true);
                }
                return true;
            }

            /**
             * Quick sort using pointers.
             * 
             * @param tPointers
             * @param offset
             * @param length
             */
            private void sort(int mTable, int[] tPointers, int offset, int length) {
                int m = offset + (length >> 1);
                int mRow = tPointers[m * 2];
                int mNormKey = tPointers[m * 2 + 1];

                table.getTuplePointer(mTable, mRow, storedTuplePointer);
                int mFrame = storedTuplePointer.frameIndex;
                int mTuple = storedTuplePointer.tupleIndex;
                storedKeysAccessor.reset(frames.get(mFrame));

                int a = offset;
                int b = a;
                int c = offset + length - 1;
                int d = c;
                while (true) {
                    while (b <= c) {
                        int bRow = tPointers[b * 2];
                        int bNormKey = tPointers[b * 2 + 1];
                        int cmp = 0;
                        if (bNormKey != mNormKey) {
                            cmp = ((((long) bNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                        } else {
                            table.getTuplePointer(mTable, bRow, storedTuplePointer);
                            int bFrame = storedTuplePointer.frameIndex;
                            int bTuple = storedTuplePointer.tupleIndex;
                            storedKeysAccessorForSort.reset(frames.get(bFrame));
                            cmp = ftpcTuple.compare(storedKeysAccessorForSort, bTuple, storedKeysAccessor, mTuple);
                        }
                        if (cmp > 0) {
                            break;
                        }
                        if (cmp == 0) {
                            swap(tPointers, a++, b);
                        }
                        ++b;
                    }
                    while (c >= b) {
                        int cRow = tPointers[c * 2];
                        int cNormKey = tPointers[c * 2 + 1];
                        int cmp = 0;
                        if (cNormKey != mNormKey) {
                            cmp = ((((long) cNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1 : 1;
                        } else {
                            table.getTuplePointer(mTable, cRow, storedTuplePointer);
                            int cFrame = storedTuplePointer.frameIndex;
                            int cTuple = storedTuplePointer.tupleIndex;
                            storedKeysAccessorForSort.reset(frames.get(cFrame));
                            cmp = ftpcTuple.compare(storedKeysAccessorForSort, cTuple, storedKeysAccessor, mTuple);
                        }
                        if (cmp < 0) {
                            break;
                        }
                        if (cmp == 0) {
                            swap(tPointers, c, d--);
                        }
                        --c;
                    }
                    if (b > c)
                        break;
                    swap(tPointers, b++, c--);
                }

                int s;
                int n = offset + length;
                s = Math.min(a - offset, b - a);
                vecswap(tPointers, offset, b - s, s);
                s = Math.min(d - c, n - d - 1);
                vecswap(tPointers, b, n - s, s);

                if ((s = b - a) > 1) {
                    sort(mTable, tPointers, offset, s);
                }
                if ((s = d - c) > 1) {
                    sort(mTable, tPointers, n - s, s);
                }
            }

            private void swap(int x[], int a, int b) {
                for (int i = 0; i < 2; ++i) {
                    int t = x[a * 2 + i];
                    x[a * 2 + i] = x[b * 2 + i];
                    x[b * 2 + i] = t;
                }
            }

            private void vecswap(int x[], int a, int b, int n) {
                for (int i = 0; i < n; i++, a++, b++) {
                    swap(x, a, b);
                }
            }

        };
    }

}
