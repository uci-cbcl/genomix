/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
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
import edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable;
import edu.uci.ics.hyracks.dataflow.std.structures.SerializableHashTable;
import edu.uci.ics.hyracks.dataflow.std.structures.TuplePointer;

public class HashSpillableFrameSortTableFactory implements ISpillableTableFactory {

    private static final long serialVersionUID = 1L;
    private final ITuplePartitionComputerFactory tpcf;

    public HashSpillableFrameSortTableFactory(ITuplePartitionComputerFactory tpcf) {
        this.tpcf = tpcf;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.ISpillableTableFactory#buildSpillableTable(edu.uci.ics.hyracks.api.context.IHyracksTaskContext, int[], edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory[], edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory, edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, int)
     */
    @Override
    public ISpillableTable buildSpillableTable(final IHyracksTaskContext ctx, final int[] keyFields,
            IBinaryComparatorFactory[] comparatorFactories, INormalizedKeyComputerFactory normalizedKeyComputerFactory,
            IAggregatorDescriptorFactory aggregateFactory, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, final int tableSize, final int framesLimit)
            throws HyracksDataException {

        // Keys for stored records
        final int[] storedKeys = new int[keyFields.length];
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keyFields.length];
        for (int i = 0; i < keyFields.length; i++) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecordDescriptor.getFields()[keyFields[i]];
        }

        int[] keyFieldsInPartialResults = new int[keyFields.length];
        for (int i = 0; i < keyFieldsInPartialResults.length; i++) {
            keyFieldsInPartialResults[i] = i;
        }

        final IAggregatorDescriptor aggregator = aggregateFactory.createAggregator(ctx, inRecordDescriptor,
                outRecordDescriptor, keyFields, keyFieldsInPartialResults);

        final ITuplePartitionComputer tpc = tpcf.createPartitioner();

        final AggregateState aggregateState = aggregator.createAggregateStates();

        final FrameTupleAccessor storedKeysAccessor;

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

        final GroupingFrameSorter frameSorter = new GroupingFrameSorter(ctx, storedKeys, normalizedKeyComputerFactory,
                comparatorFactories, internalRecordDescriptor, outRecordDescriptor, aggregator, aggregateState);

        storedKeysAccessor = new FrameTupleAccessor(ctx.getFrameSize(), internalRecordDescriptor);

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final FrameTuplePairComparator ftpcPartial = new FrameTuplePairComparator(keyFields, storedKeys, comparators);

        final ArrayTupleBuilder stateTupleBuilder;
        if (keyFields.length < outRecordDescriptor.getFields().length) {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length);
        } else {
            stateTupleBuilder = new ArrayTupleBuilder(outRecordDescriptor.getFields().length + 1);
        }

        return new ISpillableTable() {

            private final ISerializableTable table = new SerializableHashTable(tableSize, ctx);

            private final TuplePointer storedTuplePointer = new TuplePointer();

            private FrameTupleAppender stateAppender = new FrameTupleAppender(ctx.getFrameSize());

            private ByteBuffer stateFrame;

            @Override
            public void sortFrames() {
                frameSorter.sortFrames();
            }

            @Override
            public void reset() {
                table.reset();
                frameSorter.reset();
                aggregator.reset();
            }

            @Override
            public boolean insert(FrameTupleAccessor accessor, int tIndex) throws HyracksDataException {
                if (stateFrame == null) {
                    // initialize the frame for aggregation result
                    stateFrame = ctx.allocateFrame();
                    stateAppender.reset(stateFrame, true);
                }
                int entry = tpc.partition(accessor, tIndex, tableSize);
                boolean foundGroup = false;
                int offset = 0;
                do {
                    table.getTuplePointer(entry, offset++, storedTuplePointer);
                    if (storedTuplePointer.frameIndex < 0)
                        break;
                    if (storedTuplePointer.frameIndex == frameSorter.getFrameCount()) {
                        storedKeysAccessor.reset(stateFrame);
                    } else {
                        storedKeysAccessor.reset(frameSorter.getBuffer(storedTuplePointer.frameIndex));
                    }
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
                    if (!stateAppender.append(stateTupleBuilder.getFieldEndOffsets(),
                            stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                        if (!nextAvailableFrame()) {
                            return false;
                        }
                        if (!stateAppender.append(stateTupleBuilder.getFieldEndOffsets(),
                                stateTupleBuilder.getByteArray(), 0, stateTupleBuilder.getSize())) {
                            throw new HyracksDataException("Cannot init external aggregate state in a frame.");
                        }
                    }

                    storedTuplePointer.frameIndex = frameSorter.getFrameCount();
                    storedTuplePointer.tupleIndex = stateAppender.getTupleCount() - 1;
                    table.insert(entry, storedTuplePointer);
                } else {

                    aggregator.aggregate(accessor, tIndex, storedKeysAccessor, storedTuplePointer.tupleIndex,
                            aggregateState);

                }
                return true;
            }

            @Override
            public int getFrameCount() {
                return frameSorter.getFrameCount();
            }

            @Override
            public void flushFrames(IFrameWriter writer, boolean isPartial) throws HyracksDataException {
                writer.open();
                frameSorter.flushFrames(writer, isPartial);
            }

            @Override
            public void close() {
                table.close();
                aggregateState.close();
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
                if (frameSorter.getFrameCount() >= framesLimit) {
                    return false;
                }
                // Otherwise, dump the frame to the frame sorter, and reset the frame
                frameSorter.insertFrame(stateFrame);
                stateAppender.reset(stateFrame, true);
                return true;
            }

            @Override
            public void finishup(boolean isSorted) {
                if (stateFrame.getInt(stateFrame.capacity() - 4) > 0) {
                    frameSorter.insertFrame(stateFrame);
                    if (isSorted) {
                        frameSorter.sortFrames();
                    }
                    stateAppender.reset(stateFrame, true);
                }
            }
        };
    }
}
