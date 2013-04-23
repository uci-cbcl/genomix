/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.genomix.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.std.accessors.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

/**
 * sum
 */
public class DistributedMergeLmerAggregateFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;

    public DistributedMergeLmerAggregateFactory() {
    }

    public class DistributeAggregatorDescriptor implements IAggregatorDescriptor {
        private static final int MAX = 127;

        @Override
        public void reset() {
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub

        }

        @Override
        public AggregateState createAggregateStates() {
            return new AggregateState(new Object() {
            });
        }

        protected byte getField(IFrameTupleAccessor accessor, int tIndex, int fieldId) {
            int tupleOffset = accessor.getTupleStartOffset(tIndex);
            int fieldStart = accessor.getFieldStartOffset(tIndex, fieldId);
            int offset = tupleOffset + fieldStart + accessor.getFieldSlotsLength();
            byte data = ByteSerializerDeserializer.getByte(accessor.getBuffer().array(), offset);
            return data;
        }

        @Override
        public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                throws HyracksDataException {
            byte bitmap = getField(accessor, tIndex, 1);
            byte count = getField(accessor, tIndex, 2);

            DataOutput fieldOutput = tupleBuilder.getDataOutput();
            try {
                fieldOutput.writeByte(bitmap);
                tupleBuilder.addFieldEndOffset();
                fieldOutput.writeByte(count);
                tupleBuilder.addFieldEndOffset();
            } catch (IOException e) {
                throw new HyracksDataException("I/O exception when initializing the aggregator.");
            }
        }

        @Override
        public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                int stateTupleIndex, AggregateState state) throws HyracksDataException {
            byte bitmap = getField(accessor, tIndex, 1);
            short count = getField(accessor, tIndex, 2);

            int statetupleOffset = stateAccessor.getTupleStartOffset(stateTupleIndex);
            int bitfieldStart = stateAccessor.getFieldStartOffset(stateTupleIndex, 1);
            int countfieldStart = stateAccessor.getFieldStartOffset(stateTupleIndex, 2);
            int bitoffset = statetupleOffset + stateAccessor.getFieldSlotsLength() + bitfieldStart;
            int countoffset = statetupleOffset + stateAccessor.getFieldSlotsLength() + countfieldStart;

            byte[] data = stateAccessor.getBuffer().array();

            bitmap |= data[bitoffset];
            count += data[countoffset];
            if (count >= MAX) {
                count = (byte) MAX;
            }
            data[bitoffset] = bitmap;
            data[countoffset] = (byte) count;
        }

        @Override
        public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                AggregateState state) throws HyracksDataException {
            byte bitmap = getField(accessor, tIndex, 1);
            byte count = getField(accessor, tIndex, 2);
            DataOutput fieldOutput = tupleBuilder.getDataOutput();
            try {
                fieldOutput.writeByte(bitmap);
                tupleBuilder.addFieldEndOffset();
                fieldOutput.writeByte(count);
                tupleBuilder.addFieldEndOffset();
            } catch (IOException e) {
                throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
            }

        }

        @Override
        public void outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                AggregateState state) throws HyracksDataException {
            outputPartialResult(tupleBuilder, accessor, tIndex, state);
        }

    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        return new DistributeAggregatorDescriptor();
    }

}
