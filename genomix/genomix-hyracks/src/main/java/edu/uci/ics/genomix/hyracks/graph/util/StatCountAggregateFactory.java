/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.genomix.hyracks.graph.util;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class StatCountAggregateFactory implements IAggregatorDescriptorFactory {

    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;

    public class CountAggregator implements IAggregatorDescriptor {
        private final int[] keyFields;

        public CountAggregator(int[] keyFields) {
            this.keyFields = keyFields;
        }

        @Override
        public AggregateState createAggregateStates() {
            return null;
        }

        @Override
        public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex, AggregateState state)
                throws HyracksDataException {
            int count = 1;
            DataOutput fieldOutput = tupleBuilder.getDataOutput();
            try {
                fieldOutput.writeInt(count);
                tupleBuilder.addFieldEndOffset();
            } catch (IOException e) {
                throw new HyracksDataException("I/O exception when initializing the aggregator.");
            }
        }

        @Override
        public void reset() {

        }

        @Override
        public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                int stateTupleIndex, AggregateState state) throws HyracksDataException {
            int count = 1;

            int statetupleOffset = stateAccessor.getTupleStartOffset(stateTupleIndex);
            int countfieldStart = stateAccessor.getFieldStartOffset(stateTupleIndex, keyFields.length);
            int countoffset = statetupleOffset + stateAccessor.getFieldSlotsLength() + countfieldStart;

            byte[] data = stateAccessor.getBuffer().array();
            count += IntegerSerializerDeserializer.getInt(data, countoffset);
            IntegerSerializerDeserializer.putInt(count, data, countoffset);
        }

        @Override
        public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                AggregateState state) throws HyracksDataException {
            int count = getCount(accessor, tIndex);
            DataOutput fieldOutput = tupleBuilder.getDataOutput();
            try {
                fieldOutput.writeInt(count);
                tupleBuilder.addFieldEndOffset();
            } catch (IOException e) {
                throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
            }

        }

        protected int getCount(IFrameTupleAccessor accessor, int tIndex) {
            int tupleOffset = accessor.getTupleStartOffset(tIndex);
            int fieldStart = accessor.getFieldStartOffset(tIndex, keyFields.length);
            int countoffset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
            byte[] data = accessor.getBuffer().array();

            return IntegerSerializerDeserializer.getInt(data, countoffset);
        }

        @Override
        public void outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                AggregateState state) throws HyracksDataException {
            outputPartialResult(tupleBuilder, accessor, tIndex, state);
        }

        @Override
        public void close() {

        }

    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        return new CountAggregator(keyFields);
    }

}
