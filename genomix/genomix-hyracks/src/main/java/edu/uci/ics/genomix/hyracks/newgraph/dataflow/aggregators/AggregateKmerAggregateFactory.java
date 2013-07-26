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

package edu.uci.ics.genomix.hyracks.newgraph.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.hyracks.data.primitive.PositionReference;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class AggregateKmerAggregateFactory implements IAggregatorDescriptorFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final int readLength;
    private final int kmerSize;
    
    public AggregateKmerAggregateFactory(int readlength, int k) {
        this.readLength = readlength;
        this.kmerSize = k;
    }
    
    
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        return new IAggregatorDescriptor() {
//            private PositionReference position = new PositionReference();
            
            private NodeWritable readNode = new NodeWritable(kmerSize);
            
            protected int getOffSet(IFrameTupleAccessor accessor, int tIndex, int fieldId) {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, fieldId);
                int offset = tupleOffset + fieldStart + accessor.getFieldSlotsLength();
                return offset;
            }

            @Override
            public void reset() {
            }

            @Override
            public void close() {

            }

            @Override
            public AggregateState createAggregateStates() {
                return new AggregateState(new NodeWritable());
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                NodeWritable localUniNode = (NodeWritable) state.state;
                localUniNode.reset(kmerSize);
                readNode.setNewReference(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));//????
                localUniNode.getNodeIdList().appendList(readNode.getNodeIdList());
                localUniNode.getFFList().appendList(readNode.getFFList());
                localUniNode.getFRList().appendList(readNode.getFRList());
                localUniNode.getRFList().appendList(readNode.getRFList());
                localUniNode.getRRList().appendList(readNode.getRRList());
//                inputVal.append(position);

                // make an empty field
                tupleBuilder.addFieldEndOffset();///???
            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                NodeWritable localUniNode = (NodeWritable) state.state;
                readNode.setNewReference(accessor.getBuffer().array(), getOffSet(accessor, tIndex, 1));//????
                localUniNode.getNodeIdList().appendList(readNode.getNodeIdList());
                localUniNode.getFFList().appendList(readNode.getFFList());
                localUniNode.getFRList().appendList(readNode.getFRList());
                localUniNode.getRFList().appendList(readNode.getRFList());
                localUniNode.getRRList().appendList(readNode.getRRList());
            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                throw new IllegalStateException("partial result method should not be called");
            }

            @Override
            public void outputFinalResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                DataOutput fieldOutput = tupleBuilder.getDataOutput();
                NodeWritable localUniNode = (NodeWritable) state.state;
                try {
                    fieldOutput.write(localUniNode.getByteArray(), localUniNode.getStartOffset(), localUniNode.getLength());

                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

        };
    }

}
