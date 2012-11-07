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
package edu.uci.ics.hyracks.examples.text;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;

public class ListifyRecordFieldAggregatorFactory implements IFieldAggregateDescriptorFactory {

    private static final long serialVersionUID = 1L;

    private final boolean hasBinaryState;

    public ListifyRecordFieldAggregatorFactory(boolean hasBinaryState) {
        this.hasBinaryState = hasBinaryState;
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory#createAggregator(edu.uci.ics.hyracks.api.context.IHyracksTaskContext, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor)
     */
    @Override
    public IFieldAggregateDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor) throws HyracksDataException {
        return new IFieldAggregateDescriptor() {

            @Override
            public void reset() {
            }

            @Override
            public void outputPartialResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                try {
                    if (hasBinaryState) {
                        int stateIdx = IntegerSerializerDeserializer.getInt(data, offset);
                        Object[] storedState = (Object[]) state.state;
                        fieldOutput.write((byte[]) storedState[stateIdx]);
                    } else {
                        fieldOutput.write((byte[]) state.state);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "I/O exception when writing a string to the output writer in ListifyRecordFieldAggregatorFactory.");
                }
            }

            @Override
            public void outputFinalResult(DataOutput fieldOutput, byte[] data, int offset, AggregateState state)
                    throws HyracksDataException {
                try {
                    if (hasBinaryState) {
                        int stateIdx = IntegerSerializerDeserializer.getInt(data, offset);
                        Object[] storedState = (Object[]) state.state;
                        fieldOutput.write((byte[]) storedState[stateIdx]);
                    } else {
                        fieldOutput.write((byte[]) state.state);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(
                            "I/O exception when writing a string to the output writer in MinMaxStringAggregatorFactory.");
                }
            }

            @Override
            public boolean needsObjectState() {
                return true;
            }

            @Override
            public boolean needsBinaryState() {
                return hasBinaryState;
            }

            @Override
            public void init(IFrameTupleAccessor accessor, int tIndex, DataOutput fieldOutput, AggregateState state)
                    throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int tupleLength = accessor.getTupleEndOffset(tIndex) - tupleOffset + 1;

                byte[] data = new byte[tupleLength];

                System.arraycopy(accessor.getBuffer().array(), tupleOffset, data, 0, tupleLength);

                if (hasBinaryState) {
                    // Object-binary-state
                    Object[] storedState;
                    if (state.state == null) {
                        storedState = new Object[8];
                        storedState[0] = new Integer(0);
                        state.state = storedState;
                    } else {
                        storedState = (Object[]) state.state;
                    }
                    int stateCount = (Integer) (storedState[0]);
                    if (stateCount + 1 >= storedState.length) {
                        storedState = Arrays.copyOf(storedState, storedState.length * 2);
                        state.state = storedState;
                    }

                    stateCount++;
                    storedState[0] = stateCount;
                    storedState[stateCount] = data;
                    try {
                        fieldOutput.writeInt(stateCount);
                    } catch (IOException e) {
                        throw new HyracksDataException(e.fillInStackTrace());
                    }
                } else {
                    // Only object-state
                    state.state = data;
                }
            }

            @Override
            public AggregateState createState() {
                return new AggregateState();
            }

            @Override
            public void close() {

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, byte[] data, int offset,
                    AggregateState state) throws HyracksDataException {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int tupleLength = accessor.getTupleEndOffset(tIndex) - tupleOffset + 1;

                if (hasBinaryState) {
                    int stateIdx = IntegerSerializerDeserializer.getInt(data, offset);

                    Object[] storedState = (Object[]) state.state;

                    byte[] newLoad = new byte[((byte[]) (storedState[stateIdx])).length + tupleLength];

                    System.arraycopy(((byte[]) (storedState[stateIdx])), 0, newLoad, 0,
                            ((byte[]) (storedState[stateIdx])).length);
                    System.arraycopy(accessor.getBuffer().array(), tupleOffset, newLoad,
                            ((byte[]) (storedState[stateIdx])).length, tupleLength);

                    storedState[stateIdx] = newLoad;
                } else {

                    byte[] newLoad = new byte[((byte[]) state.state).length + tupleLength];
                    System.arraycopy(((byte[]) state.state), 0, newLoad, 0, ((byte[]) state.state).length);
                    System.arraycopy(accessor.getBuffer().array(), tupleOffset, newLoad, ((byte[]) state.state).length,
                            tupleLength);

                    state.state = newLoad;
                }
            }

            @Override
            public int getInitSize(IFrameTupleAccessor accessor, int tIndex) {
                if (hasBinaryState) {
                    return 4;
                } else {
                    return 0;
                }
            }
        };
    }

}
