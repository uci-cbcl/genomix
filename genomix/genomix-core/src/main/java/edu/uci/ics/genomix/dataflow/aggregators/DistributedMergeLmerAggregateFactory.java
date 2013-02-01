package edu.uci.ics.genomix.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.genomix.data.serde.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class DistributedMergeLmerAggregateFactory implements IAggregatorDescriptorFactory {
    private static final long serialVersionUID = 1L;
    private static final int max = 255;

    public DistributedMergeLmerAggregateFactory() {
    }

    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        return new IAggregatorDescriptor() {

            @Override
            public void reset() {
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub

            }

            @Override
            public AggregateState createAggregateStates() {
                // TODO Auto-generated method stub
                return new AggregateState(new Object() {
                });
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                byte bitmap = 0;
                byte count = 0;
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, 1);
                bitmap |= ByteSerializerDeserializer.getByte(accessor.getBuffer().array(),
                        tupleOffset + accessor.getFieldSlotsLength() + fieldStart);

                tupleOffset = accessor.getTupleStartOffset(tIndex);
                fieldStart = accessor.getFieldStartOffset(tIndex, 2);
                int offset = tupleOffset + fieldStart + accessor.getFieldSlotsLength();

                count += ByteSerializerDeserializer.getByte(accessor.getBuffer().array(), offset);

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
                // TODO Auto-generated method stub
               
            	byte bitmap = 0;
                byte count = 0;

                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, 1);
                int offset = tupleOffset + accessor.getFieldSlotsLength() + fieldStart;
                bitmap |= ByteSerializerDeserializer.getByte(accessor.getBuffer().array(), offset);

                tupleOffset = accessor.getTupleStartOffset(tIndex);
                fieldStart = accessor.getFieldStartOffset(tIndex, 2);
                offset = tupleOffset + fieldStart + accessor.getFieldSlotsLength();
                count = ByteSerializerDeserializer.getByte(accessor.getBuffer().array(), offset);
                

                int statetupleOffset = stateAccessor.getTupleStartOffset(stateTupleIndex);
                int statefieldStart = stateAccessor.getFieldStartOffset(stateTupleIndex, 1);
                int stateoffset = statetupleOffset + stateAccessor.getFieldSlotsLength() + statefieldStart;

                byte[] data = stateAccessor.getBuffer().array();

                ByteBuffer buf = ByteBuffer.wrap(data);
                bitmap |= buf.getChar(stateoffset);
                buf.position(stateoffset+1);
                count += buf.get();
                
                if(count > max){
                	count = (byte) max;
                }
                
                buf.put(stateoffset, bitmap);
                buf.put(stateoffset + 1, count);
            }

            @Override
            public void outputPartialResult(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                // TODO Auto-generated method stub
                byte bitmap;
                byte count;
                DataOutput fieldOutput = tupleBuilder.getDataOutput();
                byte[] data = accessor.getBuffer().array();
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldOffset = accessor.getFieldStartOffset(tIndex, 1);

                int offset = fieldOffset + accessor.getFieldSlotsLength() + tupleOffset;
                bitmap = ByteSerializerDeserializer.getByte(data, offset);

                count = ByteSerializerDeserializer.getByte(data, offset + 1);
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
                // TODO Auto-generated method stub
                byte bitmap;
                byte count;

                byte[] data = accessor.getBuffer().array();
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldOffset = accessor.getFieldStartOffset(tIndex, 1);
                int offset = tupleOffset + accessor.getFieldSlotsLength() + fieldOffset;

                bitmap = ByteSerializerDeserializer.getByte(data, offset);
                count = ByteSerializerDeserializer.getByte(data, offset + 1);

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

        };
    }

}
