package edu.uci.ics.genomix.hyracks.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.hyracks.data.accessors.ByteSerializerDeserializer;
import edu.uci.ics.genomix.hyracks.dataflow.MapKmerPositionToReadOperator;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class AggregateReadIDAggregateFactory implements IAggregatorDescriptorFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    public static final int InputReadIDField = MapKmerPositionToReadOperator.OutputReadIDField;
    public static final int InputPosInReadField = MapKmerPositionToReadOperator.OutputPosInReadField;
    public static final int InputPositionListField = MapKmerPositionToReadOperator.OutputOtherReadIDListField;
    public static final int InputKmerField = MapKmerPositionToReadOperator.OutputKmerField;

    public static final int OutputReadIDField = 0;
    public static final int OutputPositionListField = 1;

    public AggregateReadIDAggregateFactory() {
    }

    /**
     * (ReadID,PosInRead,{OtherPosition,...},Kmer) to
     * (ReadID, {(PosInRead,{OtherPositoin..},Kmer) ...}
     */
    @Override
    public IAggregatorDescriptor createAggregator(IHyracksTaskContext ctx, RecordDescriptor inRecordDescriptor,
            RecordDescriptor outRecordDescriptor, int[] keyFields, int[] keyFieldsInPartialResults)
            throws HyracksDataException {
        return new IAggregatorDescriptor() {

            protected int getOffSet(IFrameTupleAccessor accessor, int tIndex, int fieldId) {
                int tupleOffset = accessor.getTupleStartOffset(tIndex);
                int fieldStart = accessor.getFieldStartOffset(tIndex, fieldId);
                int offset = tupleOffset + fieldStart + accessor.getFieldSlotsLength();
                return offset;
            }

            protected byte readByteField(IFrameTupleAccessor accessor, int tIndex, int fieldId) {
                return ByteSerializerDeserializer.getByte(accessor.getBuffer().array(),
                        getOffSet(accessor, tIndex, fieldId));
            }

            @Override
            public AggregateState createAggregateStates() {
                return new AggregateState(new ArrayBackedValueStorage());
            }

            @Override
            public void init(ArrayTupleBuilder tupleBuilder, IFrameTupleAccessor accessor, int tIndex,
                    AggregateState state) throws HyracksDataException {
                ArrayBackedValueStorage storage = (ArrayBackedValueStorage) state.state;
                storage.reset();
                DataOutput out = storage.getDataOutput();
                byte posInRead = readByteField(accessor, tIndex, InputPositionListField);

                try {
                    out.writeByte(posInRead);
                    writeBytesToStorage(out, accessor, tIndex, InputPositionListField);
                    writeBytesToStorage(out, accessor, tIndex, InputKmerField);
                } catch (IOException e) {
                    throw new HyracksDataException("Failed to write into temporary storage");
                }

            }

            private void writeBytesToStorage(DataOutput out, IFrameTupleAccessor accessor, int tIndex, int idField)
                    throws IOException {
                int len = accessor.getFieldLength(tIndex, idField);
                out.writeInt(len);
                out.write(accessor.getBuffer().array(), getOffSet(accessor, tIndex, idField), len);
            }

            @Override
            public void reset() {
                // TODO Auto-generated method stub

            }

            @Override
            public void aggregate(IFrameTupleAccessor accessor, int tIndex, IFrameTupleAccessor stateAccessor,
                    int stateTupleIndex, AggregateState state) throws HyracksDataException {
                ArrayBackedValueStorage storage = (ArrayBackedValueStorage) state.state;
                DataOutput out = storage.getDataOutput();
                byte posInRead = readByteField(accessor, tIndex, InputPositionListField);

                try {
                    out.writeByte(posInRead);
                    writeBytesToStorage(out, accessor, tIndex, InputPositionListField);
                    writeBytesToStorage(out, accessor, tIndex, InputKmerField);
                } catch (IOException e) {
                    throw new HyracksDataException("Failed to write into temporary storage");
                }
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
                ArrayBackedValueStorage inputVal = (ArrayBackedValueStorage) state.state;
                try {
                    fieldOutput.write(inputVal.getByteArray(), inputVal.getStartOffset(), inputVal.getLength());
                    tupleBuilder.addFieldEndOffset();
                } catch (IOException e) {
                    throw new HyracksDataException("I/O exception when writing aggregation to the output buffer.");
                }
            }

            @Override
            public void close() {
                // TODO Auto-generated method stub

            }

        };
    }
}
