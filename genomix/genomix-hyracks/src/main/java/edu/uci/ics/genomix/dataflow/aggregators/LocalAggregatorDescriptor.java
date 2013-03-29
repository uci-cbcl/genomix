package edu.uci.ics.genomix.dataflow.aggregators;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.std.accessors.ByteSerializerDeserializer;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class LocalAggregatorDescriptor implements IAggregatorDescriptor {
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

	protected byte getField(IFrameTupleAccessor accessor, int tIndex,
			int fieldId) {
		int tupleOffset = accessor.getTupleStartOffset(tIndex);
		int fieldStart = accessor.getFieldStartOffset(tIndex, fieldId);
		int offset = tupleOffset + fieldStart + accessor.getFieldSlotsLength();
		byte data = ByteSerializerDeserializer.getByte(accessor.getBuffer()
				.array(), offset);
		return data;
	}

	@Override
	public void init(ArrayTupleBuilder tupleBuilder,
			IFrameTupleAccessor accessor, int tIndex, AggregateState state)
			throws HyracksDataException {
		byte bitmap = getField(accessor, tIndex, 1);
		byte count = 1;

		DataOutput fieldOutput = tupleBuilder.getDataOutput();
		try {
			fieldOutput.writeByte(bitmap);
			tupleBuilder.addFieldEndOffset();
			fieldOutput.writeByte(count);
			tupleBuilder.addFieldEndOffset();
		} catch (IOException e) {
			throw new HyracksDataException(
					"I/O exception when initializing the aggregator.");
		}
	}

	@Override
	public void aggregate(IFrameTupleAccessor accessor, int tIndex,
			IFrameTupleAccessor stateAccessor, int stateTupleIndex,
			AggregateState state) throws HyracksDataException {
		byte bitmap = getField(accessor, tIndex, 1);
		short count = 1;

		int statetupleOffset = stateAccessor
				.getTupleStartOffset(stateTupleIndex);
		int bitfieldStart = stateAccessor.getFieldStartOffset(stateTupleIndex,
				1);
		int countfieldStart = stateAccessor.getFieldStartOffset(
				stateTupleIndex, 2);
		int bitoffset = statetupleOffset + stateAccessor.getFieldSlotsLength()
				+ bitfieldStart;
		int countoffset = statetupleOffset
				+ stateAccessor.getFieldSlotsLength() + countfieldStart;

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
	public void outputPartialResult(ArrayTupleBuilder tupleBuilder,
			IFrameTupleAccessor accessor, int tIndex, AggregateState state)
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
			throw new HyracksDataException(
					"I/O exception when writing aggregation to the output buffer.");
		}

	}

	@Override
	public void outputFinalResult(ArrayTupleBuilder tupleBuilder,
			IFrameTupleAccessor accessor, int tIndex, AggregateState state)
			throws HyracksDataException {
		outputPartialResult(tupleBuilder, accessor, tIndex, state);
	}

};
