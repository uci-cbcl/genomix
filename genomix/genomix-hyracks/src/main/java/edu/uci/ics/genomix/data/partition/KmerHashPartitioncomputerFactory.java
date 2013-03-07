package edu.uci.ics.genomix.data.partition;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;

public class KmerHashPartitioncomputerFactory implements
		ITuplePartitionComputerFactory {

	private static final long serialVersionUID = 1L;

	public static int hashBytes(byte[] bytes, int offset, int length) {
		int hash = 1;
		for (int i = offset; i < offset + length; i++)
			hash = (31 * hash) + (int) bytes[i];
		
		return hash;
	}

	@Override
	public ITuplePartitionComputer createPartitioner() {
		return new ITuplePartitionComputer() {
			@Override
			public int partition(IFrameTupleAccessor accessor, int tIndex,
					int nParts) {
				if (nParts == 1) {
					return 0;
				}
				int startOffset = accessor.getTupleStartOffset(tIndex);
				int fieldOffset = accessor.getFieldStartOffset(tIndex, 0);
				int slotLength = accessor.getFieldSlotsLength();
				int fieldLength = accessor.getFieldLength(tIndex, 0);

				ByteBuffer buf = accessor.getBuffer();

				int part = hashBytes(buf.array(), startOffset + fieldOffset + slotLength, fieldLength) % nParts;
				if (part < 0){
					part = -(part+1);
				}
				return part;
			}
		};
	}
}
