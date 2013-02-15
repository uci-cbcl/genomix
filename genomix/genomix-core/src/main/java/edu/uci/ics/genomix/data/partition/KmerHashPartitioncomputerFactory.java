package edu.uci.ics.genomix.data.partition;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.pregelix.core.util.BufferSerDeUtils;

public class KmerHashPartitioncomputerFactory implements
		ITuplePartitionComputerFactory {

	private static final long serialVersionUID = 1L;

	public KmerHashPartitioncomputerFactory() {
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

				ByteBuffer buf = accessor.getBuffer();
//				buf.position(startOffset + fieldOffset + slotLength);
//				long l = accessor.getBuffer().getLong();
				long l = BufferSerDeUtils.getLong(buf.array(), startOffset + fieldOffset + slotLength);
				return (int) (l % nParts);
			}
		};
	}
}
