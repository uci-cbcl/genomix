package edu.uci.ics.genomix.data.partition;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;

public class KmerHashPartitioncomputerFactory implements
		ITuplePartitionComputerFactory {

	private static final long serialVersionUID = 1L;

	public KmerHashPartitioncomputerFactory() {
	}

    public static long getLong(byte[] bytes, int offset) {
        return (((long) (bytes[offset] & 0xff)) << 56) + (((long) (bytes[offset + 1] & 0xff)) << 48)
                + (((long) (bytes[offset + 2] & 0xff)) << 40) + (((long) (bytes[offset + 3] & 0xff)) << 32)
                + (((long) (bytes[offset + 4] & 0xff)) << 24) + (((long) (bytes[offset + 5] & 0xff)) << 16)
                + (((long) (bytes[offset + 6] & 0xff)) << 8) + (((long) (bytes[offset + 7] & 0xff)) << 0);
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
				long l = getLong(buf.array(), startOffset
						+ fieldOffset + slotLength);
				return (int) (l % nParts);
			}
		};
	}
}
