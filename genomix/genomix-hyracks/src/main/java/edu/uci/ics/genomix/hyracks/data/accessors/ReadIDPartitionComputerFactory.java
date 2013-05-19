package edu.uci.ics.genomix.hyracks.data.accessors;

import java.nio.ByteBuffer;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;

public class ReadIDPartitionComputerFactory implements ITuplePartitionComputerFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public ITuplePartitionComputer createPartitioner() {
        // TODO Auto-generated method stub
        return new ITuplePartitionComputer() {
            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) {
                int startOffset = accessor.getTupleStartOffset(tIndex);
                int fieldOffset = accessor.getFieldStartOffset(tIndex, 0);
                int slotLength = accessor.getFieldSlotsLength();

                ByteBuffer buf = accessor.getBuffer();

                int hash = Marshal.getInt(buf.array(), startOffset + fieldOffset + slotLength);
                if (hash < 0) {
                    hash = -(hash + 1);
                }

                return hash % nParts;
            }
        };
    }

}
