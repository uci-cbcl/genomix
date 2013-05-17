package edu.uci.ics.genomix.hyracks.data.accessors;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

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

                int hash = IntegerSerializerDeserializer.getInt(buf.array(), startOffset + fieldOffset + slotLength);
                if (hash < 0) {
                    hash = -(hash + 1);
                }

                return hash % nParts;
            }
        };
    }

}
