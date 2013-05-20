package edu.uci.ics.genomix.hyracks.dataflow.io;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class NodeTextWriterFactory implements ITupleWriterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final int initialKmerSize;

    public NodeTextWriterFactory(int initialKmerSize) {
        this.initialKmerSize = initialKmerSize;
    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        return new ITupleWriter() {
            NodeWritable node = new NodeWritable(initialKmerSize);

            @Override
            public void open(DataOutput output) throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                node.getNodeID().setNewReference(tuple.getFieldData(NodeSequenceWriterFactory.InputNodeIDField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputNodeIDField));
                node.getIncomingList().setNewReference(
                        PositionListWritable.getCountByDataLength(tuple
                                .getFieldLength(NodeSequenceWriterFactory.InputIncomingField)),
                        tuple.getFieldData(NodeSequenceWriterFactory.InputIncomingField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputIncomingField));
                node.getOutgoingList().setNewReference(
                        PositionListWritable.getCountByDataLength(tuple
                                .getFieldLength(NodeSequenceWriterFactory.InputOutgoingField)),
                        tuple.getFieldData(NodeSequenceWriterFactory.InputOutgoingField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputOutgoingField));

                node.getKmer().setNewReference(
                        Marshal.getInt(tuple.getFieldData(NodeSequenceWriterFactory.InputCountOfKmerField),
                                tuple.getFieldStart(NodeSequenceWriterFactory.InputCountOfKmerField)),
                        tuple.getFieldData(NodeSequenceWriterFactory.InputKmerBytesField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputKmerBytesField));
                try {
                    output.write(node.toString().getBytes());
                    output.writeByte('\n');
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            @Override
            public void close(DataOutput output) throws HyracksDataException {
                // TODO Auto-generated method stub

            }

        };
    }

}
