package edu.uci.ics.genomix.hyracks.dataflow.io;

import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;
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
                node.getFFList().setNewReference(
                        tuple.getFieldLength(NodeSequenceWriterFactory.InputFFField) / PositionWritable.LENGTH,
                        tuple.getFieldData(NodeSequenceWriterFactory.InputFFField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputFFField));
                node.getFRList().setNewReference(
                        tuple.getFieldLength(NodeSequenceWriterFactory.InputFRField) / PositionWritable.LENGTH,
                        tuple.getFieldData(NodeSequenceWriterFactory.InputFRField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputFRField));
                node.getRFList().setNewReference(
                        tuple.getFieldLength(NodeSequenceWriterFactory.InputRFField) / PositionWritable.LENGTH,
                        tuple.getFieldData(NodeSequenceWriterFactory.InputRFField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputRFField));
                node.getRRList().setNewReference(
                        tuple.getFieldLength(NodeSequenceWriterFactory.InputRRField) / PositionWritable.LENGTH,
                        tuple.getFieldData(NodeSequenceWriterFactory.InputRRField),
                        tuple.getFieldStart(NodeSequenceWriterFactory.InputRRField));

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
