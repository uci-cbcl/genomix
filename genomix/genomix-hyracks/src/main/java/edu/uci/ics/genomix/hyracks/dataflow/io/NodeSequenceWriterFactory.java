package edu.uci.ics.genomix.hyracks.dataflow.io;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.hyracks.dataflow.MapReadToNodeOperator;
import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;

@SuppressWarnings("deprecation")
public class NodeSequenceWriterFactory implements ITupleWriterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public static final int InputNodeIDField = MapReadToNodeOperator.OutputNodeIDField;
    public static final int InputCountOfKmerField = MapReadToNodeOperator.OutputCountOfKmerField;
    public static final int InputIncomingField = MapReadToNodeOperator.OutputIncomingField;
    public static final int InputOutgoingField = MapReadToNodeOperator.OutputOutgoingField;
    public static final int InputKmerBytesField = MapReadToNodeOperator.OutputKmerBytesField;

    private ConfFactory confFactory;
    private final int kmerlength;

    public NodeSequenceWriterFactory(JobConf conf) throws HyracksDataException {
        this.confFactory = new ConfFactory(conf);
        this.kmerlength = conf.getInt(GenomixJobConf.KMER_LENGTH, GenomixJobConf.DEFAULT_KMERLEN);
    }

    public class TupleWriter implements ITupleWriter {

        public TupleWriter(ConfFactory confFactory) {
            this.cf = confFactory;
        }

        ConfFactory cf;
        Writer writer = null;
        NodeWritable node = new NodeWritable(kmerlength);

        @Override
        public void open(DataOutput output) throws HyracksDataException {
            try {
                writer = SequenceFile.createWriter(cf.getConf(), (FSDataOutputStream) output, NodeWritable.class, null,
                        CompressionType.NONE, null);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
            node.getNodeID().setNewReference(tuple.getFieldData(InputNodeIDField),
                    tuple.getFieldStart(InputNodeIDField));
            node.setCount(Marshal.getInt(tuple.getFieldData(InputCountOfKmerField),
                    tuple.getFieldStart(InputCountOfKmerField)));
            node.getIncomingList().setNewReference(tuple.getFieldLength(InputIncomingField) / PositionWritable.LENGTH,
                    tuple.getFieldData(InputIncomingField), tuple.getFieldStart(InputIncomingField));
            node.getOutgoingList().setNewReference(tuple.getFieldLength(InputOutgoingField) / PositionWritable.LENGTH,
                    tuple.getFieldData(InputOutgoingField), tuple.getFieldStart(InputOutgoingField));

            node.getKmer().setNewReference(node.getCount() + kmerlength - 1, tuple.getFieldData(InputKmerBytesField),
                    tuple.getFieldStart(InputKmerBytesField));

            try {
                writer.append(node, null);
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public void close(DataOutput output) throws HyracksDataException {
            // TODO Auto-generated method stub

        }

    }

    /**
     * Input schema:
     * (Position, LengthCount, InComingPosList, OutgoingPosList, Kmer)
     */
    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        return new TupleWriter(confFactory);
    }

}
