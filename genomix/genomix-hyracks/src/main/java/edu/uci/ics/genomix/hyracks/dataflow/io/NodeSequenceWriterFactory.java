package edu.uci.ics.genomix.hyracks.dataflow.io;

import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class NodeSequenceWriterFactory implements ITupleWriterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public NodeSequenceWriterFactory(JobConf job) {
        // TODO Auto-generated constructor stub
    }

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

}
