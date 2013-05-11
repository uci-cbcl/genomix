package edu.uci.ics.genomix.hyracks.dataflow;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;

public class NodeTextWriterFactory implements ITupleWriterFactory {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

}
