package edu.uci.ics.genomix.pregelix.checker;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.format.CheckerOutputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class SymmetryCheckerVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {

    @Override
    public void initVertex() {
        super.initVertex();
        if (outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if (getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
        outFlag = 0;
    }

    /**
     * check symmetry: A -> B, A'edgeMap should have B and B's corresponding edgeMap should have A
     * otherwise, output error vertices
     */
    public void checkSymmetry(Iterator<MessageWritable> msgIterator) {
        while (msgIterator.hasNext()) {
            MessageWritable incomingMsg = msgIterator.next();
            EDGETYPE neighborToMe = EDGETYPE.fromByte(incomingMsg.getFlag());
            boolean exist = getVertexValue().getEdgeMap(neighborToMe).containsKey(incomingMsg.getSourceVertexId());
            if (!exist) {
                getVertexValue().setState(State.ERROR_NODE);
            }
        }
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
        initVertex();
        if (getSuperstep() == 1) {
            sendSettledMsgToAllNeighborNodes(getVertexValue());
        } else if (getSuperstep() == 2) {
            //check if the corresponding edge exists
            checkSymmetry(msgIterator);
        }
        voteToHalt();
    }

    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexOutputFormatClass(CheckerOutputFormat.class);
        return job;
    }

}
