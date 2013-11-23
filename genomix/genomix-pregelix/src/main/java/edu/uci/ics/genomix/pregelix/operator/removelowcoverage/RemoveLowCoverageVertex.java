package edu.uci.ics.genomix.pregelix.operator.removelowcoverage;

import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.EDGETYPE;

/**
 * Graph clean pattern: Remove Lowcoverage
 * Detais: Chimeric reads and other sequencing artifacts create edges that are
 * only supported by a small number of reads. These edges are identified
 * and removed. This is then followed by recompressing the graph.
 */
public class RemoveLowCoverageVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {
    private static float minAverageCoverage = -1;

    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (minAverageCoverage < 0)
            minAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.REMOVE_LOW_COVERAGE_MAX_COVERAGE));
        if (outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        if (getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    public void detectLowCoverageVertex() {
        VertexValueWritable vertex = getVertexValue();
        if (vertex.getAverageCoverage() <= minAverageCoverage) {
            //broadcase kill self
            broadcastKillself();
            vertex.setState(State.DEAD_NODE);
            activate();
        } else
            voteToHalt();
    }

    public void cleanupDeadVertex() {
        deleteVertex(getVertexId());
        //set statistics counter: Num_RemovedLowCoverageNodes
        incrementCounter(StatisticsCounter.Num_RemovedLowCoverageNodes);
        getVertexValue().setCounters(counters);
    }

    public void responseToDeadVertex(Iterator<MessageWritable> msgIterator) {
        MessageWritable incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            //response to dead node
            EDGETYPE deadToMeEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            getVertexValue().getEdges(deadToMeEdgetype).remove(incomingMsg.getSourceVertexId());
        }
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (verbose)
            LOG.fine("Iteration " + getSuperstep() + " for key " + getVertexId());
        if (getSuperstep() == 1) {
            detectLowCoverageVertex();
        } else if (getSuperstep() == 2) {
            if (getVertexValue().getState() == State.DEAD_NODE) {
                cleanupDeadVertex();
            } else {
                responseToDeadVertex(msgIterator);
            }
            voteToHalt();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, RemoveLowCoverageVertex.class));
    }
}
