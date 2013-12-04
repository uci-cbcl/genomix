package edu.uci.ics.genomix.pregelix.operator.removelowcoverage;

import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.GraphMutations;
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
    }

    public void responseToDeadVertex(Iterator<MessageWritable> msgIterator) {
        MessageWritable incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            //response to dead node
            EDGETYPE deadToMeEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            getVertexValue().getEdgeMap(deadToMeEdgetype).remove(incomingMsg.getSourceVertexId());
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

}
