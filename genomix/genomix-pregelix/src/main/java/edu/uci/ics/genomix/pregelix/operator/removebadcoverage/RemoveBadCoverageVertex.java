package edu.uci.ics.genomix.pregelix.operator.removebadcoverage;

import java.util.Iterator;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable.State;

/**
 * Graph clean pattern: Remove Badcoverage
 * Details: Chimeric reads and other sequencing artifacts create edges that are
 * only supported by a small number of reads. Besides that, there are some nodes 
 * with a very high coverage because of repeats. These edges/nodes are identified
 * and removed. This is then followed by recompressing the graph. 
 * 
 */
public class RemoveBadCoverageVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {
    protected static float minAverageCoverage = -1;
    protected static float maxAverageCoverage = -1;

    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (minAverageCoverage < 0)
            minAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.REMOVE_BAD_COVERAGE_MIN_COVERAGE));
        if(maxAverageCoverage < 0)
        	maxAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.REMOVE_BAD_COVERAGE_MAX_COVERAGE));
        if (outgoingMsg == null)
            outgoingMsg = new MessageWritable();
    }

    public void detectLowCoverageVertex() {
        VertexValueWritable vertex = getVertexValue();
        if (vertex.getAverageCoverage() < minAverageCoverage || vertex.getAverageCoverage() > maxAverageCoverage) {
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

}
