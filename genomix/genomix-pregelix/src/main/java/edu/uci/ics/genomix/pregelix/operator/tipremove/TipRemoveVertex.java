package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.util.Iterator;
import java.util.logging.Logger;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.GraphMutations;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

/**
 * Remove tip or single node when kmerLength < MIN_LENGTH_TO_KEEP
 * Details: Sequencing errors at the ends of the reads form "tips": short, low coverage nodes
 * with in-degree + out-degree = 1 (they either have a single edge in or a single edge out).
 * The algorithm identifies these nodes and prunes them from the graph. This is then followed
 * by recompressing the graph.
 */
public class TipRemoveVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {

    private static final Logger LOG = Logger.getLogger(TipRemoveVertex.class.getName());

    private int MIN_LENGTH_TO_KEEP = -1;

    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (MIN_LENGTH_TO_KEEP == -1) {
            MIN_LENGTH_TO_KEEP = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.TIP_REMOVE_MAX_LENGTH));
        }
        if (outgoingMsg == null) {
            outgoingMsg = new MessageWritable();
        } else {
            outgoingMsg.reset();
        }
    }

    /**
     * detect the tip and figure out what edgeType neighborToTip is
     */
    public EDGETYPE getTipToNeighbor() {
        VertexValueWritable vertex = getVertexValue();
        for (DIR d : DIR.values()) {
            if (vertex.degree(d) == 1 && vertex.degree(d.mirror()) == 0) {
                return vertex.getNeighborEdgeType(d);
            }
        }
        return null;
    }

    /**
     * step 1
     */
    public void updateTipNeighbor() {
        EDGETYPE tipToNeighborEdgetype = getTipToNeighbor();
        //I'm tip and my length is less than the minimum
        if (tipToNeighborEdgetype != null && getVertexValue().getKmerLength() <= MIN_LENGTH_TO_KEEP) {
            outgoingMsg.reset();
            outgoingMsg.setFlag(tipToNeighborEdgetype.mirror().get());
            outgoingMsg.setSourceVertexId(getVertexId());
            VKmerList edges = getVertexValue().getEdges(tipToNeighborEdgetype);
            if (edges.size() != 1)
                throw new IllegalArgumentException("In this edgeType, the size of edges has to be 1!");
            VKmer destVertexId = edges.getPosition(0);
            sendMsg(destVertexId, outgoingMsg);
            deleteVertex(getVertexId());

            if (verbose) {
                LOG.fine("I'm tip! " + "\r\n" + "My vertexId is " + getVertexId() + "\r\n" + "My vertexValue is "
                        + getVertexValue() + "\r\n" + "Kill self and broadcast kill self to " + destVertexId + "\r\n"
                        + "The message is: " + outgoingMsg + "\r\n\n");
            }
            getCounters().findCounter(GraphMutations.Num_RemovedTips).increment(1);
        }
    }

    /**
     * step 2
     */
    public void processUpdates(Iterator<MessageWritable> msgIterator) {
        pruneDeadEdges(msgIterator);
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        if (getSuperstep() == 1) {
            initVertex();
            updateTipNeighbor();
        } else if (getSuperstep() == 2) {
            processUpdates(msgIterator);
        }
        voteToHalt();
    }
}
