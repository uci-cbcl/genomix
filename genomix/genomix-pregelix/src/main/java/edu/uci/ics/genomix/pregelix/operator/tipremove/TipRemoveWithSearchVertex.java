package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;

/**
 * Remove tip or single node when kmerLength < MIN_LENGTH_TO_KEEP
 * Details: Sequencing errors at the ends of the reads form "tips": short, low coverage nodes
 * with in-degree + out-degree = 1 (they either have a single edge in or a single edge out).
 * The algorithm identifies these nodes and prunes them from the graph. This is then followed
 * by recompressing the graph.
 * This variant of the algorithm can identify tips in an uncompressed graph
 */
public class TipRemoveWithSearchVertex extends
        DeBruijnGraphCleanVertex<VertexValueWritable, TipRemoveWithSearchMessage> {

    private static final Logger LOG = Logger.getLogger(TipRemoveWithSearchVertex.class.getName());

    private static int MIN_LENGTH_TO_KEEP = -1;

    @Override
    public void compute(Iterator<TipRemoveWithSearchMessage> msgIterator) {
        if (getSuperstep() == 1) {
            initVertex();
            msgIterator = initiateTipSearch();
        }
        processSearch(msgIterator);
        voteToHalt();
    }

    @Override
    public void initVertex() {
        super.initVertex();
        if (MIN_LENGTH_TO_KEEP == -1) {
            MIN_LENGTH_TO_KEEP = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.TIP_REMOVE_MAX_LENGTH));
        }
    }

    /**
     * simulate an incoming tip msg if this is a tip. this lets us call the processSearch function in each iteration
     */
    public Iterator<TipRemoveWithSearchMessage> initiateTipSearch() {
        Node node = getVertexValue();
        ArrayList<TipRemoveWithSearchMessage> tipMsgs = new ArrayList<TipRemoveWithSearchMessage>();
        if (node.degree(DIR.FORWARD) == 0) {
            TipRemoveWithSearchMessage tipMsg = new TipRemoveWithSearchMessage();
            tipMsg.setFlag(EDGETYPE.RR.get());
            tipMsgs.add(tipMsg);
        } else if (node.degree(DIR.REVERSE) == 0) {
            TipRemoveWithSearchMessage tipMsg = new TipRemoveWithSearchMessage();
            tipMsg.setFlag(EDGETYPE.FF.get());
            tipMsgs.add(tipMsg);
        }
        return tipMsgs.iterator();
    }

    public void processSearch(Iterator<TipRemoveWithSearchMessage> msgIterator) {
        Node node = getVertexValue();
        TipRemoveWithSearchMessage incomingMsg;
        boolean stop;
        EDGETYPE inET;
        DIR toPrevDir;
        DIR outDir;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            inET = EDGETYPE.fromByte(incomingMsg.getFlag());
            outDir = inET.neighborDir(); // way I'd leave this node
            toPrevDir = outDir.mirror(); // direction towards last visited node
            if (node.degree(toPrevDir) > 1) {
                // reached node A which has multiple incoming edges (where B is origin  ...A<__B)
                // stop the search without considering this node
                stop = true;
            } else if (node.degree(outDir) > 1) {
                // reached node A with multiple paths leading out of this node  (where B is origin  ...>A----B)
                // this is NOT a tip! must drop this message
                continue;
            } else if (node.degree(outDir) == 0) {
                // no longer a path in this direction; stop at this node  ( A----B ) 
                stop = true;
                incomingMsg.visitNode(getVertexId(), node);
            } else {
                // a simple path node; continue the search
                stop = false;
                incomingMsg.visitNode(getVertexId(), node);
            }

            if (incomingMsg.getVisitedLength() < MIN_LENGTH_TO_KEEP) {
                if (stop) {
                    deleteVisitedNodes(incomingMsg);
                } else {
                    continueSearch(outDir, incomingMsg);
                }
            }
        }
    }

    private void deleteVisitedNodes(TipRemoveWithSearchMessage msg) {
        Node node = getVertexValue();
        EDGETYPE inET = EDGETYPE.fromByte(msg.getFlag());
        VKmerList visitedNodes = msg.getVisitedNodes();
        if (visitedNodes.size() == 0) {
            return;
        }
        for (VKmer id : visitedNodes) {
            deleteVertex(id);
        }
        // the last visited node is either me or I have a dangling edge to it
        // if it's not me, remove the edge
        VKmer lastVisited = visitedNodes.getPosition(visitedNodes.size() - 1);
        if (!lastVisited.equals(getVertexId())) {
            // I am not in the path but have an edge towards the deleted node
            node.getEdges(inET.mirror()).remove(lastVisited);
        }
    }

    private void continueSearch(DIR outDir, TipRemoveWithSearchMessage msg) {
        Node node = getVertexValue();
        if (node.degree(outDir) != 1) {
            throw new IllegalStateException("Should have degree == 1 in " + outDir + ". I am " + node);
        }
        for (EDGETYPE outET : outDir.edgeTypes()) {
            for (VKmer id : node.getEdges(outET)) {
                msg.setFlag(outET.get());
                sendMsg(id, msg);
            }
        }
    }
}
