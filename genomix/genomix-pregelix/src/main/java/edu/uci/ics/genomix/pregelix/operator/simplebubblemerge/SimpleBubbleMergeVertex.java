package edu.uci.ics.genomix.pregelix.operator.simplebubblemerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.Node.NeighborInfo;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.types.GraphMutations;

/**
 * Graph clean pattern: Simple Bubble Merge
 */
public class SimpleBubbleMergeVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, SimpleBubbleMergeMessage> {

    private static final Logger LOG = Logger.getLogger(SimpleBubbleMergeVertex.class.getName());

    private static float DISSIMILAR_THRESHOLD = -1;

    private Map<VKmer, ArrayList<SimpleBubbleMergeMessage>> receivedMsgMap = new HashMap<VKmer, ArrayList<SimpleBubbleMergeMessage>>();
    private ArrayList<SimpleBubbleMergeMessage> receivedMsgList = new ArrayList<SimpleBubbleMergeMessage>();
    private SimpleBubbleMergeMessage topMsg = new SimpleBubbleMergeMessage();
    private SimpleBubbleMergeMessage curMsg = new SimpleBubbleMergeMessage();

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (DISSIMILAR_THRESHOLD < 0)
            DISSIMILAR_THRESHOLD = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.BUBBLE_MERGE_MAX_DISSIMILARITY));
        if (outgoingMsg == null)
            outgoingMsg = new SimpleBubbleMergeMessage();
    }

    public void sendBubbleAndMajorVertexMsgToMinorVertex() {
        VertexValueWritable vertex = getVertexValue();
        // SingleNeighbor: single neighbor in the given direction, return null if there are multiple or no neighbors
        NeighborInfo reverseNeighbor = vertex.getSingleNeighbor(DIR.REVERSE);
        NeighborInfo forwardNeighbor = vertex.getSingleNeighbor(DIR.FORWARD);

        // get majorVertex and minorVertex and meToMajorEdgeType and meToMinorEdgeType
        if (forwardNeighbor.kmer.equals(reverseNeighbor.kmer)) {
            LOG.fine("majorVertexId is equal to minorVertexId, this is not allowed!\n" + "forwardKmer is "
                    + forwardNeighbor.kmer + "\n" + "reverseKmer is " + reverseNeighbor.kmer + "\n" + "this vertex is "
                    + getVertexId() + ", value: " + getVertexValue());
            return;
        }

        outgoingMsg.reset();
        VKmer minorVertexId;
        boolean forwardIsMajor = forwardNeighbor.kmer.compareTo(reverseNeighbor.kmer) >= 0;
        if (forwardIsMajor) {
            outgoingMsg.setMajorVertexId(forwardNeighbor.kmer);
            outgoingMsg.setMajorToBubbleEdgetype(forwardNeighbor.et.mirror());
            outgoingMsg.setMinorToBubbleEdgetype(reverseNeighbor.et.mirror());
            minorVertexId = reverseNeighbor.kmer;
        } else {
            outgoingMsg.setMajorVertexId(reverseNeighbor.kmer);
            outgoingMsg.setMajorToBubbleEdgetype(reverseNeighbor.et.mirror());
            outgoingMsg.setMinorToBubbleEdgetype(forwardNeighbor.et.mirror());
            minorVertexId = forwardNeighbor.kmer;
        }
        if (verbose) {
            LOG.info("Major vertex is: " + outgoingMsg.getMajorVertexId());
        }
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setNode(getVertexValue());
        sendMsg(minorVertexId, outgoingMsg);
    }

    public void aggregateBubbleNodesByMajorNode(Iterator<SimpleBubbleMergeMessage> msgIterator) {
        receivedMsgMap.clear();
        SimpleBubbleMergeMessage incomingMsg;
        ArrayList<SimpleBubbleMergeMessage> curMsgList;

        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (!receivedMsgMap.containsKey(incomingMsg.getMajorVertexId())) {
                curMsgList = new ArrayList<SimpleBubbleMergeMessage>();
                receivedMsgMap.put(incomingMsg.getMajorVertexId(), curMsgList);
            } else {
                curMsgList = receivedMsgMap.get(incomingMsg.getMajorVertexId());
            }
            curMsgList.add(incomingMsg);
        }
    }

    public static boolean isValidMajorAndMinor(SimpleBubbleMergeMessage topMsg, SimpleBubbleMergeMessage curMsg) {
        EDGETYPE topMajorToBubbleEdgetype = topMsg.getMajorToBubbleEdgetype();
        EDGETYPE curMajorToBubbleEdgetype = curMsg.getMajorToBubbleEdgetype();
        EDGETYPE topMinorToBubbleEdgetype = topMsg.getMinorToBubbleEdgetype();
        EDGETYPE curMinorToBubbleEdgetype = curMsg.getMinorToBubbleEdgetype();
        return (topMajorToBubbleEdgetype.dir() == curMajorToBubbleEdgetype.dir())
                && topMinorToBubbleEdgetype.dir() == curMinorToBubbleEdgetype.dir();
    }

    public void processSimilarSet() {
        while (!receivedMsgList.isEmpty()) {
            // add 'editDistance' and 'pathLength' to statistics distribution
            for(int i = 0; i < receivedMsgList.size(); i ++){
                for(int j = i + 1; j < receivedMsgList.size(); j ++){
                    float editDistance = receivedMsgList.get(i).editDistance(receivedMsgList.get(j));
                    updateStats("editDistance", Math.round(editDistance));
                }
                updateStats("pathLength", receivedMsgList.get(i).getNode().getKmerLength());
            }
            
            int removedNum = 0;
            Iterator<SimpleBubbleMergeMessage> it = receivedMsgList.iterator();
            topMsg = it.next();
            it.remove(); //delete topCoverage node
            Node topNode = topMsg.getNode();
            boolean topChanged = false;
            while (it.hasNext()) {
                curMsg = it.next();
                //check if the vertex is valid minor and if it comes from valid major
                if (!isValidMajorAndMinor(topMsg, curMsg))
                    continue;

                //compute the dissimilarity
                float fractionDissimilar = topMsg.computeDissimilar(curMsg); // TODO change to simmilarity everywhere
                
                if (fractionDissimilar <= DISSIMILAR_THRESHOLD) { //if similar with top node, delete this node
                    topChanged = true;
                    boolean sameOrientation = topMsg.sameOrientation(curMsg);
                    // 1. add coverage to top node -- for unchangedSet
                    topNode.addFromNode(!sameOrientation, curMsg.getNode());

                    // 2. send message to delete vertices -- for deletedSet
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(MESSAGETYPE.KILL_SELF.get());
                    sendMsg(curMsg.getSourceVertexId(), outgoingMsg);
                    it.remove();
                    removedNum++;
                }
            }
            // add 'removedPaths' to statistics distribution
            updateStats("removedPaths", removedNum);
            
            // process topNode -- send message to topVertex to update their coverage
            if (topChanged) {
                outgoingMsg.reset();
                outgoingMsg.setFlag(MESSAGETYPE.REPLACE_NODE.get());
                outgoingMsg.setNode(topNode);
                sendMsg(topMsg.getSourceVertexId(), outgoingMsg);
            }
        }
    }

    /**
     * step 1
     */
    public void detectBubble() {
        VertexValueWritable vertex = getVertexValue();
        if (vertex.degree(DIR.REVERSE) == 1 && vertex.degree(DIR.FORWARD) == 1) {
            // send bubble and major vertex msg to minor vertex 
            sendBubbleAndMajorVertexMsgToMinorVertex();
        }
    }

    /**
     * step 2
     */
    public void processBubblesInMinorVertex(Iterator<SimpleBubbleMergeMessage> msgIterator) {
        // aggregate bubble nodes and grouped by major vertex
        aggregateBubbleNodesByMajorNode(msgIterator);

        for (VKmer majorVertexId : receivedMsgMap.keySet()) {
            receivedMsgList = receivedMsgMap.get(majorVertexId);
            if (receivedMsgList.size() > 1) { // filter simple paths
                // add to 'totalPaths' statistics distribution
                updateStats("totalPaths", receivedMsgList.size());
                
                // for each majorVertex, sort the node by decreasing order of coverage
                Collections.sort(receivedMsgList, new SimpleBubbleMergeMessage.SortByCoverage());

                // process similarSet, keep the unchanged set and deleted set & add coverage to unchange node 
                processSimilarSet();
                
                getCounters().findCounter(GraphMutations.Num_RemovedBubbles).increment(1);
            }
        }
    }

    /**
     * step 3
     */
    public void receiveUpdates(Iterator<SimpleBubbleMergeMessage> msgIterator) {
        while (msgIterator.hasNext()) {
            SimpleBubbleMergeMessage incomingMsg = msgIterator.next();
            MESSAGETYPE mt = MESSAGETYPE.fromByte(incomingMsg.getFlag());
            switch (mt) {
                case REPLACE_NODE:
                    // update Node including average coverage 
                    getVertexValue().setNode(incomingMsg.getNode());
                    activate();
                    break;
                case KILL_SELF:
                    broadcastKillself();
                    deleteVertex(getVertexId());
                    break;
                default:
                    throw new IllegalStateException("The received message types should have only two kinds: "
                            + MESSAGETYPE.REPLACE_NODE + " and " + MESSAGETYPE.KILL_SELF);
            }
        }
    }

    @Override
    public void compute(Iterator<SimpleBubbleMergeMessage> msgIterator) {
        if (getSuperstep() == 1) {
            initVertex();
            detectBubble();
        } else if (getSuperstep() == 2) {
            processBubblesInMinorVertex(msgIterator);
        } else if (getSuperstep() == 3) {
            receiveUpdates(msgIterator);
        } else if (getSuperstep() == 4) {
            pruneDeadEdges(msgIterator);
        }
        voteToHalt();
    }

}
