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
import edu.uci.ics.genomix.data.types.Node.NeighborInfo;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.types.GraphMutations;

/**
 * Graph clean pattern: Simple Bubble Merge
 */
public class SimpleBubbleMergeVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, SimpleBubbleMergeMessage> {

    private static final Logger LOG = Logger.getLogger(SimpleBubbleMergeVertex.class.getName());

    private static float MAX_DISSIMILARITY = -1;
    private static int MAX_LENGTH = -1;
    private String logInfo = "";

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
        if (MAX_DISSIMILARITY < 0)
            MAX_DISSIMILARITY = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.BUBBLE_MERGE_MAX_DISSIMILARITY));
        if (MAX_LENGTH < 0)
            MAX_LENGTH = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.BUBBLE_MERGE_MAX_LENGTH));
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

    public void updateRelativeStats() {
        for (int i = 1; i < receivedMsgList.size(); i++) {
            VKmer bubble = receivedMsgList.get(i).getNode().getInternalKmer();

            // add 'pathLength' to statistics counter
            updateStats("pathLength", bubble.getKmerLetterLength());

            // add 'pathCoverage' to statistics counter
            updateStats("totalPathsCoverage", Math.round(receivedMsgList.get(i).getNode().getAverageCoverage()));

            for (int j = 0; j < receivedMsgList.size(); j++) {
                if (i == j)
                    continue;
                // add 'editDistance' to statistics counter
                float editDistance = receivedMsgList.get(i).editDistance(receivedMsgList.get(j));
                updateStats("editDistance", Math.round(editDistance));
                float fractionDissimilar = receivedMsgList.get(i).computeDissimilar(receivedMsgList.get(j));
                updateStats("fractionDissimilar", Math.round(fractionDissimilar));
            }
        }
    }

    public void logBubbleInfo() {
        boolean[] sameOrientationWithFirstBubble = new boolean[receivedMsgList.size()];

        // keep relative orientation with the first bubble
        sameOrientationWithFirstBubble[0] = true;
        for (int i = 1; i < receivedMsgList.size(); i++)
            sameOrientationWithFirstBubble[i] = receivedMsgList.get(i).sameOrientation(receivedMsgList.get(0));

        for (int i = 0; i < receivedMsgList.size(); i++) {
            // print in the same orientation
            VKmer bubble;
            if (sameOrientationWithFirstBubble[i])
                bubble = receivedMsgList.get(i).getNode().getInternalKmer();
            else
                bubble = receivedMsgList.get(i).getNode().getInternalKmer().reverse();

            // log bubble info
            logInfo += "\tNo." + (i + 1) + " bubble(internalKmer): " + bubble + "\tPathLength: "
                    + bubble.getKmerLetterLength() + "\tCoverage: "
                    + Math.round(receivedMsgList.get(i).getNode().getAverageCoverage()) + "\tID: "
                    + receivedMsgList.get(i).getSourceVertexId() + "\n";
            logInfo += "\t\t Dissimilar: ";

            for (int j = 0; j < receivedMsgList.size(); j++) {
                if (i == j)
                    continue;
                float fractionDissimilar = receivedMsgList.get(i).computeDissimilar(receivedMsgList.get(j));
                // log bubble info
                logInfo += fractionDissimilar + " with No." + (j + 1) + " bubble;  ";
            }
            logInfo += "\n";
        }
    }

    public void processSimilarSet() {
        while (!receivedMsgList.isEmpty()) {
            /* update stats */
            updateRelativeStats();

            /* log bubble info*/
            if (verbose)
                logBubbleInfo();

            /* detect kept bubble and removed bubble **/
            String logInfo_remove = "";
            int removedNum = 0;
            ArrayList<SimpleBubbleMergeMessage> originalMsgList = null;
            if (verbose)
                originalMsgList = new ArrayList<SimpleBubbleMergeMessage>(receivedMsgList);
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
                if (fractionDissimilar <= MAX_DISSIMILARITY) { //if similar with top node, delete this node
                    topChanged = true;
                    boolean sameOrientation = topMsg.sameOrientation(curMsg);
                    // 1. add coverage to top node -- for unchangedSet
                    topNode.addFromNode(!sameOrientation, curMsg.getNode());

                    // 2. send message to delete vertices -- for deletedSet
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(MESSAGETYPE.KILL_SELF.get());
                    sendMsg(curMsg.getSourceVertexId(), outgoingMsg);
                    // log bubble info
                    if (verbose) {
                        int i = 0;
                        for (; i < originalMsgList.size(); i++) {
                            if (originalMsgList.get(i).equals(curMsg))
                                break;
                        }
                        logInfo_remove += "No." + (i + 1) + " bubble - " + curMsg.getSourceVertexId() + "; ";
                    }

                    // add 'removedPathCoverage' to statistics counter
                    updateStats("removedPathsCoverage", Math.round(curMsg.getNode().getAverageCoverage()));

                    it.remove();
                    removedNum++;

                    getCounters().findCounter(GraphMutations.Num_RemovedBubbles).increment(1);
                }
            }
            // add 'removedPaths' to statistics counter
            updateStats("removedPaths", removedNum);

            // log bubble info
            if (verbose) {
                logInfo += "Remove " + removedNum + " bubbles: ";
                logInfo += logInfo_remove;
                logInfo += "\n/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// \n";
            }
            // process topNode -- send message to topVertex to update their coverage
            if (topChanged) {
                outgoingMsg.reset();
                outgoingMsg.setFlag(MESSAGETYPE.REPLACE_NODE.get());
                outgoingMsg.setNode(topNode);
                sendMsg(topMsg.getSourceVertexId(), outgoingMsg);
            }
        }
        if (verbose) {
            LOG.info(logInfo);
        }
    }

    /**
     * step 1
     */
    public void detectBubble() {
        VertexValueWritable vertex = getVertexValue();
        if (vertex.degree(DIR.REVERSE) == 1 && vertex.degree(DIR.FORWARD) == 1
                && vertex.getInternalKmer().getKmerLetterLength() < MAX_LENGTH) {
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
                // add to 'totalPaths' statistics counter
                updateStats("totalPaths", receivedMsgList.size());

                logInfo = "";
                if (verbose) {
                    logInfo += "\nMajor: " + majorVertexId + "  Minor: " + getVertexId() + "\n";
                    logInfo += "NumOfPaths: " + receivedMsgList.size() + "\n";
                }

                // for each majorVertex, sort the node by decreasing order of coverage
                Collections.sort(receivedMsgList, new SimpleBubbleMergeMessage.SortByCoverage());

                // process similarSet, keep the unchanged set and deleted set & add coverage to unchange node 
                processSimilarSet();
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
        initVertex();
        if (getSuperstep() == 1) {
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
