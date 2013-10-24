package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.BubbleMergeMessage;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag.MESSAGETYPE;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Node.NeighborInfo;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

/**
 * Graph clean pattern: Simple Bubble Merge
 */
public class SimpleBubbleMergeVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, BubbleMergeMessage> {
    
    private static final Logger LOG = Logger.getLogger(SimpleBubbleMergeVertex.class.getName());
    
    private float dissimilarThreshold = -1;

    private Map<VKmer, ArrayList<BubbleMergeMessage>> receivedMsgMap = new HashMap<VKmer, ArrayList<BubbleMergeMessage>>();
    private ArrayList<BubbleMergeMessage> receivedMsgList = new ArrayList<BubbleMergeMessage>();
    private BubbleMergeMessage topMsg = new BubbleMergeMessage();
    private BubbleMergeMessage curMsg = new BubbleMergeMessage();

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (dissimilarThreshold < 0)
            dissimilarThreshold = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.BUBBLE_MERGE_MAX_DISSIMILARITY));
        if (outgoingMsg == null)
            outgoingMsg = new BubbleMergeMessage();
        StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    public void sendBubbleAndMajorVertexMsgToMinorVertex() {
        VertexValueWritable vertex = getVertexValue();
        // SingleNeighbor: single neighbor in the given direction, return null if there are multiple or no neighbors
        NeighborInfo reverseNeighbor = vertex.getSingleNeighbor(DIR.REVERSE);
        NeighborInfo forwardNeighbor = vertex.getSingleNeighbor(DIR.FORWARD);

        // get majorVertex and minorVertex and meToMajorEdgeType and meToMinorEdgeType
        if (forwardNeighbor.kmer.equals(reverseNeighbor.kmer))
            throw new IllegalStateException("majorVertexId is equal to minorVertexId, this is not allowed!");

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
        if(verbose){
            LOG.info("Major vertex is: " + outgoingMsg.getMajorVertexId());
        }
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(minorVertexId, outgoingMsg);
    }

    public void aggregateBubbleNodesByMajorNode(Iterator<BubbleMergeMessage> msgIterator) {
        receivedMsgMap.clear();
        BubbleMergeMessage incomingMsg;
        ArrayList<BubbleMergeMessage> curMsgList;

        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if (!receivedMsgMap.containsKey(incomingMsg.getMajorVertexId())) {
                curMsgList = new ArrayList<BubbleMergeMessage>();
                receivedMsgMap.put(incomingMsg.getMajorVertexId(), curMsgList);
            } else {
                curMsgList = receivedMsgMap.get(incomingMsg.getMajorVertexId());
            }
            curMsgList.add(incomingMsg);
        }
    }

    public static boolean isValidMajorAndMinor(BubbleMergeMessage topMsg, BubbleMergeMessage curMsg) {
        EDGETYPE topMajorToBubbleEdgetype = topMsg.getMajorToBubbleEdgetype();
        EDGETYPE curMajorToBubbleEdgetype = curMsg.getMajorToBubbleEdgetype();
        EDGETYPE topMinorToBubbleEdgetype = topMsg.getMinorToBubbleEdgetype();
        EDGETYPE curMinorToBubbleEdgetype = curMsg.getMinorToBubbleEdgetype();
        return (topMajorToBubbleEdgetype.dir() == curMajorToBubbleEdgetype.dir())
                && topMinorToBubbleEdgetype.dir() == curMinorToBubbleEdgetype.dir();
    }

    public void addNewMinorToBubbleEdges(boolean sameOrientation, BubbleMergeMessage msg, 
            VKmer topKmer) {
        EdgeMap edgeMap = msg.getMinorToBubbleEdgeMap();
        ReadIdSet newReadIds = edgeMap.get(getVertexId());
        EDGETYPE minorToBubble = msg.getMinorToBubbleEdgetype();
        getVertexValue().getEdgeMap(sameOrientation ? minorToBubble : minorToBubble.flipNeighbor()).get(topKmer)
                .addAll(newReadIds);
    }

    public void processSimilarSet() {
        while (!receivedMsgList.isEmpty()) {
            Iterator<BubbleMergeMessage> it = receivedMsgList.iterator();
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

                if (fractionDissimilar <= dissimilarThreshold) { //if similar with top node, delete this node
                    topChanged = true;
                    boolean sameOrientation = topMsg.sameOrientation(curMsg);
                    // 1. add coverage to top node -- for unchangedSet
                    topNode.addFromNode(!sameOrientation, curMsg.getNode());

                    // 2. add curMsg.edge in minToBubbleEdgetype to minorVertex
                    addNewMinorToBubbleEdges(sameOrientation, curMsg, topMsg.getSourceVertexId());

                    // 3. send message to add curMsg.edge in majorToBubbleEdgetype to majorVertex
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(MESSAGETYPE.ADD_READIDS.get());
                    outgoingMsg.addNewMajorToBubbleEdges(sameOrientation, curMsg, topMsg.getSourceVertexId());
                    sendMsg(curMsg.getMajorVertexId(), outgoingMsg);

                    // 4. send message to delete vertices -- for deletedSet
                    outgoingMsg.reset();
                    outgoingMsg.setFlag(MESSAGETYPE.KILL_SELF.get());
                    sendMsg(curMsg.getSourceVertexId(), outgoingMsg);
                    it.remove();
                }
            }
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
    public void processBubblesInMinorVertex(Iterator<BubbleMergeMessage> msgIterator) {
        // aggregate bubble nodes and grouped by major vertex
        aggregateBubbleNodesByMajorNode(msgIterator);

        for (VKmer majorVertexId : receivedMsgMap.keySet()) {
            receivedMsgList = receivedMsgMap.get(majorVertexId);
            if (receivedMsgList.size() > 1) { // filter simple paths
                // for each majorVertex, sort the node by decreasing order of coverage
                Collections.sort(receivedMsgList, new BubbleMergeMessage.SortByCoverage());

                // process similarSet, keep the unchanged set and deleted set & add coverage to unchange node 
                processSimilarSet();
            }
        }
    }

    /**
     * step 3
     */
    public void receiveUpdates(Iterator<BubbleMergeMessage> msgIterator) {
        while (msgIterator.hasNext()) {
            BubbleMergeMessage incomingMsg = msgIterator.next();
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
                case ADD_READIDS:
                    for (EDGETYPE et : EDGETYPE.values()) {
                        EdgeMap edgeMap = incomingMsg.getNode().getEdgeMap(et);
                        if (edgeMap.size() > 0){
                            getVertexValue().getEdgeMap(et).unionUpdate(edgeMap);
                            activate();
                            break;
                        }
                    }
                    break;
                default:
                    throw new IllegalStateException("The received message types should have only two kinds: "
                            + MESSAGETYPE.REPLACE_NODE + " and " + MESSAGETYPE.KILL_SELF);
            }
        }
    }

    @Override
    public void compute(Iterator<BubbleMergeMessage> msgIterator) {
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

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, SimpleBubbleMergeVertex.class));
    }
}
