package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.BubbleMergeMessage;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag.MESSAGETYPE;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

/**
 * Graph clean pattern: Bubble Merge
 */
public class ComplexBubbleMergeVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, BubbleMergeMessage> {
    private float dissimilarThreshold = -1;

    private Map<VKmer, ArrayList<BubbleMergeMessage>> receivedMsgMap = new HashMap<VKmer, ArrayList<BubbleMergeMessage>>();
    private ArrayList<BubbleMergeMessage> receivedMsgList = new ArrayList<BubbleMergeMessage>();
    private BubbleMergeMessage topMsg = new BubbleMergeMessage();
    private BubbleMergeMessage curMsg = new BubbleMergeMessage();
    //    private Set<BubbleMergeMessageWritable> unchangedSet = new HashSet<BubbleMergeMessageWritable>();
    //    private Set<BubbleMergeMessageWritable> deletedSet = new HashSet<BubbleMergeMessageWritable>();

    //    private static Set<BubbleMergeMessageWritable> allDeletedSet = Collections.synchronizedSet(new HashSet<BubbleMergeMessageWritable>());
    private static Set<VKmer> allDeletedSet = Collections.synchronizedSet(new HashSet<VKmer>());

    private VKmerList incomingEdges = null;
    private VKmerList outgoingEdges = null;
    private EDGETYPE incomingEdgeType;
    private EDGETYPE outgoingEdgeType;

    //    private VKmerBytesWritable incomingKmer = new VKmerBytesWritable();
    //    private VKmerBytesWritable outgoingKmer = new VKmerBytesWritable();
    //    private VKmerBytesWritable majorVertexId = new VKmerBytesWritable();
    //    private VKmerBytesWritable minorVertexId = new VKmerBytesWritable();

    public void setEdgesAndEdgeType(int i) {
        incomingEdges.setAsCopy(getVertexValue().getEdges(validPathsTable[i][0]));
        incomingEdgeType = validPathsTable[i][0];

        outgoingEdges.setAsCopy(getVertexValue().getEdges(validPathsTable[i][1]));
        outgoingEdgeType = validPathsTable[i][1];
    }

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (dissimilarThreshold == -1)
            dissimilarThreshold = Float.parseFloat(getContext().getConfiguration().get(
                    GenomixJobConf.BUBBLE_MERGE_MAX_DISSIMILARITY));
        if (outgoingMsg == null)
            outgoingMsg = new BubbleMergeMessage();
        else
            outgoingMsg.reset();
        if (incomingEdges == null)
            incomingEdges = new VKmerList();
        if (outgoingEdges == null)
            outgoingEdges = new VKmerList();
        outFlag = 0;
        if (fakeVertex == null) {
            fakeVertex = new VKmer();
            String random = generaterRandomDNAString(kmerSize + 1);
            fakeVertex.setFromStringBytes(kmerSize + 1, random.getBytes(), 0);
        }
    }

    public void sendBubbleAndMajorVertexMsgToMinorVertex() {
        for (int i = 0; i < 4; i++) {
            // set edgeList and edgeDir based on connectedTable 
            setEdgesAndEdgeType(i);

            for (VKmer incomingKmer : incomingEdges) {
                for (VKmer outgoingKmer : outgoingEdges) {
                    // get majorVertex and minorVertex and meToMajorDir and meToMinorDir
                    VKmer majorVertexId = null;
                    EDGETYPE majorToMeEdgetype = null;
                    EDGETYPE minorToMeEdgetype = null;
                    VKmer minorVertexId = null;
                    if (incomingKmer.compareTo(outgoingKmer) >= 0) {
                        majorVertexId = incomingKmer;
                        majorToMeEdgetype = incomingEdgeType;
                        minorVertexId = outgoingKmer;
                        minorToMeEdgetype = outgoingEdgeType;
                    } else {
                        majorVertexId = outgoingKmer;
                        majorToMeEdgetype = outgoingEdgeType;
                        minorVertexId = incomingKmer;
                        minorToMeEdgetype = incomingEdgeType;
                    }
                    if (majorVertexId == minorVertexId)
                        throw new IllegalArgumentException(
                                "majorVertexId is equal to minorVertexId, this is not allowd!");
                    EDGETYPE meToMajorEdgetype = majorToMeEdgetype.mirror();
                    EDGETYPE meToMinorEdgetype = minorToMeEdgetype.mirror();

                    // setup outgoingMsg
                    outgoingMsg.setMajorVertexId(majorVertexId);
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setNode(getVertexValue());
                    outgoingMsg.setMajorToBubbleEdgetype(meToMajorEdgetype);
                    outgoingMsg.setMinorToBubbleEdgetype(meToMinorEdgetype);
                    sendMsg(minorVertexId, outgoingMsg);
                }
            }
        }
    }

    @SuppressWarnings({ "unchecked" })
    public void aggregateBubbleNodesByMajorNode(Iterator<BubbleMergeMessage> msgIterator) {
        while (msgIterator.hasNext()) {
            BubbleMergeMessage incomingMsg = msgIterator.next();
            if (!receivedMsgMap.containsKey(incomingMsg.getMajorVertexId())) {
                receivedMsgList.clear();
                receivedMsgList.add(incomingMsg);
                receivedMsgMap.put(incomingMsg.getMajorVertexId(),
                        (ArrayList<BubbleMergeMessage>) receivedMsgList.clone());
            } else {
                receivedMsgList.clear();
                receivedMsgList.addAll(receivedMsgMap.get(incomingMsg.getMajorVertexId()));
                receivedMsgList.add(incomingMsg);
                receivedMsgMap.put(incomingMsg.getMajorVertexId(),
                        (ArrayList<BubbleMergeMessage>) receivedMsgList.clone());
            }
        }
    }

    public boolean isValidMajorAndMinor() {
        EDGETYPE topMajorToBubbleEdgetype = topMsg.getMajorToBubbleEdgetype();
        EDGETYPE curMajorToBubbleEdgetype = curMsg.getMajorToBubbleEdgetype();
        EDGETYPE topMinorToBubbleEdgetype = topMsg.getMinorToBubbleEdgetype();
        EDGETYPE curMinorToBubbleEdgetype = curMsg.getMinorToBubbleEdgetype();
        return (topMajorToBubbleEdgetype.dir() == curMajorToBubbleEdgetype.dir())
                && topMinorToBubbleEdgetype.dir() == curMinorToBubbleEdgetype.dir();
    }

    public void processSimilarSetToUnchangeSetAndDeletedSet() {
        topMsg.reset();
        curMsg.reset();
        while (!receivedMsgList.isEmpty()) {
            Iterator<BubbleMergeMessage> it = receivedMsgList.iterator();
            topMsg.set(it.next());
            it.remove(); //delete topCoverage node
            Node topNode = topMsg.getNode();
            while (it.hasNext()) {
                curMsg.set(it.next());
                //check if the vertex is valid minor and if it comes from valid major
                if (!isValidMajorAndMinor())
                    continue;

                //compute the similarity
                float fracDissimilar = topMsg.computeDissimilar(curMsg);

                if (fracDissimilar < dissimilarThreshold) { //if similar with top node, delete this node and put it in deletedSet
                    // 1. update my own(minor's) edges
                    EDGETYPE MinorToBubble = curMsg.getMinorToBubbleEdgetype();
                    getVertexValue().getEdges(MinorToBubble).remove(curMsg.getSourceVertexId());
                    activate();

                    // 2. add coverage to top node -- for unchangedSet
                    boolean sameOrientation = topMsg.sameOrientation(curMsg);
                    topNode.addFromNode(sameOrientation, curMsg.getNode());

                    // 3. treat msg as a bubble vertex, broadcast kill self message to major vertex to update their edges
                    EDGETYPE majorToBubble = curMsg.getMajorToBubbleEdgetype();
                    EDGETYPE bubbleToMajor = majorToBubble.mirror();
                    outgoingMsg.reset();
                    outFlag = 0;
                    outFlag |= bubbleToMajor.get() | MESSAGETYPE.UPDATE.get();
                    outgoingMsg.setFlag(outFlag);
                    sendMsg(curMsg.getMajorVertexId(), outgoingMsg);
                    //                    boolean flip = curMsg.isFlip(topCoverageVertexMsg);
                    //                    curMsg.setFlip(flip);
                    //                    curMsg.setTopCoverageVertexId(topCoverageVertexMsg.getSourceVertexId());
                    //                    curMsg.setMinorVertexId(getVertexId());
                    //                    deletedSet.add(new BubbleMergeMessageWritable(curMsg));

                    // 4. store deleted vertices -- for deletedSet
                    allDeletedSet.add(new VKmer(curMsg.getSourceVertexId()));
                    it.remove();
                }
            }

            //process unchangedSet -- send message to topVertex to update their coverage
            outgoingMsg.reset();
            outFlag = 0;
            outFlag |= MESSAGETYPE.REPLACE_NODE.get();
            outgoingMsg.setNode(topNode);
            outgoingMsg.setFlag(outFlag);
            sendMsg(topMsg.getSourceVertexId(), outgoingMsg);
            //            unchangedSet.add(new BubbleMergeMessageWritable(topCoverageVertexMsg));
        }
    }

    //    public void processUnchangedSet(){
    //        for(BubbleMergeMessageWritable msg : unchangedSet){
    //            outFlag = MessageFlag.UNCHANGE;
    //            outgoingMsg.setFlag(outFlag);
    //            outgoingMsg.setNode(msg.getNode());
    //            sendMsg(msg.getSourceVertexId(), outgoingMsg);
    //        }
    //    }
    //    
    //    public void processDeletedSet(){
    //        for(BubbleMergeMessageWritable msg : deletedSet){
    //            outgoingMsg.set(msg);
    //            outFlag = MessageFlag.KILL;
    //            outgoingMsg.setFlag(outFlag);
    //            outgoingMsg.setSourceVertexId(msg.getMinorVertexId());
    //            sendMsg(msg.getSourceVertexId(), outgoingMsg);
    //        }
    //    }

    //    public void processAllDeletedSet(){
    //        synchronized(allDeletedSet){
    //            for(BubbleMergeMessageWritable msg : allDeletedSet){
    //                outgoingMsg.set(msg);
    //                outFlag = MessageFlag.KILL;
    //                outgoingMsg.setFlag(outFlag);
    //                outgoingMsg.setSourceVertexId(msg.getMinorVertexId());
    //                sendMsg(msg.getSourceVertexId(), outgoingMsg);
    //            }
    //        }
    //    }

    //    public void removeEdgesToMajorAndMinor(BubbleMergeMessageWritable incomingMsg){
    //        EDGETYPE meToMajorDir = EDGETYPE.fromByte(incomingMsg.getMeToMajorEdgetype());
    //        EDGETYPE majorToMeDir = meToMajorDir.mirror();
    //        EDGETYPE meToMinorDir = EDGETYPE.fromByte(incomingMsg.getMeToMinorEdgetype());
    //        EDGETYPE minorToMeDir = meToMinorDir.mirror();
    //        getVertexValue().getEdgeMap(majorToMeDir).remove(incomingMsg.getMajorVertexId());
    //        getVertexValue().getEdgeMap(minorToMeDir).remove(incomingMsg.getSourceVertexId());
    //    }

    public void broadcaseUpdateEdges(BubbleMergeMessage incomingMsg) {
        outFlag = 0;
        outFlag |= MESSAGETYPE.KILL_SELF.get() | MESSAGETYPE.FROM_DEAD.get();

        outgoingMsg.setTopCoverageVertexId(incomingMsg.getTopCoverageVertexId());
        // TODO remove? Clean up!!!
        //        outgoingMsg.setFlip(incomingMsg.isFlip());
        //        sendSettledMsgToAllNeighborNodes(getVertexValue());
    }

    /**
     * broadcast kill self to all neighbors and send message to update neighbor's edges ***
     */
    public void broadcaseKillselfAndNoticeToUpdateEdges(BubbleMergeMessage incomingMsg) {
        outFlag = 0;
        outFlag |= MESSAGETYPE.KILL_SELF.get() | MESSAGETYPE.FROM_DEAD.get();

        outgoingMsg.setTopCoverageVertexId(incomingMsg.getTopCoverageVertexId());
        //        outgoingMsg.setFlip(incomingMsg.isFlip());
        //        sendSettledMsgToAllNeighborNodes(getVertexValue());

        deleteVertex(getVertexId());
    }

    /**
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertexAndUpdateEdges(BubbleMergeMessage incomingMsg) {
        VertexValueWritable vertex = getVertexValue();

        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();

        if (vertex.getEdges(neighborToMeDir).contains(incomingMsg.getSourceVertexId())) {
            vertex.getEdges(neighborToMeDir).remove(incomingMsg.getSourceVertexId());
        } else {
            throw new IllegalStateException("Tried to remove an edge that doesn't exist! I am " + vertex
                    + " incomingMsg is " + incomingMsg);
        }
        //        EDGETYPE updateDir = incomingMsg.isFlip() ? neighborToMeDir.flipNeighbor() : neighborToMeDir;
        //        getVertexValue().getEdgeMap(updateDir).unionAdd(incomingMsg.getTopCoverageVertexId(), readIds);
    }

    @Override
    public void compute(Iterator<BubbleMergeMessage> msgIterator) throws IOException {
        initVertex();
        if (getSuperstep() == 1) {
            if (VertexUtil.isBubbleVertex(getVertexValue())) {
                // clean allDeleteSet 
                allDeletedSet.clear();
                //                // add a fake node
                //                addFakeVertex();
                // send bubble and major vertex msg to minor vertex 
                sendBubbleAndMajorVertexMsgToMinorVertex();
            }
        } else if (getSuperstep() == 2) {
            if (!getVertexValue().isFakeVertex()) {
                // aggregate bubble nodes and grouped by major vertex
                aggregateBubbleNodesByMajorNode(msgIterator);

                for (VKmer majorVertexId : receivedMsgMap.keySet()) {
                    receivedMsgList.clear();
                    receivedMsgList = receivedMsgMap.get(majorVertexId);
                    //                    receivedMsgList.addAll(receivedMsgMap.get(majorVertexId));
                    if (receivedMsgList.size() > 1) { // filter bubble
                        // for each majorVertex, sort the node by decreasing order of coverage
                        Collections.sort(receivedMsgList, new BubbleMergeMessage.SortByCoverage());

                        // process similarSet, keep the unchanged set and deleted set & add coverage to unchange node 
                        processSimilarSetToUnchangeSetAndDeletedSet();

                        //                        // send message to the unchanged set for updating coverage & send kill message to the deleted set
                        //                        processUnchangedSet();
                        //                        processDeletedSet();
                    }
                }
            }
        } else if (getSuperstep() == 3) {
            if (allDeletedSet.contains(getVertexId()))
                deleteVertex(getVertexId());
            else {
                while (msgIterator.hasNext()) {
                    BubbleMergeMessage incomingMsg = msgIterator.next();
                    MESSAGETYPE msgType = MESSAGETYPE.fromByte(incomingMsg.getFlag());
                    switch (msgType) {
                        case UPDATE:
                            break;
                        case REPLACE_NODE:
                            break;
                    }
                }
            }

            //            if(!isFakeVertex()){
            //                while(msgIterator.hasNext()) {
            //                    incomingMsg = msgIterator.next();
            //                    if(incomingMsg.getFlag() == MessageFlag.KILL){
            //                        broadcaseUpdateEdges();
            //                    } else 
            //                    if(incomingMsg.getFlag() == MessageFlag.UNCHANGE){
            //                        // update Node including average coverage 
            //                        getVertexValue().setNode(incomingMsg.getNode());
            //                    }
            //                }
            //            }
        }
        //        else if(getSuperstep() == 4){
        //            if(!isFakeVertex()){
        //                while(msgIterator.hasNext()) {
        //                    incomingMsg = msgIterator.next();
        //                    if(isResponseKillMsg()){
        //                        responseToDeadVertexAndUpdateEdges();
        //                    }
        //                }
        //            }else{
        //                processAllDeletedSet();
        //                deleteVertex(getVertexId());
        //            }
        //        } else if(getSuperstep() == 5){
        //            while(msgIterator.hasNext()) {
        //                incomingMsg = msgIterator.next();
        //                if(incomingMsg.getFlag() == MessageFlag.KILL){
        //                    broadcaseKillselfAndNoticeToUpdateEdges();
        //                    //set statistics counter: Num_RemovedBubbles
        //                    updateStatisticsCounter(StatisticsCounter.Num_RemovedBubbles);
        //                    getVertexValue().setCounters(counters);
        //                }
        //            }
        //        } else if(getSuperstep() == 6){
        //            while(msgIterator.hasNext()) {
        //                incomingMsg = msgIterator.next();
        //                if(isResponseKillMsg()){
        //                    responseToDeadVertexAndUpdateEdges();
        //                }
        //            }
        //        }
        //        if(!isFakeVertex())
        //            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, ComplexBubbleMergeVertex.class));
    }
}
