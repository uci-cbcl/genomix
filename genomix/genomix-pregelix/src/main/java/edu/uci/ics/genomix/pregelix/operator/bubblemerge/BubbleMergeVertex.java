package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.BubbleMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/**
 * Graph clean pattern: Bubble Merge
 * @author anbangx
 *
 */
public class BubbleMergeVertex extends
    BasicGraphCleanVertex<VertexValueWritable, BubbleMergeMessageWritable> {
    private float dissimilarThreshold = -1;
    
    public static class EdgeType{
        public static final byte INCOMINGEDGE = 0b1 << 0;
        public static final byte OUTGOINGEDGE = 0b1 << 1;
    }
    
    private Map<VKmerBytesWritable, ArrayList<BubbleMergeMessageWritable>> receivedMsgMap = new HashMap<VKmerBytesWritable, ArrayList<BubbleMergeMessageWritable>>();
    private ArrayList<BubbleMergeMessageWritable> receivedMsgList = new ArrayList<BubbleMergeMessageWritable>();
    private BubbleMergeMessageWritable topMsg = new BubbleMergeMessageWritable();
    private BubbleMergeMessageWritable curMsg = new BubbleMergeMessageWritable();
//    private Set<BubbleMergeMessageWritable> unchangedSet = new HashSet<BubbleMergeMessageWritable>();
//    private Set<BubbleMergeMessageWritable> deletedSet = new HashSet<BubbleMergeMessageWritable>();
    private EdgeWritable tmpEdge = new EdgeWritable();
    
//    private static Set<BubbleMergeMessageWritable> allDeletedSet = Collections.synchronizedSet(new HashSet<BubbleMergeMessageWritable>());
    private static Set<VKmerBytesWritable> allDeletedSet = Collections.synchronizedSet(new HashSet<VKmerBytesWritable>());
    
//    private VKmerBytesWritable incomingKmer = new VKmerBytesWritable();
//    private VKmerBytesWritable outgoingKmer = new VKmerBytesWritable();
//    private VKmerBytesWritable majorVertexId = new VKmerBytesWritable();
//    private VKmerBytesWritable minorVertexId = new VKmerBytesWritable();
    
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(dissimilarThreshold == -1)
            dissimilarThreshold = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.BUBBLE_MERGE_MAX_DISSIMILARITY));
        if(incomingMsg == null)
            incomingMsg = new BubbleMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new BubbleMergeMessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(incomingEdgeList == null)
            incomingEdgeList = new EdgeListWritable();
        if(outgoingEdgeList == null)
            outgoingEdgeList = new EdgeListWritable();
        outFlag = 0;
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    public void sendBubbleAndMajorVertexMsgToMinorVertex(){
        for(int i = 0; i < 4; i++){
            // set edgeList and edgeDir based on connectedTable 
            setEdgeListAndEdgeType(i);
            
            for(EdgeWritable incomingEdge : incomingEdgeList){
                for(EdgeWritable outgoingEdge : outgoingEdgeList){
                    // get majorVertex and minorVertex and meToMajorDir and meToMinorDir
                    VKmerBytesWritable incomingKmer = incomingEdge.getKey();
                    VKmerBytesWritable outgoingKmer = outgoingEdge.getKey();
                    VKmerBytesWritable majorVertexId = null;
                    EDGETYPE majorToMeEdgetype = null; 
                    EDGETYPE minorToMeEdgetype = null; 
                    VKmerBytesWritable minorVertexId = null;
                    if(incomingKmer.compareTo(outgoingKmer) >= 0){
                        majorVertexId = incomingKmer;
                        majorToMeEdgetype = incomingEdgeType;
                        minorVertexId = outgoingKmer;
                        minorToMeEdgetype = outgoingEdgeType;
                    } else{
                        majorVertexId = outgoingKmer;
                        majorToMeEdgetype = outgoingEdgeType;
                        minorVertexId = incomingKmer;
                        minorToMeEdgetype = incomingEdgeType;
                    }
                    if(majorVertexId == minorVertexId)
                        throw new IllegalArgumentException("majorVertexId is equal to minorVertexId, this is not allowd!");
                    EDGETYPE meToMajorEdgetype = majorToMeEdgetype.mirror();
                    EDGETYPE meToMinorEdgetype = minorToMeEdgetype.mirror();

                    // setup outgoingMsg
                    outgoingMsg.setMajorVertexId(majorVertexId);
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setNode(getVertexValue().getNode());
                    outgoingMsg.setMeToMajorEdgetype(meToMajorEdgetype.get());
                    outgoingMsg.setMeToMinorEdgetype(meToMinorEdgetype.get());
                    sendMsg(minorVertexId, outgoingMsg);
                }
            }
        }
    }
    
    @SuppressWarnings({ "unchecked" })
    public void aggregateBubbleNodesByMajorNode(Iterator<BubbleMergeMessageWritable> msgIterator){
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(!receivedMsgMap.containsKey(incomingMsg.getMajorVertexId())){
                receivedMsgList.clear();
                receivedMsgList.add(incomingMsg);
                receivedMsgMap.put(incomingMsg.getMajorVertexId(), (ArrayList<BubbleMergeMessageWritable>)receivedMsgList.clone());
            }
            else{
                receivedMsgList.clear();
                receivedMsgList.addAll(receivedMsgMap.get(incomingMsg.getMajorVertexId()));
                receivedMsgList.add(incomingMsg);
                receivedMsgMap.put(incomingMsg.getMajorVertexId(), (ArrayList<BubbleMergeMessageWritable>)receivedMsgList.clone());
            }
        }
    }
    
    public boolean isValidMajorAndMinor(){
        EDGETYPE topBubbleToMajorEdgetype = EDGETYPE.fromByte(topMsg.getMeToMajorEdgetype());
        EDGETYPE curBubbleToMajorEdgetype = EDGETYPE.fromByte(curMsg.getMeToMajorEdgetype());
        EDGETYPE topBubbleToMinorEdgetype = EDGETYPE.fromByte(topMsg.getMeToMinorEdgetype());
        EDGETYPE curBubbleToMinorEdgetype = EDGETYPE.fromByte(curMsg.getMeToMinorEdgetype());
        return (topBubbleToMajorEdgetype.dir() == curBubbleToMajorEdgetype.dir()) && topBubbleToMinorEdgetype.dir() == curBubbleToMinorEdgetype.dir();
    }
    
    public boolean isFlipRelativeToMajor(BubbleMergeMessageWritable msg1, BubbleMergeMessageWritable msg2){
        return msg1.getRelativeDirToMajor() != msg2.getRelativeDirToMajor();
    }
    
    public void processSimilarSetToUnchangeSetAndDeletedSet(){
//        unchangedSet.clear();
//        deletedSet.clear();
        topMsg.reset();
        curMsg.reset();
        while(!receivedMsgList.isEmpty()){
            Iterator<BubbleMergeMessageWritable> it = receivedMsgList.iterator();
            topMsg.set(it.next());
            it.remove(); //delete topCoverage node
            NodeWritable topNode = topMsg.getNode();
            while(it.hasNext()){
                curMsg.set(it.next());
                //check if the vertex is valid minor and if it comes from valid major
                if(!isValidMajorAndMinor())
                    continue;
                
                //compute the similarity
                float fracDissimilar = topMsg.computeDissimilar(curMsg);
                
                if(fracDissimilar < dissimilarThreshold){ //if similar with top node, delete this node and put it in deletedSet
                    // 1. update my own(minor's) edges
                    EDGETYPE bubbleToMinor = EDGETYPE.fromByte(curMsg.getMeToMinorEdgetype());
                    getVertexValue().getEdgeList(bubbleToMinor).remove(curMsg.getSourceVertexId());
                    activate();
                    
                    // 2. add coverage to top node -- for unchangedSet
                    topNode.addFromNode(isFlipRelativeToMajor(topMsg, curMsg), 
                            curMsg.getNode()); 
                    
                    // 3. treat msg as a bubble vertex, broadcast kill self message to major vertex to update their edges
                    EDGETYPE bubbleToMajor = EDGETYPE.fromByte(curMsg.getMeToMajorEdgetype());
                    EDGETYPE majorToBubble = bubbleToMajor.mirror();
                    outgoingMsg.reset();
                    outFlag = 0;
                    outFlag |= majorToBubble.get() | MessageFlag.UPDATE;
                    outgoingMsg.setFlag(outFlag);
                    sendMsg(curMsg.getMajorVertexId(), outgoingMsg);
//                    boolean flip = curMsg.isFlip(topCoverageVertexMsg);
//                    curMsg.setFlip(flip);
//                    curMsg.setTopCoverageVertexId(topCoverageVertexMsg.getSourceVertexId());
//                    curMsg.setMinorVertexId(getVertexId());
//                    deletedSet.add(new BubbleMergeMessageWritable(curMsg));
                    
                    // 4. store deleted vertices -- for deletedSet
                    allDeletedSet.add(new VKmerBytesWritable(curMsg.getSourceVertexId()));
                    it.remove();
                }
            }
            
            //process unchangedSet -- send message to topVertex to update their coverage
            outgoingMsg.reset();
            outFlag = 0;
            outFlag |= MessageFlag.UNCHANGE;
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
    
    public void removeEdgesToMajorAndMinor(){
        EDGETYPE meToMajorDir = EDGETYPE.fromByte(incomingMsg.getMeToMajorEdgetype());
        EDGETYPE majorToMeDir = meToMajorDir.mirror();
        EDGETYPE meToMinorDir = EDGETYPE.fromByte(incomingMsg.getMeToMinorEdgetype());
        EDGETYPE minorToMeDir = meToMinorDir.mirror();
        getVertexValue().getEdgeList(majorToMeDir).remove(incomingMsg.getMajorVertexId());
        getVertexValue().getEdgeList(minorToMeDir).remove(incomingMsg.getSourceVertexId());
    }
    
    public void broadcaseUpdateEdges(){
        outFlag = 0;
        outFlag |= MessageFlag.KILL;
        outFlag |= MessageFlag.DIR_FROM_DEADVERTEX;
        
        outgoingMsg.setTopCoverageVertexId(incomingMsg.getTopCoverageVertexId());
        outgoingMsg.setFlip(incomingMsg.isFlip());
        sendSettledMsgToAllNeighborNodes(getVertexValue());
    }
    
    /**
     * broadcast kill self to all neighbors and send message to update neighbor's edges ***
     */
    public void broadcaseKillselfAndNoticeToUpdateEdges(){
        outFlag = 0;
        outFlag |= MessageFlag.KILL;
        outFlag |= MessageFlag.DIR_FROM_DEADVERTEX;
        
        outgoingMsg.setTopCoverageVertexId(incomingMsg.getTopCoverageVertexId());
        outgoingMsg.setFlip(incomingMsg.isFlip());
        sendSettledMsgToAllNeighborNodes(getVertexValue());
        
        deleteVertex(getVertexId());
    }
    
    /**
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertexAndUpdateEdges(){
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        
        if(getVertexValue().getEdgeList(neighborToMeDir).getEdge(incomingMsg.getSourceVertexId()) != null){
            tmpEdge.setAsCopy(getVertexValue().getEdgeList(neighborToMeDir).getEdge(incomingMsg.getSourceVertexId()));
            
            getVertexValue().getEdgeList(neighborToMeDir).remove(incomingMsg.getSourceVertexId());
        }
        tmpEdge.setKey(incomingMsg.getTopCoverageVertexId());
        EDGETYPE updateDir = incomingMsg.isFlip() ? neighborToMeDir.flip() : neighborToMeDir;
        getVertexValue().getEdgeList(updateDir).unionAdd(tmpEdge);
    }
    
    @Override
    public void compute(Iterator<BubbleMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if(VertexUtil.isBubbleVertex(getVertexValue())){
                // clean allDeleteSet 
                allDeletedSet.clear();
//                // add a fake node
//                addFakeVertex();
                // send bubble and major vertex msg to minor vertex 
                sendBubbleAndMajorVertexMsgToMinorVertex();
            }
        } else if (getSuperstep() == 2){
            if(!isFakeVertex()){
                // aggregate bubble nodes and grouped by major vertex
                aggregateBubbleNodesByMajorNode(msgIterator);
                
                for(VKmerBytesWritable majorVertexId : receivedMsgMap.keySet()){
                    receivedMsgList.clear();
                    receivedMsgList = receivedMsgMap.get(majorVertexId);
//                    receivedMsgList.addAll(receivedMsgMap.get(majorVertexId));
                    if(receivedMsgList.size() > 1){ // filter bubble
                        // for each majorVertex, sort the node by decreasing order of coverage
                        Collections.sort(receivedMsgList, new BubbleMergeMessageWritable.SortByCoverage());
                        
                        // process similarSet, keep the unchanged set and deleted set & add coverage to unchange node 
                        processSimilarSetToUnchangeSetAndDeletedSet();
                        
//                        // send message to the unchanged set for updating coverage & send kill message to the deleted set
//                        processUnchangedSet();
//                        processDeletedSet();
                    }
                }
            }
        } else if (getSuperstep() == 3){
            if(allDeletedSet.contains(getVertexId()))
                deleteVertex(getVertexId());
            else{
                while(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    short msgType = (short) (incomingMsg.getFlag() & MessageFlag.MSG_TYPE_MASK);
                    switch(msgType){
                        case MessageFlag.UPDATE:
                            break;
                        case MessageFlag.UNCHANGE:
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
        Client.run(args, getConfiguredJob(null, BubbleMergeVertex.class));
    }
}
