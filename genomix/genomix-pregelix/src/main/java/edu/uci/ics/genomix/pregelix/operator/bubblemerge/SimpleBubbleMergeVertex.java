package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

/**
 * Graph clean pattern: Simple Bubble Merge
 * @author anbangx
 *
 */
public class SimpleBubbleMergeVertex extends
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
    
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(dissimilarThreshold == -1)
            dissimilarThreshold = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.BUBBLE_MERGE_MAX_DISSIMILARITY));
        if(outgoingMsg == null)
            outgoingMsg = new BubbleMergeMessageWritable();
        else
            outgoingMsg.reset();
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
            BubbleMergeMessageWritable incomingMsg = msgIterator.next();
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
    
    public void processSimilarSet(){
        topMsg.reset();
        curMsg.reset();
        while(!receivedMsgList.isEmpty()){
            Iterator<BubbleMergeMessageWritable> it = receivedMsgList.iterator();
            topMsg = it.next();
            it.remove(); //delete topCoverage node
            NodeWritable topNode = topMsg.getNode();
            while(it.hasNext()){
                curMsg = it.next();
                //check if the vertex is valid minor and if it comes from valid major
                if(!isValidMajorAndMinor())
                    continue;
                
                //compute the similarity
                float fracDissimilar = topMsg.computeDissimilar(curMsg);
                
                if(fracDissimilar < dissimilarThreshold){ //if similar with top node, delete this node and put it in deletedSet
                    // 1. add coverage to top node -- for unchangedSet
                    topNode.addFromNode(isFlipRelativeToMajor(topMsg, curMsg), 
                            curMsg.getNode()); 
                    
                    // 2. send message to delete vertices -- for deletedSet
                    outgoingMsg.reset();
                    outFlag = 0;
                    outFlag |= MessageFlag.KILL;
                    outgoingMsg.setFlag(outFlag);
                    sendMsg(curMsg.getSourceVertexId(), outgoingMsg);
                    it.remove();
                    
//                    // 1. update my own(minor's) edges
//                    EDGETYPE bubbleToMinor = EDGETYPE.fromByte(curMsg.getMeToMinorEdgetype());
//                    getVertexValue().getEdgeList(bubbleToMinor).remove(curMsg.getSourceVertexId());
//                    activate();
//                    
//                    // 2. add coverage to top node -- for unchangedSet
//                    topNode.addFromNode(isFlipRelativeToMajor(topMsg, curMsg), 
//                            curMsg.getNode()); 
//                    
//                    // 3. treat msg as a bubble vertex, broadcast kill self message to major vertex to update their edges
//                    EDGETYPE bubbleToMajor = EDGETYPE.fromByte(curMsg.getMeToMajorEdgetype());
//                    EDGETYPE majorToBubble = bubbleToMajor.mirror();
//                    outgoingMsg.reset();
//                    outFlag = 0;
//                    outFlag |= majorToBubble.get() | MessageFlag.UPDATE;
//                    outgoingMsg.setFlag(outFlag);
//                    sendMsg(curMsg.getMajorVertexId(), outgoingMsg);
//                    
//                    // 4. send message to delete vertices -- for deletedSet
//                    outgoingMsg.reset();
//                    outFlag = 0;
//                    outFlag |= MessageFlag.KILL;
//                    outgoingMsg.setFlag(outFlag);
//                    sendMsg(curMsg.getSourceVertexId(), outgoingMsg);
//                    it.remove();
                }
            }
            // process unchangedSet -- send message to topVertex to update their coverage
            outgoingMsg.reset();
            outFlag = 0;
            outFlag |= MessageFlag.UNCHANGE;
            outgoingMsg.setNode(topNode);
            outgoingMsg.setFlag(outFlag);
            sendMsg(topMsg.getSourceVertexId(), outgoingMsg);
        }
    }
    
    /**
     * step 1
     */
    public void detectBubble(){
        VertexValueWritable vertex = getVertexValue();
        if(vertex.getDegree(DIR.PREVIOUS) == 1 && vertex.getDegree(DIR.NEXT) == 1){
            // send bubble and major vertex msg to minor vertex 
            sendBubbleAndMajorVertexMsgToMinorVertex();
        }
    }
    
    /**
     * step 2
     */
    public void processBubblesInMinorVertex(Iterator<BubbleMergeMessageWritable> msgIterator){
     // aggregate bubble nodes and grouped by major vertex
        aggregateBubbleNodesByMajorNode(msgIterator);
        
        for(VKmerBytesWritable majorVertexId : receivedMsgMap.keySet()){
            receivedMsgList.clear();
            receivedMsgList = receivedMsgMap.get(majorVertexId);
            if(receivedMsgList.size() > 1){ // filter bubble
                // for each majorVertex, sort the node by decreasing order of coverage
                Collections.sort(receivedMsgList, new BubbleMergeMessageWritable.SortByCoverage());
                
                // process similarSet, keep the unchanged set and deleted set & add coverage to unchange node 
                processSimilarSet();
            }
        }
    }
    
    /**
     * step 3
     */
    public void receiveUpdates(Iterator<BubbleMergeMessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            BubbleMergeMessageWritable incomingMsg = msgIterator.next();
            short msgType = (short) (incomingMsg.getFlag() & MessageFlag.MSG_TYPE_MASK);
            switch(msgType){
                case MessageFlag.UNCHANGE:
                    // update Node including average coverage 
                    getVertexValue().setNode(incomingMsg.getNode());
                    activate();
                    break;
                case MessageFlag.KILL:
                    broadcastKillself();
                    deleteVertex(getVertexId());
                    break;
            }
        } 
    }
    
    @Override
    public void compute(Iterator<BubbleMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            detectBubble();
        } else if (getSuperstep() == 2){
            processBubblesInMinorVertex(msgIterator);
        } else if (getSuperstep() == 3){
            receiveUpdates(msgIterator);
        } else if (getSuperstep() == 5){
            responseToDeadNode(msgIterator);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, SimpleBubbleMergeVertex.class));
    }
}
