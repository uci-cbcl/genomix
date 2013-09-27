package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.BubbleMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.NodeWritable.NeighborInfo;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * Graph clean pattern: Simple Bubble Merge
 * @author anbangx
 *
 */
public class SimpleBubbleMergeVertex extends
    BasicGraphCleanVertex<VertexValueWritable, BubbleMergeMessageWritable> {
    private float DISSIMILAR_THRESHOLD = -1;
    
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
        if(DISSIMILAR_THRESHOLD < 0)
            DISSIMILAR_THRESHOLD = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.BUBBLE_MERGE_MAX_DISSIMILARITY));
        if(outgoingMsg == null)
            outgoingMsg = new BubbleMergeMessageWritable();
        else
            outgoingMsg.reset();
        if(incomingEdgeList == null)
            incomingEdgeList = new EdgeListWritable();
        if(outgoingEdgeList == null)
            outgoingEdgeList = new EdgeListWritable();
        outFlag = 0;
        StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    public void sendBubbleAndMajorVertexMsgToMinorVertex(){
    	VertexValueWritable vertex = getVertexValue();
    	NeighborInfo reverseNeighbor = vertex.getSingleNeighbor(DIR.REVERSE);
    	NeighborInfo forwardNeighbor = vertex.getSingleNeighbor(DIR.REVERSE);

        //TODO combine into only one byte, change internally/under the hood
        // get majorVertex and minorVertex and meToMajorEdgeType and meToMinorEdgeType
        if(forwardNeighbor.kmer == reverseNeighbor.kmer) // FIXME should this *really* be == or .equals?
            throw new IllegalArgumentException("majorVertexId is equal to minorVertexId, this is not allowed!");
        
        VKmerBytesWritable minorVertexId;
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
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(minorVertexId, outgoingMsg);
    }
    
    public void aggregateBubbleNodesByMajorNode(Iterator<BubbleMergeMessageWritable> msgIterator){
    	receivedMsgMap.clear();
    	BubbleMergeMessageWritable incomingMsg;
    	ArrayList<BubbleMergeMessageWritable> curMsgList;
    	
        while (msgIterator.hasNext()) {
        	incomingMsg = msgIterator.next();
            if(!receivedMsgMap.containsKey(incomingMsg.getMajorVertexId())){
            	curMsgList = new ArrayList<BubbleMergeMessageWritable>();
                receivedMsgMap.put(incomingMsg.getMajorVertexId(), curMsgList);
            } else {
            	curMsgList = receivedMsgMap.get(incomingMsg.getMajorVertexId());                
            }
            curMsgList.add(incomingMsg);
        }
    }
    
    public static boolean isValidMajorAndMinor(BubbleMergeMessageWritable topMsg, BubbleMergeMessageWritable curMsg){
        EDGETYPE topMajorToBubbleEdgetype = topMsg.getMajorToBubbleEdgetype();
        EDGETYPE curMajorToBubbleEdgetype = curMsg.getMajorToBubbleEdgetype();
        EDGETYPE topMinorToBubbleEdgetype = topMsg.getMinorToBubbleEdgetype();
        EDGETYPE curMinorToBubbleEdgetype = curMsg.getMinorToBubbleEdgetype();
        return (topMajorToBubbleEdgetype.dir() == curMajorToBubbleEdgetype.dir()) && topMinorToBubbleEdgetype.dir() == curMinorToBubbleEdgetype.dir();
    }
    
    public boolean isFlipRelativeToMajor(BubbleMergeMessageWritable msg1, BubbleMergeMessageWritable msg2){
        return msg1.getRelativeDirToMajor() != msg2.getRelativeDirToMajor();
    }
    
    public void processSimilarSet(){
        while(!receivedMsgList.isEmpty()){
            Iterator<BubbleMergeMessageWritable> it = receivedMsgList.iterator();
            topMsg = it.next();
            it.remove(); //delete topCoverage node
            NodeWritable topNode = topMsg.getNode();
            boolean topChanged = false;
            while(it.hasNext()){
                curMsg = it.next();
                //check if the vertex is valid minor and if it comes from valid major TODO pass the check variable and make static
                if(!isValidMajorAndMinor(topMsg, curMsg))
                    continue;
                
                //compute the dissimilarity
                float fractionDissimilar = topMsg.computeDissimilar(curMsg); // TODO change to simmilarity everywhere
                
                if(fractionDissimilar <= DISSIMILAR_THRESHOLD){ //if similar with top node, delete this node and put it in deletedSet
                	topChanged = true;
                    // 1. add coverage to top node -- for unchangedSet
                    topNode.addFromNode(isFlipRelativeToMajor(topMsg, curMsg), 
                            curMsg.getNode()); 
                    
                    // 2. send message to delete vertices -- for deletedSet
                    outgoingMsg.reset();
                    outFlag = 0;
                    outFlag |= MessageFlag.KILL_SELF; //TODO make msg type flag to enum
                    outgoingMsg.setFlag(outFlag);
                    sendMsg(curMsg.getSourceVertexId(), outgoingMsg);
                    it.remove();
                }
            }
            // process topNode -- send message to topVertex to update their coverage
            //TODO if only one field needs to set in flag, directly set
            if (topChanged) {
	            outgoingMsg.reset();
	            outFlag = 0;
	            outFlag |= MessageFlag.REPLACE_NODE;
	            outgoingMsg.setNode(topNode);
	            outgoingMsg.setFlag(outFlag);
	            sendMsg(topMsg.getSourceVertexId(), outgoingMsg);
            }
        }
    }
    
    /**
     * step 1
     */
    public void detectBubble(){
        VertexValueWritable vertex = getVertexValue();
        if(vertex.getDegree(DIR.REVERSE) == 1 && vertex.getDegree(DIR.FORWARD) == 1){
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
            receivedMsgList = receivedMsgMap.get(majorVertexId);
            if(receivedMsgList.size() > 1){ // filter simple paths
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
            short msgType = (short) (incomingMsg.getFlag() & MessageFlag.MSG_TYPE_MASK); //TODO change msg type to enum
            switch(msgType){
                case MessageFlag.REPLACE_NODE:
                    // update Node including average coverage 
                    getVertexValue().setNode(incomingMsg.getNode());
                    activate();
                    break;
                case MessageFlag.KILL_SELF:
                    broadcastKillself();
                    deleteVertex(getVertexId());
                    break;
            }
        } 
    }
    
    @Override
    public void compute(Iterator<BubbleMergeMessageWritable> msgIterator) {
        if (getSuperstep() == 1) {
        	initVertex();
            detectBubble();
        } else if (getSuperstep() == 2){
            processBubblesInMinorVertex(msgIterator);
        } else if (getSuperstep() == 3){
            receiveUpdates(msgIterator);
        } else if (getSuperstep() == 4){
            responseToDeadNode(msgIterator);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, SimpleBubbleMergeVertex.class));
    }
}
