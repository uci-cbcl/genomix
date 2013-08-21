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
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.BubbleMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/**
 * Naive Algorithm for path merge graph
 */
public class BubbleMergeVertex extends
    BasicGraphCleanVertex<BubbleMergeMessageWritable> {
    public static final String DISSIMILARITY_THRESHOLD = "BubbleMergeVertex.dissimilarThreshold";
    private float dissimilarThreshold = -1;
    
    public static class EdgeType{
        public static final byte INCOMINGEDGE = 0b1 << 0;
        public static final byte OUTGOINGEDGE = 0b1 << 1;
    }
    
    private Map<VKmerBytesWritable, ArrayList<BubbleMergeMessageWritable>> receivedMsgMap = new HashMap<VKmerBytesWritable, ArrayList<BubbleMergeMessageWritable>>();
    private ArrayList<BubbleMergeMessageWritable> receivedMsgList = new ArrayList<BubbleMergeMessageWritable>();
    private BubbleMergeMessageWritable topCoverageMessage = new BubbleMergeMessageWritable();
    private BubbleMergeMessageWritable curMessage = new BubbleMergeMessageWritable();
    private Set<BubbleMergeMessageWritable> unchangedSet = new HashSet<BubbleMergeMessageWritable>();
    private Set<BubbleMergeMessageWritable> deletedSet = new HashSet<BubbleMergeMessageWritable>();

    private VKmerBytesWritable incomingKmer = new VKmerBytesWritable();
    private VKmerBytesWritable outgoingKmer = new VKmerBytesWritable();
    private VKmerBytesWritable majorVertexId = new VKmerBytesWritable();
    private VKmerBytesWritable minorVertexId = new VKmerBytesWritable();
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(dissimilarThreshold == -1)
            dissimilarThreshold = getContext().getConfiguration().getFloat(DISSIMILARITY_THRESHOLD, (float) 0.5);
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
    }
    
    public void sendBubbleAndMajorVertexMsgToMinorVertex(){
        for(int i = 0; i < 4; i++){
            /** set edgeList and edgeDir based on connectedTable **/
            setEdgeListAndEdgeDir(i);
            
            for(EdgeWritable incomingEdge : incomingEdgeList){
                for(EdgeWritable outgoingEdge : outgoingEdgeList){
                    /** get majorVertex and minorVertex and meToMajorDir and meToMinorDir **/
                    incomingKmer.setAsCopy(incomingEdge.getKey());
                    outgoingKmer.setAsCopy(outgoingEdge.getKey());
                    majorVertexId.setAsCopy(incomingKmer.compareTo(outgoingKmer) >= 0 ? incomingKmer : outgoingKmer);
                    minorVertexId.setAsCopy(incomingKmer.compareTo(outgoingKmer) < 0 ? incomingKmer : outgoingKmer);
                    byte majorToMeDir = (incomingKmer.compareTo(outgoingKmer) >= 0 ? incomingEdgeDir : outgoingEdgeDir);
                    byte meToMajorDir = mirrorDirection(majorToMeDir);
                    byte minorToMeDir = (incomingKmer.compareTo(outgoingKmer) < 0 ? incomingEdgeDir : outgoingEdgeDir);
                    byte meToMinorDir = mirrorDirection(minorToMeDir);
                    
                    /** setup outgoingMsg **/
                    outgoingMsg.setMajorVertexId(majorVertexId);
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setNode(getVertexValue().getNode());
                    outgoingMsg.setMeToMajorDir(meToMajorDir);
                    outgoingMsg.setMeToMinorDir(meToMinorDir);
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
    
    public byte getEdgeTypeFromDir(byte dir){
        switch(dir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                return EdgeType.OUTGOINGEDGE;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                return EdgeType.INCOMINGEDGE;
        }
        return 0;
    }
    
    public boolean isSameEdgeType(byte edgeDir1, byte edgeDir2){
        return getEdgeTypeFromDir(edgeDir1) == getEdgeTypeFromDir(edgeDir2);
    }
    
    public boolean isValidMajorAndMinor(){
        byte topBubbleToMajorDir = topCoverageMessage.getMeToMajorDir();
        byte curBubbleToMajorDir = curMessage.getMeToMajorDir();
        byte topBubbleToMinorDir = topCoverageMessage.getMeToMinorDir();
        byte curBubbleToMinorDir = curMessage.getMeToMinorDir();
        return isSameEdgeType(topBubbleToMajorDir, curBubbleToMajorDir) && isSameEdgeType(topBubbleToMinorDir, curBubbleToMinorDir);
    }
    
    public void processSimilarSetToUnchangeSetAndDeletedSet(){
        unchangedSet.clear();
        deletedSet.clear();
        topCoverageMessage.reset();
        curMessage.reset();
        Iterator<BubbleMergeMessageWritable> it;
        while(!receivedMsgList.isEmpty()){
            it = receivedMsgList.iterator();
            topCoverageMessage.set(it.next());
            it.remove(); //delete topCoverage node
            while(it.hasNext()){
                curMessage.set(it.next());
                //check if the vertex is valid minor and if it comes from valid major
                if(!isValidMajorAndMinor())
                    continue;
                //compute the similarity  
                float fracDissimilar = topCoverageMessage.computeDissimilar(curMessage);
                if(fracDissimilar < dissimilarThreshold){ //if similar with top node, delete this node and put it in deletedSet 
                    //add coverage to top node
                    topCoverageMessage.getNode().addFromNode(curMessage.getNode());
                    deletedSet.add(curMessage);
                    it.remove();
                }
            }
            unchangedSet.add(topCoverageMessage);
        }
    }
    
    public void processUnchangedSet(){
        for(BubbleMergeMessageWritable msg : unchangedSet){
            outFlag = MessageFlag.UNCHANGE;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setNode(msg.getNode());
            sendMsg(msg.getSourceVertexId(), outgoingMsg);
        }
    }
    
    public void processDeletedSet(){
        for(BubbleMergeMessageWritable msg : deletedSet){
            outFlag = MessageFlag.KILL;
            outgoingMsg.setFlag(outFlag);
            sendMsg(msg.getSourceVertexId(), outgoingMsg);
        }
    }
    
    @Override
    public void compute(Iterator<BubbleMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if(VertexUtil.isBubbleVertex(getVertexValue())){
                /** send bubble and major vertex msg to minor vertex **/
                sendBubbleAndMajorVertexMsgToMinorVertex();
            }
        } else if (getSuperstep() == 2){
            /** aggregate bubble nodes and grouped by major vertex **/ 
            aggregateBubbleNodesByMajorNode(msgIterator);
            
            for(VKmerBytesWritable prevId : receivedMsgMap.keySet()){
                if(receivedMsgList.size() > 1){ // filter bubble
                    /** for each majorVertex, sort the node by decreasing order of coverage **/
                    receivedMsgList = receivedMsgMap.get(prevId);
                    Collections.sort(receivedMsgList, new BubbleMergeMessageWritable.SortByCoverage());
                    
                    /** process similarSet, keep the unchanged set and deleted set & add coverage to unchange node **/
                    processSimilarSetToUnchangeSetAndDeletedSet();
                    
                    /** send message to the unchanged set for updating coverage & send kill message to the deleted set **/ 
                    processUnchangedSet();
                    processDeletedSet();
                }
            }
        } else if (getSuperstep() == 3){
            if(msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getFlag() == MessageFlag.KILL){
                    broadcaseKillself();
                } else if(incomingMsg.getFlag() == MessageFlag.UNCHANGE){
                    /** update average coverage **/
                    getVertexValue().setNode(incomingMsg.getNode());
                }
            }
        } else if(getSuperstep() == 4){
            if(msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(isResponseKillMsg()){
                    responseToDeadVertex();
                }
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BubbleMergeVertex.class.getSimpleName());
        job.setVertexClass(BubbleMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
