package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
    
    private Map<VKmerBytesWritable, ArrayList<BubbleMergeMessageWritable>> receivedMsgMap = new HashMap<VKmerBytesWritable, ArrayList<BubbleMergeMessageWritable>>();
    private ArrayList<BubbleMergeMessageWritable> receivedMsgList = new ArrayList<BubbleMergeMessageWritable>();
    private Set<BubbleMergeMessageWritable> unchangedSet = new HashSet<BubbleMergeMessageWritable>();
    private Set<BubbleMergeMessageWritable> deletedSet = new HashSet<BubbleMergeMessageWritable>();

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
        outFlag = 0;
    }
    
    public void sendBubbleAndMajorVertexMsgToMinorVertex(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                if(getVertexValue().hasNextDest()){
                    outgoingMsg.setMajorVertexId(incomingMsg.getSourceVertexId());
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setNode(getVertexValue().getNode());
                    destVertexId.setAsCopy(getNextDestVertexId());
                    sendMsg(destVertexId, outgoingMsg);
                }
                break;
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                if(getVertexValue().hasPrevDest()){
                    outgoingMsg.setMajorVertexId(incomingMsg.getSourceVertexId());
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setNode(getVertexValue().getNode());
                    destVertexId.setAsCopy(getPrevDestVertexId());
                    sendMsg(destVertexId, outgoingMsg);
                }
                break;
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
    
    public void processSimilarSetToUnchangeSetAndDeletedSet(){
        unchangedSet.clear();
        deletedSet.clear();
        BubbleMergeMessageWritable topCoverageMessage = new BubbleMergeMessageWritable();
        BubbleMergeMessageWritable tmpMessage = new BubbleMergeMessageWritable();
        Iterator<BubbleMergeMessageWritable> it;
        while(!receivedMsgList.isEmpty()){
            it = receivedMsgList.iterator();
            topCoverageMessage.set(it.next());
            it.remove(); //delete topCoverage node
            while(it.hasNext()){
                tmpMessage.set(it.next());
                //compute the similarity  TODO take care of if flip
                float fracDissimilar = topCoverageMessage.getSourceVertexId().fracDissimilar(tmpMessage.getSourceVertexId());
                if(fracDissimilar < dissimilarThreshold){ //If similar with top node, delete this node and put it in deletedSet 
                    //add coverage to top node
                    topCoverageMessage.getNode().mergeCoverage(tmpMessage.getNode());
                    deletedSet.add(tmpMessage);
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
            if(VertexUtil.isHeadVertexWithIndegree(getVertexValue())
                    || VertexUtil.isHeadWithoutIndegree(getVertexValue())){
                sendSettledMsgToAllNextNodes();
            }
        } else if (getSuperstep() == 2){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(VertexUtil.isPathVertex(getVertexValue())){
                    /** send bubble and major vertex msg to minor vertex **/
                    sendBubbleAndMajorVertexMsgToMinorVertex();
                }
            }
        } else if (getSuperstep() == 3){
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
        } else if (getSuperstep() == 4){
            if(msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getFlag() == MessageFlag.KILL){
                    broadcaseKillself();
                } else if(incomingMsg.getFlag() == MessageFlag.UNCHANGE){
                    /** update average coverage **/
                    getVertexValue().setAvgCoverage(incomingMsg.getNode().getAverageCoverage());
                }
            }
        } else if(getSuperstep() == 5){
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
