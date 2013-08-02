package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/**
 * Naive Algorithm for path merge graph
 */
public class BubbleMergeVertex extends
    BasicGraphCleanVertex {

    private Map<VKmerBytesWritable, ArrayList<MessageWritable>> receivedMsgMap = new HashMap<VKmerBytesWritable, ArrayList<MessageWritable>>();
    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        outgoingMsg.reset();
    }
    
    public void sendBubbleAndMajorVertexMsgToMinorVertex(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                if(hasNextDest(getVertexValue())){
                    outgoingMsg.setStartVertexId(incomingMsg.getSourceVertexId());
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setActualKmer(getVertexValue().getActualKmer());
                    destVertexId.setAsCopy(getNextDestVertexId(getVertexValue()));
                    sendMsg(destVertexId, outgoingMsg);
                }
                break;
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                if(hasPrevDest(getVertexValue())){
                    outgoingMsg.setStartVertexId(incomingMsg.getSourceVertexId());
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setActualKmer(getVertexValue().getActualKmer());
                    destVertexId.setAsCopy(getPrevDestVertexId(getVertexValue()));
                    sendMsg(destVertexId, outgoingMsg);
                }
                break;
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if(VertexUtil.isHeadVertexWithIndegree(getVertexValue())
                    || VertexUtil.isHeadWithoutIndegree(getVertexValue())){
                sendSettledMsgToAllNextNodes(getVertexValue());
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
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(!receivedMsgMap.containsKey(incomingMsg.getStartVertexId())){
                    receivedMsgList.clear();
                    receivedMsgList.add(incomingMsg);
                    receivedMsgMap.put(incomingMsg.getStartVertexId(), (ArrayList<MessageWritable>)receivedMsgList.clone());
                }
                else{
                    receivedMsgList.clear();
                    receivedMsgList.addAll(receivedMsgMap.get(incomingMsg.getStartVertexId()));
                    receivedMsgList.add(incomingMsg);
                    receivedMsgMap.put(incomingMsg.getStartVertexId(), (ArrayList<MessageWritable>)receivedMsgList.clone());
                }
            }
            for(VKmerBytesWritable prevId : receivedMsgMap.keySet()){
                if(receivedMsgList.size() > 1){ // filter bubble
                    /** for each startVertex, sort the node by decreasing order of coverage **/
                    receivedMsgList = receivedMsgMap.get(prevId);
                    Collections.sort(receivedMsgList, new MessageWritable.SortByCoverage());
                    System.out.println("");
                    
                    
                    /** process similarSet, keep the unchanged set and deleted set & add coverage to unchange node **/
                    
                    /** send message to the unchanged set for updating coverage & send kill message to the deleted set **/ 
                    
                }
            }
        } else if (getSuperstep() == 4){
            if(msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getFlag() == MessageFlag.KILL){
                    broadcaseKillself();
                } 
            }
        } else if(getSuperstep() == 5){
            if(msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getFlag() == MessageFlag.KILL){
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
