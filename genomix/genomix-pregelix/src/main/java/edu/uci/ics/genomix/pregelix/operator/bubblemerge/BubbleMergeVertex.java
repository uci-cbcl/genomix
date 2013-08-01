package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MergeBubbleMessageWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.AdjMessage;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/**
 * Naive Algorithm for path merge graph
 */
public class BubbleMergeVertex extends
    BasicGraphCleanVertex {

    private KmerBytesWritableFactory kmerFactory = new KmerBytesWritableFactory(1);
    
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
                    byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
                    byte neighborToMeDir = mirrorDirection(meToNeighborDir);
                    switch(neighborToMeDir){
                        case MessageFlag.DIR_RF:
                        case MessageFlag.DIR_RR:
                            if(hasNextDest(getVertexValue())){
                                outgoingMsg.setStartVertexId(incomingMsg.getSourceVertexId());
                                outgoingMsg.setSourceVertexId(getVertexId());
                                outgoingMsg.setActualKmer(getVertexValue().getKmer());
                                destVertexId.setAsCopy(getNextDestVertexId(getVertexValue()));
                                sendMsg(destVertexId, outgoingMsg);
                            }
                            break;
                        case MessageFlag.DIR_FF:
                        case MessageFlag.DIR_FR:
                            if(hasPrevDest(getVertexValue())){
                                outgoingMsg.setStartVertexId(incomingMsg.getSourceVertexId());
                                outgoingMsg.setSourceVertexId(getVertexId());
                                outgoingMsg.setActualKmer(getVertexValue().getKmer());
                                destVertexId.setAsCopy(getPrevDestVertexId(getVertexValue()));
                                sendMsg(destVertexId, outgoingMsg);
                            }
                            break;
                    }
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
                receivedMsgList = receivedMsgMap.get(prevId);
                if(receivedMsgList.size() > 1){
                    /** for each startVertex, sort the node by decreasing order of coverage **/
                    
                    /** process similarSet, keep the unchanged set and deleted set & add coverage to unchange node **/
                    
                    /** send message to the unchanged set for updating coverage & send kill message to the deleted set **/ 
                    //send unchange or merge Message to node with largest length
                    if(flag == true){
                        //1. send unchange Message to node with largest length
                        //   we can send no message to complete this step
                        //2. send delete Message to node which doesn't have largest length
                        for(int i = 0; i < receivedMsgList.size(); i++){
                            //if(receivedMsgList.get(i).getSourceVertexId().compareTo(max) != 0)
                            if(receivedMsgList.get(i).getSourceVertexId().compareTo(secondMax) == 0){ 
                                outgoingMsg.setMessage(AdjMessage.KILL);
                                outgoingMsg.setStartVertexId(prevId);
                                outgoingMsg.setSourceVertexId(getVertexId());
                                sendMsg(secondMax, outgoingMsg);
                            } else if(receivedMsgList.get(i).getSourceVertexId().compareTo(max) == 0){
                                outgoingMsg.setMessage(AdjMessage.UNCHANGE);
                                sendMsg(max, outgoingMsg);
                            }
                        }
                    } else{
                        //send merge Message to node with largest length
                        for(int i = 0; i < receivedMsgList.size(); i++){
                            //if(receivedMsgList.get(i).getSourceVertexId().compareTo(max) != 0)
                            if(receivedMsgList.get(i).getSourceVertexId().compareTo(secondMax) == 0){
                                outgoingMsg.setMessage(AdjMessage.KILL);
                                outgoingMsg.setStartVertexId(prevId);
                                outgoingMsg.setSourceVertexId(getVertexId());
                                sendMsg(receivedMsgList.get(i).getSourceVertexId(), outgoingMsg);
                            } else if(receivedMsgList.get(i).getSourceVertexId().compareTo(max) == 0){
                                outgoingMsg.setMessage(AdjMessage.MERGE);
                                /* add other node in message */
                                for(int j = 0; j < receivedMsgList.size(); i++){
                                    if(receivedMsgList.get(j).getSourceVertexId().compareTo(secondMax) == 0){
                                        outgoingMsg.setChainVertexId(receivedMsgList.get(j).getChainVertexId());
                                        break;
                                    }
                                }
                                sendMsg(receivedMsgList.get(i).getSourceVertexId(), outgoingMsg);
                            }
                        }
                    }
                }
            }
        } else if (getSuperstep() == 4){
            if(msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getMessage() == AdjMessage.KILL){
                    broadcaseKillself();
                } else if (incomingMsg.getMessage() == AdjMessage.MERGE){
                    //merge with small node
                    getVertexValue().setKmer(kmerFactory.mergeTwoKmer(getVertexValue().getKmer(), 
                            incomingMsg.getChainVertexId()));
                }
            }
        } else if(getSuperstep() == 5){
            responseToDeadVertex(msgIterator);
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
