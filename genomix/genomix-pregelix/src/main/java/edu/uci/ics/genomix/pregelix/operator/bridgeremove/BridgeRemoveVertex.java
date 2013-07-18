package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.DataCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.DataCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicPathMergeVertex;
import edu.uci.ics.genomix.pregelix.type.AdjMessage;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: MessageWritable
 * 
 * DNA:
 * A: 00
 * C: 01
 * G: 10
 * T: 11
 * 
 * succeed node
 *  A 00000001 1
 *  G 00000010 2
 *  C 00000100 4
 *  T 00001000 8
 * precursor node
 *  A 00010000 16
 *  G 00100000 32
 *  C 01000000 64
 *  T 10000000 128
 *  
 * For example, ONE LINE in input file: 00,01,10    0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 * Naive Algorithm for path merge graph
 */
public class BridgeRemoveVertex extends
    BasicPathMergeVertex {
    public static final String LENGTH = "BridgeRemoveVertex.length";
    private int length = -1;

    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
   
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if(length == -1)
            length = getContext().getConfiguration().getInt(LENGTH, kmerSize + 5);
        outgoingMsg.reset();
        receivedMsgList.clear();
    }
    
    /**
     * broadcast kill self to all neighbers 
     */
    public void broadcaseKillself(){
        outgoingMsg.setSourceVertexId(getVertexId());
        if(receivedMsgList.get(0).getFlag() == AdjMessage.FROMFF 
                && receivedMsgList.get(1).getFlag() == AdjMessage.FROMRR){
            outgoingMsg.setFlag(AdjMessage.FROMRR);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMFF);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } else if (receivedMsgList.get(0).getFlag() == AdjMessage.FROMFF 
                && receivedMsgList.get(1).getFlag() == AdjMessage.FROMRF) {
            outgoingMsg.setFlag(AdjMessage.FROMRR);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMRF);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } else if (receivedMsgList.get(0).getFlag() == AdjMessage.FROMFR 
                && receivedMsgList.get(1).getFlag() == AdjMessage.FROMRR) {
            outgoingMsg.setFlag(AdjMessage.FROMFR);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMFF);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } else if (receivedMsgList.get(0).getFlag() == AdjMessage.FROMFR 
                && receivedMsgList.get(1).getFlag() == AdjMessage.FROMRF) {
            outgoingMsg.setFlag(AdjMessage.FROMFR);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMRF);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } // RR
        else if(receivedMsgList.get(1).getFlag() == AdjMessage.FROMFF 
                && receivedMsgList.get(0).getFlag() == AdjMessage.FROMRR){
            outgoingMsg.setFlag(AdjMessage.FROMRR);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMFF);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } else if (receivedMsgList.get(1).getFlag() == AdjMessage.FROMFF 
                && receivedMsgList.get(0).getFlag() == AdjMessage.FROMRF) {
            outgoingMsg.setFlag(AdjMessage.FROMRR);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMRF);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } else if (receivedMsgList.get(1).getFlag() == AdjMessage.FROMFR 
                && receivedMsgList.get(0).getFlag() == AdjMessage.FROMRR) {
            outgoingMsg.setFlag(AdjMessage.FROMFR);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMFF);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } else if (receivedMsgList.get(1).getFlag() == AdjMessage.FROMFR 
                && receivedMsgList.get(0).getFlag() == AdjMessage.FROMRF) {
            outgoingMsg.setFlag(AdjMessage.FROMFR);
            sendMsg(receivedMsgList.get(1).getSourceVertexId(), outgoingMsg);
            outgoingMsg.setFlag(AdjMessage.FROMRF);
            sendMsg(receivedMsgList.get(0).getSourceVertexId(), outgoingMsg);
            deleteVertex(getVertexId());
        } else
            voteToHalt();
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if(VertexUtil.isUpBridgeVertex(getVertexValue())){
                sendSettledMsgToAllNextNodes(getVertexValue());
            }
            else if(VertexUtil.isDownBridgeVertex(getVertexValue())){
                sendSettledMsgToAllPreviousNodes(getVertexValue());
            }
        }
        else if (getSuperstep() == 2){
            int i = 0;
            while (msgIterator.hasNext()) {
                if(i == 3)
                    break;
                receivedMsgList.add(msgIterator.next());
                i++;
            }
            if(receivedMsgList.size() == 2){
                if(getVertexValue().getLengthOfKmer() <= length){
                    broadcaseKillself();
                }
            }
        }
        else if(getSuperstep() == 3){
            responseToDeadVertex(msgIterator);
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BridgeRemoveVertex.class.getSimpleName());
        job.setVertexClass(BridgeRemoveVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(DataCleanInputFormat.class);
        job.setVertexOutputFormatClass(DataCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
