package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.DataCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.DataCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
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
        Vertex<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "BridgeRemoveVertex.kmerSize";
    public static final String LENGTH = "BridgeRemoveVertex.length";
    public static int kmerSize = -1;
    private int length = -1;

    private MessageWritable incomingMsg = new MessageWritable();
    private MessageWritable outgoingMsg = new MessageWritable();
    private ArrayList<MessageWritable> receivedMsg = new ArrayList<MessageWritable>();
    
    private PositionWritable destVertexId = new PositionWritable();
    private Iterator<PositionWritable> posIterator;
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if(length == -1)
            length = getContext().getConfiguration().getInt(LENGTH, kmerSize + 5);
        outgoingMsg.reset();
        receivedMsg.clear();
    }
    
    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes(ValueStateWritable value) {
        posIterator = value.getFFList().iterator(); // FFList
        while(posIterator.hasNext()){
            outgoingMsg.setMessage(AdjMessage.FROMFF);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getFRList().iterator(); // FRList
        while(posIterator.hasNext()){
            outgoingMsg.setMessage(AdjMessage.FROMFR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    /**
     * head send message to all previous nodes
     */
    public void sendMsgToAllPreviousNodes(ValueStateWritable value) {
        posIterator = value.getRFList().iterator(); // RFList
        while(posIterator.hasNext()){
            outgoingMsg.setMessage(AdjMessage.FROMRF);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getRRList().iterator(); // RRList
        while(posIterator.hasNext()){
            outgoingMsg.setMessage(AdjMessage.FROMRR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if(VertexUtil.isUpBridgeVertex(getVertexValue())){
                sendMsgToAllNextNodes(getVertexValue());
            }
           else if(VertexUtil.isUpBridgeVertex(getVertexValue())){
                sendMsgToAllPreviousNodes(getVertexValue());
           }
        }
        else if (getSuperstep() == 2){
            int i = 0;
            while (msgIterator.hasNext()) {
                if(i == 3)
                    break;
                receivedMsg.add(msgIterator.next());
                i++;
            }
            if(receivedMsg.size() == 2){
                if(getVertexValue().getLengthOfMergeChain() > length){
                    outgoingMsg.setSourceVertexId(getVertexId());
                    if(receivedMsg.get(0).getMessage() == AdjMessage.FROMFF 
                            && receivedMsg.get(1).getMessage() == AdjMessage.FROMRR){
                        outgoingMsg.setMessage(AdjMessage.FROMRR);
                        sendMsg(receivedMsg.get(0).getSourceVertexId(), outgoingMsg);
                        outgoingMsg.setMessage(AdjMessage.FROMFF);
                        sendMsg(receivedMsg.get(1).getSourceVertexId(), outgoingMsg);
                        deleteVertex(getVertexId());
                    } else if (receivedMsg.get(0).getMessage() == AdjMessage.FROMFF 
                            && receivedMsg.get(1).getMessage() == AdjMessage.FROMRF) {
                        outgoingMsg.setMessage(AdjMessage.FROMRR);
                        sendMsg(receivedMsg.get(0).getSourceVertexId(), outgoingMsg);
                        outgoingMsg.setMessage(AdjMessage.FROMFR);
                        sendMsg(receivedMsg.get(1).getSourceVertexId(), outgoingMsg);
                        deleteVertex(getVertexId());
                    } else if (receivedMsg.get(0).getMessage() == AdjMessage.FROMFR 
                            && receivedMsg.get(1).getMessage() == AdjMessage.FROMRR) {
                        outgoingMsg.setMessage(AdjMessage.FROMRF);
                        sendMsg(receivedMsg.get(0).getSourceVertexId(), outgoingMsg);
                        outgoingMsg.setMessage(AdjMessage.FROMFF);
                        sendMsg(receivedMsg.get(1).getSourceVertexId(), outgoingMsg);
                        deleteVertex(getVertexId());
                    } else if (receivedMsg.get(0).getMessage() == AdjMessage.FROMFR 
                            && receivedMsg.get(1).getMessage() == AdjMessage.FROMRF) {
                        outgoingMsg.setMessage(AdjMessage.FROMRF);
                        sendMsg(receivedMsg.get(0).getSourceVertexId(), outgoingMsg);
                        outgoingMsg.setMessage(AdjMessage.FROMFR);
                        sendMsg(receivedMsg.get(1).getSourceVertexId(), outgoingMsg);
                        deleteVertex(getVertexId());
                    }
                }
            }
        }
        else if(getSuperstep() == 3){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getMessage() == AdjMessage.FROMFF){
                    //remove incomingMsg.getSourceId from RR positionList
                } else if(incomingMsg.getMessage() == AdjMessage.FROMFR){
                  //remove incomingMsg.getSourceId from RF positionList
                } else if(incomingMsg.getMessage() == AdjMessage.FROMRF){
                  //remove incomingMsg.getSourceId from FR positionList
                } else{ //incomingMsg.getMessage() == AdjMessage.FROMRR
                  //remove incomingMsg.getSourceId from FF positionList
                }
            }
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
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
    }
}
