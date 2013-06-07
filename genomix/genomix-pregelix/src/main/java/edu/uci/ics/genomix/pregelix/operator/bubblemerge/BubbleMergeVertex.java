package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.DataCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.DataCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MergeBubbleMessageWritable;
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
public class BubbleMergeVertex extends
        Vertex<PositionWritable, ValueStateWritable, NullWritable, MergeBubbleMessageWritable> {
    public static final String KMER_SIZE = "BubbleMergeVertex.kmerSize";
    public static final String ITERATIONS = "BubbleMergeVertex.iteration";
    public static int kmerSize = -1;
    private int maxIteration = -1;

    private MergeBubbleMessageWritable incomingMsg = new MergeBubbleMessageWritable();
    private MergeBubbleMessageWritable outgoingMsg = new MergeBubbleMessageWritable();

    private PositionWritable destVertexId = new PositionWritable();
    private Iterator<PositionWritable> posIterator;
    private Map<PositionWritable, ArrayList<MergeBubbleMessageWritable>> receivedMsg = new HashMap<PositionWritable, ArrayList<MergeBubbleMessageWritable>>();
    private ArrayList<MergeBubbleMessageWritable> tmpMsg = new ArrayList<MergeBubbleMessageWritable>();
    
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
    /**
     * get destination vertex
     */
    public PositionWritable getNextDestVertexId(ValueStateWritable value) {
        if(value.getFFList().getCountOfPosition() > 0) // #FFList() > 0
            posIterator = value.getFFList().iterator();
        else // #FRList() > 0
            posIterator = value.getFRList().iterator();
        return posIterator.next();
    }

    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes(ValueStateWritable value) {
        posIterator = value.getFFList().iterator(); // FFList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getFRList().iterator(); // FRList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    @Override
    public void compute(Iterator<MergeBubbleMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if(VertexUtil.isHeadVertex(getVertexValue())){
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsgToAllNextNodes(getVertexValue());
            }
        } else if (getSuperstep() == 2){
            while (msgIterator.hasNext()) {
                if(VertexUtil.isPathVertex(getVertexValue())){
                    outgoingMsg.setStartVertexId(incomingMsg.getSourceVertexId());
                    outgoingMsg.setSourceVertexId(getVertexId());
                    outgoingMsg.setChainVertexId(getVertexValue().getMergeChain());
                    destVertexId.set(getNextDestVertexId(getVertexValue()));
                    sendMsg(destVertexId, outgoingMsg);
                }
            }
        } else if (getSuperstep() == 3){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(!receivedMsg.containsKey(incomingMsg.getStartVertexId())){
                    tmpMsg.clear();
                    tmpMsg.add(incomingMsg);
                    receivedMsg.put(incomingMsg.getStartVertexId(), tmpMsg);
                }
                else{
                    tmpMsg.clear();
                    tmpMsg.addAll(receivedMsg.get(incomingMsg.getStartVertexId()));
                    tmpMsg.add(incomingMsg);
                    receivedMsg.put(incomingMsg.getStartVertexId(), tmpMsg);
                }
            }
            for(PositionWritable prevId : receivedMsg.keySet()){
                tmpMsg = receivedMsg.get(prevId);
                if(tmpMsg.size() > 1){
                    //find the node with largest length of mergeChain
                    boolean flag = true; //the same length
                    int maxLength = tmpMsg.get(0).getLengthOfChain();
                    PositionWritable max = tmpMsg.get(0).getSourceVertexId();
                    for(int i = 1; i < tmpMsg.size(); i++){
                        if(tmpMsg.get(i).getLengthOfChain() != maxLength)
                            flag = false;
                        if(tmpMsg.get(i).getLengthOfChain() > maxLength){
                            maxLength = tmpMsg.get(i).getLengthOfChain();
                            max = tmpMsg.get(i).getSourceVertexId();
                        }
                    }
                    //send merge or unchange Message to node with largest length
                    if(flag == true){
                        //send unchange Message to node with largest length
                        //we can send no message to complete this step
                        //send delete Message to node which doesn't have largest length
                        for(int i = 0; i < tmpMsg.size(); i++){
                            if(tmpMsg.get(i).getSourceVertexId().compareTo(max) != 0){
                                outgoingMsg.setMessage(AdjMessage.KILL);
                                sendMsg(tmpMsg.get(i).getSourceVertexId(), outgoingMsg);
                            } else {
                                outgoingMsg.setMessage(AdjMessage.UNCHANGE);
                                sendMsg(tmpMsg.get(i).getSourceVertexId(), outgoingMsg);
                            }
                        }
                    } else{
                        //send merge Message to node with largest length
                        for(int i = 0; i < tmpMsg.size(); i++){
                            if(tmpMsg.get(i).getSourceVertexId().compareTo(max) != 0){
                                outgoingMsg.setMessage(AdjMessage.KILL);
                                sendMsg(tmpMsg.get(i).getSourceVertexId(), outgoingMsg);
                            } else {
                                outgoingMsg.setMessage(AdjMessage.MERGE);
                                /* add other node in message */
                                sendMsg(tmpMsg.get(i).getSourceVertexId(), outgoingMsg);
                            }
                        }
                    }
                }
            }
        } else if (getSuperstep() == 4){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getMessage() == AdjMessage.KILL){
                    deleteVertex(getVertexId());
                } else if (incomingMsg.getMessage() == AdjMessage.MERGE){
                    //merge with small node
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
        job.setVertexInputFormatClass(DataCleanInputFormat.class);
        job.setVertexOutputFormatClass(DataCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
    }
}
