package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.PositionWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.AdjacencyListWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.State;
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
public class P4ForPathMergeVertex extends
        Vertex<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "P4ForPathMergeVertex.kmerSize";
    public static final String ITERATIONS = "P4ForPathMergeVertex.iteration";
    public static final String RANDSEED = "P4ForPathMergeVertex.randSeed";
    public static final String PROBBEINGRANDOMHEAD = "P4ForPathMergeVertex.probBeingRandomHead";
    public static int kmerSize = -1;
    private int maxIteration = -1;

    private static long randSeed = -1;
    private float probBeingRandomHead = -1;
    private Random randGenerator;
    
    private PositionWritable curID = new PositionWritable();
    private PositionWritable nextID = new PositionWritable();
    private PositionWritable prevID = new PositionWritable();
    private boolean hasNext;
    private boolean hasPrev;
    private boolean curHead;
    private boolean nextHead;
    private boolean prevHead;
    private byte headFlag;
    private byte tailFlag;
    private byte outFlag;
    private byte outUpFlag;
    
    private MessageWritable incomingMsg = new MessageWritable();
    private MessageWritable outgoingMsg = new MessageWritable();
    private MessageWritable outgoingUpMsg = new MessageWritable();
    private PositionWritable destVertexId = new PositionWritable();
    private Iterator<PositionWritable> posIterator;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if (randSeed < 0)
            randSeed = getContext().getConfiguration().getLong("randomSeed", 0);
        randGenerator = new Random(randSeed);
        if (probBeingRandomHead < 0)
            probBeingRandomHead = getContext().getConfiguration().getFloat("probBeingRandomHead", 0.5f);
        hasNext = false;
        hasPrev = false;
        curHead = false;
        nextHead = false;
        prevHead = false;
        outgoingMsg.reset();
    }

    protected boolean isNodeRandomHead(PositionWritable nodeID) {
        // "deterministically random", based on node id
        randGenerator.setSeed(randSeed ^ nodeID.hashCode());
        return randGenerator.nextFloat() < probBeingRandomHead;
    }

    /**
     * set nextID to the element that's next (in the node's FF or FR list), returning true when there is a next neighbor
     */
    protected boolean setNextInfo(ValueStateWritable value) {
        if (value.getFFList().getCountOfPosition() > 0) {
            nextID.set(value.getFFList().getPosition(0));
            nextHead = isNodeRandomHead(nextID);
            return true;
        }
        if (value.getFRList().getCountOfPosition() > 0) {
            nextID.set(value.getFRList().getPosition(0));
            nextHead = isNodeRandomHead(nextID);
            return true;
        }
        return false;
    }

    /**
     * set prevID to the element that's previous (in the node's RR or RF list), returning true when there is a previous neighbor
     */
    protected boolean setPrevInfo(ValueStateWritable value) {
        if (value.getRRList().getCountOfPosition() > 0) {
            prevID.set(value.getRRList().getPosition(0));
            prevHead = isNodeRandomHead(prevID);
            return true;
        }
        if (value.getRFList().getCountOfPosition() > 0) {
            prevID.set(value.getRFList().getPosition(0));
            prevHead = isNodeRandomHead(prevID);
            return true;
        }
        return false;
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

    public PositionWritable getPreDestVertexId(ValueStateWritable value) {
        if(value.getRFList().getCountOfPosition() > 0) // #RFList() > 0
            posIterator = value.getRFList().iterator();
        else // #RRList() > 0
            posIterator = value.getRRList().iterator();
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

    /**
     * head send message to all previous nodes
     */
    public void sendMsgToAllPreviousNodes(ValueStateWritable value) {
        posIterator = value.getRFList().iterator(); // RFList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getRRList().iterator(); // RRList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    /**
     * start sending message
     */
    public void startSendMsg() {
        if (VertexUtil.isHeadVertex(getVertexValue())) {
            outgoingMsg.setMessage(Message.START);
            sendMsgToAllNextNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isRearVertex(getVertexValue())) {
            outgoingMsg.setMessage(Message.END);
            sendMsgToAllPreviousNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isHeadWithoutIndegree(getVertexValue())){
            outgoingMsg.setMessage(Message.START);
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
        if (VertexUtil.isRearWithoutOutdegree(getVertexValue())){
            outgoingMsg.setMessage(Message.END);
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
    }

    /**
     * initiate head, rear and path node
     */
    public void initState(Iterator<MessageWritable> msgIterator) {
        while (msgIterator.hasNext()) {
            if (!VertexUtil.isPathVertex(getVertexValue())
                    && !VertexUtil.isHeadWithoutIndegree(getVertexValue())
                    && !VertexUtil.isRearWithoutOutdegree(getVertexValue())) {
                msgIterator.next();
                voteToHalt();
            } else {
                incomingMsg = msgIterator.next();
                setState();
            }
        }
    }

    /**
     * set vertex state
     */
    public void setState() {
        if (incomingMsg.getMessage() == Message.START) {
            getVertexValue().setState(MessageFlag.IS_HEAD); //State.START_VERTEX
        } else if (incomingMsg.getMessage() == Message.END && getVertexValue().getState() != State.START_VERTEX) {
            getVertexValue().setState(MessageFlag.IS_TAIL);
            getVertexValue().setMergeChain(getVertexValue().getMergeChain());
            //voteToHalt();
        } //else
            //voteToHalt();
    }
    
    /**
     * check if A need to be flipped with successor
     */
    public boolean ifFilpWithSuccessor(){
        if(getVertexValue().getFRList().getLength() > 0)
            return true;
        else
            return false;
    }
    
    /**
     * check if A need to be filpped with predecessor
     */
    public boolean ifFlipWithPredecessor(){
        if(getVertexValue().getRFList().getLength() > 0)
            return true;
        else
            return false;
    }
    /**
     * send update message from predecessor
     */
    public void sendUpMsgFromPredecessor(){
        outFlag |= MessageFlag.FROM_PREDECESSOR;
        outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
        if(ifFlipWithPredecessor())
            outFlag |= MessageFlag.FLIP;
        outgoingMsg.setMessage(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
        outUpFlag = (byte)(MessageFlag.FROM_DEADVERTEX | MessageFlag.FROM_SUCCESSOR);
        outgoingUpMsg.setNeighberNode(getVertexValue().getOutgoingList());
        if(ifFilpWithSuccessor())
            outFlag |= MessageFlag.FLIP;
        outgoingUpMsg.setMessage(outUpFlag);
        outgoingUpMsg.setSourceVertexId(getVertexId());
        sendMsg(getPreDestVertexId(getVertexValue()), outgoingUpMsg);
        deleteVertex(getVertexId());
    }
    
    /**
     * send update message from successor
     */
    public void sendUpMsgFromSuccessor(){
        outFlag |= MessageFlag.FROM_SUCCESSOR;
        outgoingUpMsg.setNeighberNode(getVertexValue().getOutgoingList());
        if(ifFilpWithSuccessor())
            outFlag |= MessageFlag.FLIP;
        outgoingMsg.setMessage(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        sendMsg(getPreDestVertexId(getVertexValue()), outgoingMsg);
        outUpFlag = (byte)(MessageFlag.FROM_DEADVERTEX | MessageFlag.FROM_PREDECESSOR);
        outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
        if(ifFlipWithPredecessor())
            outFlag |= MessageFlag.FLIP;
        outgoingUpMsg.setMessage(outUpFlag);
        outgoingUpMsg.setSourceVertexId(getVertexId());
        sendMsg(getNextDestVertexId(getVertexValue()), outgoingUpMsg);
        deleteVertex(getVertexId());
    }
    
    /**
     * update AdjacencyList if message from predecessor 
     */
    public void updateAdjList_MsgPredecessor(){
        if((outFlag & MessageFlag.FLIP) > 0){
            if(getVertexValue().getFFList().getLength() > 0){
                getVertexValue().setFFList(null);
                if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                    getVertexValue().setFRList(incomingMsg.getNeighberNode().getForwardList());
                else
                    getVertexValue().setFRList(incomingMsg.getNeighberNode().getReverseList());
            } else {
                getVertexValue().setFRList(null);
                if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                    getVertexValue().setFFList(incomingMsg.getNeighberNode().getForwardList());
                else
                    getVertexValue().setFFList(incomingMsg.getNeighberNode().getReverseList());
            }
        } else {
            getVertexValue().setIncomingList(incomingMsg.getNeighberNode());
        }
    }
    
    /**
     * update AdjacencyList if message from successor 
     */
    public void updateAdjList_MsgSuccessor(){
        if((outFlag & MessageFlag.FLIP) > 0){
            if(getVertexValue().getRRList().getLength() > 0){
                getVertexValue().setRRList(null);
                if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                    getVertexValue().setRFList(incomingMsg.getNeighberNode().getForwardList());
                else
                    getVertexValue().setRFList(incomingMsg.getNeighberNode().getReverseList());
            } else {
                getVertexValue().setRFList(null);
                if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                    getVertexValue().setRRList(incomingMsg.getNeighberNode().getForwardList());
                else
                    getVertexValue().setRRList(incomingMsg.getNeighberNode().getReverseList());
            }
        } else {
            getVertexValue().setOutgoingList(incomingMsg.getNeighberNode());
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 2 == 1){
            // Node may be marked as head b/c it's a real head or a real tail
            headFlag = (byte) (State.START_VERTEX & getVertexValue().getState());
            tailFlag = (byte) (State.END_VERTEX & getVertexValue().getState());
            outFlag = (byte) (headFlag | tailFlag);
            
            // only PATH vertices are present. Find the ID's for my neighbors
            curID.set(getVertexId());
            
            curHead = isNodeRandomHead(curID);
            
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            hasNext = setNextInfo(getVertexValue()) && tailFlag == 0;
            hasPrev = setPrevInfo(getVertexValue()) && headFlag == 0;
            if ((outFlag & MessageFlag.IS_HEAD) > 0 && (outFlag & MessageFlag.IS_TAIL) > 0) {
                outFlag |= MessageFlag.FROM_SELF;
                getVertexValue().setState(outFlag);
                voteToHalt();
            }
            if (hasNext || hasPrev) {
                if (curHead) {
                    if (hasNext && !nextHead) {
                        // compress this head to the forward tail
                        sendUpMsgFromPredecessor();
                    } else if (hasPrev && !prevHead) {
                        // compress this head to the reverse tail
                        sendUpMsgFromSuccessor();
                    }
                } else {
                    // I'm a tail
                    if (hasNext && hasPrev) {
                        if ((!nextHead && !prevHead) && (curID.compareTo(nextID) < 0 && curID.compareTo(prevID) < 0)) {
                            // tails on both sides, and I'm the "local minimum"
                            // compress me towards the tail in forward dir
                            sendUpMsgFromPredecessor();
                        }
                    } else if (!hasPrev) {
                        // no previous node
                        if (!nextHead && curID.compareTo(nextID) < 0) {
                            // merge towards tail in forward dir
                            sendUpMsgFromPredecessor();
                        }
                    } else if (!hasNext) {
                        // no next node
                        if (!prevHead && curID.compareTo(prevID) < 0) {
                            // merge towards tail in reverse dir
                            sendUpMsgFromSuccessor();
                        }
                    }
                }
            }
        }
        else if (getSuperstep() % 2 == 0){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                outFlag = incomingMsg.getMessage();
                if((outFlag & MessageFlag.FROM_DEADVERTEX) > 0){
                    if((outFlag & MessageFlag.FROM_PREDECESSOR) > 0){
                        updateAdjList_MsgPredecessor();
                    } 
                    else {//Message from successor.
                        updateAdjList_MsgSuccessor();
                    }
                }
                else {//Not for update, but for merging
                    if((outFlag & MessageFlag.FROM_PREDECESSOR) > 0){
                        //B merge with A's reverse
                        //append A's reverse to B
                        //
                        updateAdjList_MsgPredecessor();
                    } 
                    else {//Message from successor
                        //B merge with A's reverse
                        //append 
                        updateAdjList_MsgSuccessor();
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(P4ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P4ForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
    }
}
