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
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.AdjMessage;
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
        Vertex<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
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
    protected boolean setNextInfo(VertexValueWritable value) {
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
    protected boolean setPrevInfo(VertexValueWritable value) {
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
    public PositionWritable getNextDestVertexId(VertexValueWritable value) {
        if(value.getFFList().getCountOfPosition() > 0) // #FFList() > 0
            posIterator = value.getFFList().iterator();
        else // #FRList() > 0
            posIterator = value.getFRList().iterator();
        return posIterator.next();
    }

    public PositionWritable getPreDestVertexId(VertexValueWritable value) {
        if(value.getRFList().getCountOfPosition() > 0) // #RFList() > 0
            posIterator = value.getRFList().iterator();
        else // #RRList() > 0
            posIterator = value.getRRList().iterator();
        return posIterator.next();
    }

    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes(VertexValueWritable value) {
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
    public void sendMsgToAllPreviousNodes(VertexValueWritable value) {
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
     * set adjMessage to successor(from predecessor)
     */
    public void setSuccessorAdjMsg(){
        if(getVertexValue().getFFList().getLength() > 0)
            outgoingMsg.setAdjMessage(AdjMessage.FROMFF);
        else
            outgoingMsg.setAdjMessage(AdjMessage.FROMFR);
    }
    
    /**
     * set adjMessage to predecessor(from successor)
     */
    public void setPredecessorAdjMsg(){
        if(getVertexValue().getRFList().getLength() > 0)
            outgoingMsg.setAdjMessage(AdjMessage.FROMRF);
        else
            outgoingMsg.setAdjMessage(AdjMessage.FROMRR);
    }
    
    /**
     * send update message to neighber
     */
    public void broadcastUpdateMsg(){
        outFlag |= MessageFlag.FROM_PREDECESSOR;
        outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
        if(ifFlipWithPredecessor())
            outFlag |= MessageFlag.FLIP;
        outgoingMsg.setMessage(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        setSuccessorAdjMsg();
        sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
        outUpFlag = (byte)(MessageFlag.FROM_SUCCESSOR);
        outgoingUpMsg.setNeighberNode(getVertexValue().getOutgoingList());
        if(ifFilpWithSuccessor())
            outFlag |= MessageFlag.FLIP;
        outgoingUpMsg.setMessage(outUpFlag);
        outgoingUpMsg.setSourceVertexId(getVertexId());
        setPredecessorAdjMsg();
        sendMsg(getPreDestVertexId(getVertexValue()), outgoingUpMsg);
        //remove its own neighbers
        getVertexValue().setIncomingList(null);
        getVertexValue().setOutgoingList(null);
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to neighber
     */
    public void sendUpMsgFromPredecessor(){
        getVertexValue().setState(MessageFlag.SHOULD_MERGEWITHNEXT);
        if(getVertexValue().getFFList().getLength() > 0)
            getVertexValue().setMergeDest(getVertexValue().getFFList().getPosition(0));
        else
            getVertexValue().setMergeDest(getVertexValue().getFRList().getPosition(0));
        broadcastUpdateMsg();
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to neighber
     */
    public void sendUpMsgFromSuccessor(){
        getVertexValue().setState(MessageFlag.SHOULD_MERGEWITHPREV);
        if(getVertexValue().getRFList().getLength() > 0)
            getVertexValue().setMergeDest(getVertexValue().getRFList().getPosition(0));
        else
            getVertexValue().setMergeDest(getVertexValue().getRRList().getPosition(0));
        broadcastUpdateMsg();
    }
    
    /**
     * updateAdjList
     */
    public void updateAdjList(){
        if(incomingMsg.getAdjMessage() == AdjMessage.FROMFF){
            getVertexValue().setRRList(null); //may replace setNull with remove 
            if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                getVertexValue().setRFList(incomingMsg.getNeighberNode().getForwardList());
            else
                getVertexValue().setRFList(incomingMsg.getNeighberNode().getReverseList());
        } else if(incomingMsg.getAdjMessage() == AdjMessage.FROMFR){
            getVertexValue().setFRList(null); //may replace setNull with remove 
            if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                getVertexValue().setFFList(incomingMsg.getNeighberNode().getForwardList());
            else
                getVertexValue().setFFList(incomingMsg.getNeighberNode().getReverseList());
        } else if(incomingMsg.getAdjMessage() == AdjMessage.FROMRF){
            getVertexValue().setRFList(null); //may replace setNull with remove 
            if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                getVertexValue().setRRList(incomingMsg.getNeighberNode().getForwardList());
            else
                getVertexValue().setRRList(incomingMsg.getNeighberNode().getReverseList());
        } else if(incomingMsg.getAdjMessage() == AdjMessage.FROMRR){
            getVertexValue().setFFList(null); //may replace setNull with remove 
            if(incomingMsg.getNeighberNode().getForwardList().getLength() > 0)
                getVertexValue().setFRList(incomingMsg.getNeighberNode().getForwardList());
            else
                getVertexValue().setFRList(incomingMsg.getNeighberNode().getReverseList());
        }
    }
    
    /**
     * update AdjacencyList if message from predecessor 
     */
    public void updateAdjList_MsgPredecessor(){
        if((outFlag & MessageFlag.FLIP) > 0){
            updateAdjList();
        } else {
            getVertexValue().setIncomingList(incomingMsg.getNeighberNode());
        }
    }
    
    /**
     * update AdjacencyList if message from successor 
     */
    public void updateAdjList_MsgSuccessor(){
        if((outFlag & MessageFlag.FLIP) > 0){
            updateAdjList();
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
        else if (getSuperstep() % 4 == 3){
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
        else if (getSuperstep() % 4 == 0){
            //update neighber
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                outFlag = incomingMsg.getMessage();
                if((outFlag & MessageFlag.FROM_PREDECESSOR) > 0){
                    updateAdjList_MsgPredecessor();
                } 
                else {//Message from successor.
                    updateAdjList_MsgSuccessor();
                }
            }
        } else if (getSuperstep() % 4 == 1){
            //send message to the merge object and kill self
            if((getVertexValue().getState() | MessageFlag.SHOULD_MERGEWITHNEXT) > 0){
                setSuccessorAdjMsg();
                outgoingMsg.setChainVertexId(getVertexValue().getMergeChain());
                sendMsg(getVertexValue().getMergeDest(), outgoingMsg);
                deleteVertex(getVertexId());
            } else if((getVertexValue().getState() | MessageFlag.SHOULD_MERGEWITHPREV) > 0){
                setPredecessorAdjMsg();
                outgoingMsg.setChainVertexId(getVertexValue().getMergeChain());
                sendMsg(getVertexValue().getMergeDest(), outgoingMsg);
                deleteVertex(getVertexId());
            }
        } else if (getSuperstep() % 4 == 2){
            //merge kmer
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                outFlag = incomingMsg.getMessage();
                if(outFlag == AdjMessage.FROMFF){
                    //mergeWithRR(incomingMsg.getChain())
                }
                else if(outFlag == AdjMessage.FROMFR){
                  //mergeWithRF(incomingMsg.getChain())
                }
                else if(outFlag == AdjMessage.FROMRF){
                  //mergeWithFR(incomingMsg.getChain())
                }
                else if(outFlag == AdjMessage.FROMRR){
                  //mergeWithFF(incomingMsg.getChain())
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
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
