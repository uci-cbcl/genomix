package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;


import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
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
public class P5ForPathMergeVertex extends
        Vertex<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "P5ForPathMergeVertex.kmerSize";
    public static final String ITERATIONS = "P5ForPathMergeVertex.iteration";
    public static final String RANDSEED = "P5ForPathMergeVertex.randSeed";
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
    
    private MessageWritable incomingMsg = new MessageWritable();
    private MessageWritable outgoingMsg = new MessageWritable();
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
            nextID.setAsCopy(value.getFFList().getPosition(0));
            nextHead = isNodeRandomHead(nextID);
            return true;
        }
        if (value.getFRList().getCountOfPosition() > 0) {
            nextID.setAsCopy(value.getFRList().getPosition(0));
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
            prevID.setAsCopy(value.getRRList().getPosition(0));
            prevHead = isNodeRandomHead(prevID);
            return true;
        }
        if (value.getRFList().getCountOfPosition() > 0) {
            prevID.setAsCopy(value.getRFList().getPosition(0));
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
        if (VertexUtil.isHeadVertexWithIndegree(getVertexValue())) {
            outgoingMsg.setFlag(Message.START);
            sendMsgToAllNextNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isRearVertexWithOutdegree(getVertexValue())) {
            outgoingMsg.setFlag(Message.END);
            sendMsgToAllPreviousNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isHeadWithoutIndegree(getVertexValue())){
            outgoingMsg.setFlag(Message.START);
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
        if (VertexUtil.isRearWithoutOutdegree(getVertexValue())){
            outgoingMsg.setFlag(Message.END);
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
        if (incomingMsg.getFlag() == Message.START) {
            getVertexValue().setState(MessageFlag.IS_HEAD); //State.START_VERTEX
        } else if (incomingMsg.getFlag() == Message.END && getVertexValue().getState() != State.IS_HEAD) {
            getVertexValue().setState(MessageFlag.IS_HEAD);
            getVertexValue().setKmer(getVertexValue().getKmer());
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
            outFlag |= MessageFlag.DIR_FF;
        else
            outFlag |= MessageFlag.DIR_FR;
    }
    
    /**
     * set adjMessage to predecessor(from successor)
     */
    public void setPredecessorAdjMsg(){
        if(getVertexValue().getRFList().getLength() > 0)
            outFlag |= MessageFlag.DIR_RF;
        else
            outFlag |= MessageFlag.DIR_RF;
    }
    
    /**
     * send update message to neighber
     * @throws IOException 
     */
    public void broadcastUpdateMsg(){
       /* switch(getVertexValue().getState() & 0b0001){
            case MessageFlag.SHOULD_MERGEWITHPREV:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outFlag |= MessageFlag.FLIP;
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
                break;
            case MessageFlag.SHOULD_MERGEWITHNEXT:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outFlag |= MessageFlag.FLIP;
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(getPreDestVertexId(getVertexValue()), outgoingMsg);
                break; 
        }*/
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to neighber
     * @throws IOException 
     */
    public void sendUpMsgFromPredecessor(){
        byte state = getVertexValue().getState();
        state |= MessageFlag.SHOULD_MERGEWITHNEXT;
        getVertexValue().setState(state);
        if(getVertexValue().getFFList().getLength() > 0)
            getVertexValue().setMergeDest(getVertexValue().getFFList().getPosition(0));
        else
            getVertexValue().setMergeDest(getVertexValue().getFRList().getPosition(0));
        broadcastUpdateMsg();
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to neighber
     * @throws IOException 
     */
    public void sendUpMsgFromSuccessor(){
        byte state = getVertexValue().getState();
        state |= MessageFlag.SHOULD_MERGEWITHPREV;
        getVertexValue().setState(state);
        if(getVertexValue().getRFList().getLength() > 0)
            getVertexValue().setMergeDest(getVertexValue().getRFList().getPosition(0));
        else
            getVertexValue().setMergeDest(getVertexValue().getRRList().getPosition(0));
        broadcastUpdateMsg();
    }
    
    /**
     * Returns the edge dir for B->A when the A->B edge is type @dir
     */
    public byte mirrorDirection(byte dir) {
        switch (dir) {
            case MessageFlag.DIR_FF:
                return MessageFlag.DIR_RR;
            case MessageFlag.DIR_FR:
                return MessageFlag.DIR_FR;
            case MessageFlag.DIR_RF:
                return MessageFlag.DIR_RF;
            case MessageFlag.DIR_RR:
                return MessageFlag.DIR_FF;
            default:
                throw new RuntimeException("Unrecognized direction in flipDirection: " + dir);
        }
    }
    
    /**
     * check if need filp
     */
    public byte flipDirection(byte neighborDir, boolean flip){
        if(flip){
            switch (neighborDir) {
                case MessageFlag.DIR_FF:
                    return MessageFlag.DIR_FR;
                case MessageFlag.DIR_FR:
                    return MessageFlag.DIR_FF;
                case MessageFlag.DIR_RF:
                    return MessageFlag.DIR_RR;
                case MessageFlag.DIR_RR:
                    return MessageFlag.DIR_RF;
                default:
                    throw new RuntimeException("Unrecognized direction for neighborDir: " + neighborDir);
            }
        } else 
            return neighborDir;
    }
    
    /**
     * updateAdjList
     */
    public void processUpdate(){
        /*byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        boolean flip;
        if((outFlag & MessageFlag.FLIP) > 0)
            flip = true;
        else
            flip = false;
        byte neighborToMergeDir = flipDirection(neighborToMeDir, flip);
        
        getVertexValue().processUpdates(neighborToMeDir, incomingMsg.getSourceVertexId(), 
                neighborToMergeDir, VertexUtil.getNodeIdFromAdjacencyList(incomingMsg.getNeighberNode()));*/
    }
    
    /**
     * merge and updateAdjList
     */
    public void processMerge(){
        /*byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        boolean flip;
        if((outFlag & MessageFlag.FLIP) > 0)
            flip = true;
        else
            flip = false;
        byte neighborToMergeDir = flipDirection(neighborToMeDir, flip);
        
        getVertexValue().processMerges(neighborToMeDir, incomingMsg.getSourceVertexId(), 
                neighborToMergeDir, VertexUtil.getNodeIdFromAdjacencyList(incomingMsg.getNeighberNode()),
                kmerSize, incomingMsg.getKmer());*/
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
            headFlag = (byte) (State.IS_HEAD & getVertexValue().getState());
            tailFlag = (byte) (State.IS_HEAD & getVertexValue().getState()); //is_tail
            outFlag = (byte) (headFlag | tailFlag);
            
            // only PATH vertices are present. Find the ID's for my neighbors
            curID.set(getVertexId());
            
            curHead = isNodeRandomHead(curID);
            
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            hasNext = setNextInfo(getVertexValue()) && tailFlag == 0;
            hasPrev = setPrevInfo(getVertexValue()) && headFlag == 0;
            if ((outFlag & MessageFlag.IS_HEAD) > 0 && (outFlag & MessageFlag.IS_HEAD) > 0) {
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
      
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(P5ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P5ForPathMergeVertex.class);
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
