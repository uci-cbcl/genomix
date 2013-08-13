package edu.uci.ics.genomix.pregelix.operator;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public abstract class BasicGraphCleanVertex<M extends MessageWritable> extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, M> {
    public static final String KMER_SIZE = "BasicGraphCleanVertex.kmerSize";
    public static final String ITERATIONS = "BasicGraphCleanVertex.iteration";
    public static int kmerSize = -1;
    public static int maxIteration = -1;
    
    protected M incomingMsg = null; 
    protected M outgoingMsg = null; 
    protected VKmerBytesWritable destVertexId = null;
    protected Iterator<VKmerBytesWritable> kmerIterator;
    protected VKmerBytesWritable tmpKmer = null;
    protected byte headFlag;
    protected byte outFlag;
    protected byte inFlag;
    protected byte selfFlag;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
    }
    
    /**
     * reset headFlag
     */
    public void resetHeadFlag(){
        headFlag = (byte)(getVertexValue().getState() & State.IS_HEAD);
    }
    
    public byte getHeadFlag(){
        return (byte)(getVertexValue().getState() & State.IS_HEAD);
    }
    
    /**
     * reset selfFlag
     */
    public void resetSelfFlag(){
        selfFlag =(byte)(getVertexValue().getState() & MessageFlag.VERTEX_MASK);
    }
    
    /**
     * get Vertex state
     */
    public byte getMsgFlag(){
        return (byte)(incomingMsg.getFlag() & MessageFlag.VERTEX_MASK);
    }
    
    /**
     * set head state
     */
    public void setHeadState(){
        byte state = getVertexValue().getState();
        state &= State.VERTEX_CLEAR;
        state |= State.IS_HEAD;
        getVertexValue().setState(state);
    }
    
    /**
     * set final state
     */
    public void setFinalState(){
        byte state = getVertexValue().getState();
        state &= State.VERTEX_CLEAR;
        state |= State.IS_FINAL;
        getVertexValue().setState(state);
    }
    
    /**
     * set stop flag
     */
    public void setStopFlag(){
        byte state = getVertexValue().getState();
        state &= State.VERTEX_CLEAR;
        state |= State.IS_FINAL;
        getVertexValue().setState(state);
    }
    
    /**
     * check the message type
     */
    public boolean isReceiveKillMsg(){
        byte killFlag = (byte) (incomingMsg.getFlag() & MessageFlag.KILL_MASK);
        byte deadFlag = (byte) (incomingMsg.getFlag() & MessageFlag.DEAD_MASK);
        return killFlag == MessageFlag.KILL & deadFlag != MessageFlag.DIR_FROM_DEADVERTEX;
    }
    
    public boolean isResponseKillMsg(){
        byte killFlag = (byte) (incomingMsg.getFlag() & MessageFlag.KILL_MASK);
        byte deadFlag = (byte) (incomingMsg.getFlag() & MessageFlag.DEAD_MASK);
        return killFlag == MessageFlag.KILL & deadFlag == MessageFlag.DIR_FROM_DEADVERTEX; 
    }
    
    /**
     * get destination vertex
     */
    public VKmerBytesWritable getPrevDestVertexId() {
        if (!getVertexValue().getRFList().isEmpty()){ //#RFList() > 0
            kmerIterator = getVertexValue().getRFList().getKeys();
            return kmerIterator.next();
        } else if (!getVertexValue().getRRList().isEmpty()){ //#RRList() > 0
            kmerIterator = getVertexValue().getRRList().getKeys();
            return kmerIterator.next();
        } else {
            return null;
        }
    }
    
    public VKmerBytesWritable getNextDestVertexId() {
        if (!getVertexValue().getFFList().isEmpty()){ //#FFList() > 0
            kmerIterator = getVertexValue().getFFList().getKeys();
            return kmerIterator.next();
        } else if (!getVertexValue().getFRList().isEmpty()){ //#FRList() > 0
            kmerIterator = getVertexValue().getFRList().getKeys();
            return kmerIterator.next();
        } else {
            return null;  
        }
    }

    /**
     * get destination vertex
     */
    public VKmerBytesWritable getPrevDestVertexIdAndSetFlag() {
        if (!getVertexValue().getRFList().isEmpty()){ // #RFList() > 0
            kmerIterator = getVertexValue().getRFList().getKeys();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RF;
            return kmerIterator.next();
        } else if (!getVertexValue().getRRList().isEmpty()){ // #RRList() > 0
            kmerIterator = getVertexValue().getRRList().getKeys();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RR;
            return kmerIterator.next();
        } else {
            return null;
        }
    }
    
    public VKmerBytesWritable getNextDestVertexIdAndSetFlag() {
        if (!getVertexValue().getFFList().isEmpty()){ // #FFList() > 0
            kmerIterator = getVertexValue().getFFList().getKeys();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FF;
            return kmerIterator.next();
        } else if (!getVertexValue().getFRList().isEmpty()){ // #FRList() > 0
            kmerIterator = getVertexValue().getFRList().getKeys();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FR;
            return kmerIterator.next();
        } else {
          return null;  
        }
        
    }

    /**
     * head send message to all previous nodes
     */
    public void sendMsgToAllPreviousNodes() {
        kmerIterator = getVertexValue().getRFList().getKeys(); // RFList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getRRList().getKeys(); // RRList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes() {
        kmerIterator = getVertexValue().getFFList().getKeys(); // FFList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getFRList().getKeys(); // FRList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    /**
     * one vertex send message to previous and next vertices (neighbor)
     */
    public void sendMsgToAllNeighborNodes(){
        sendMsgToAllNextNodes();
        sendMsgToAllPreviousNodes();
    }
    
    /**
     * tip send message with sourceId and dir to previous node 
     * tip only has one incoming
     */
    public void sendSettledMsgToPrevNode(){
        if(getVertexValue().hasPrevDest()){
            if(!getVertexValue().getRFList().isEmpty())
                outgoingMsg.setFlag(MessageFlag.DIR_RF);
            else if(!getVertexValue().getRRList().isEmpty())
                outgoingMsg.setFlag(MessageFlag.DIR_RR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(getPrevDestVertexId());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * tip send message with sourceId and dir to next node 
     * tip only has one outgoing
     */
    public void sendSettledMsgToNextNode(){
        if(getVertexValue().hasNextDest()){
            if(!getVertexValue().getFFList().isEmpty())
                outgoingMsg.setFlag(MessageFlag.DIR_FF);
            else if(!getVertexValue().getFRList().isEmpty())
                outgoingMsg.setFlag(MessageFlag.DIR_FR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(getNextDestVertexId());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * head send message to all previous nodes
     */
    public void sendSettledMsgToAllPreviousNodes() {
        kmerIterator = getVertexValue().getRFList().getKeys(); // RFList
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RF;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getRRList().getKeys(); // RRList
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RR;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * head send message to all next nodes
     */
    public void sendSettledMsgToAllNextNodes() {
        kmerIterator = getVertexValue().getFFList().getKeys(); // FFList
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FF;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getFRList().getKeys(); // FRList
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FR;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    public void sendSettledMsgToAllNeighborNodes() {
        sendSettledMsgToAllPreviousNodes();
        sendSettledMsgToAllNextNodes();
    }
    
    /**
     * start sending message
     */
    public void startSendMsg() {
        if (VertexUtil.isHeadVertexWithIndegree(getVertexValue())) {
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD));
            sendMsgToAllNextNodes();
            voteToHalt();
        }
        if (VertexUtil.isRearVertexWithOutdegree(getVertexValue())) {
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD));
            sendMsgToAllPreviousNodes();
            voteToHalt();
        }
        if (VertexUtil.isHeadWithoutIndegree(getVertexValue())){
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD));
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
        if (VertexUtil.isRearWithoutOutdegree(getVertexValue())){
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD));
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
    }

    /**
     * initiate head, rear and path node
     */
    public void initState(Iterator<M> msgIterator) {
        while (msgIterator.hasNext()) {
            if (!VertexUtil.isPathVertex(getVertexValue())
                    && !VertexUtil.isHeadWithoutIndegree(getVertexValue())
                    && !VertexUtil.isRearWithoutOutdegree(getVertexValue())) {
                msgIterator.next();
                voteToHalt();
            } else {
                incomingMsg = msgIterator.next();
                if(getHeadFlag() > 0)
                    voteToHalt();
                else 
                    getVertexValue().setState(incomingMsg.getFlag());
            }
        }
    }
    
    /**
     * check if A need to be filpped with predecessor
     */
    public boolean ifFlipWithPredecessor(){
        if(!getVertexValue().getRFList().isEmpty())
            return true;
        else
            return false;
    }
    
    /**
     * check if A need to be flipped with successor
     */
    public boolean ifFilpWithSuccessor(){
        if(!getVertexValue().getFRList().isEmpty())
            return true;
        else
            return false;
    }
    
    /**
     * set adjMessage to predecessor(from successor)
     */
    public void setPredecessorAdjMsg(){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(!getVertexValue().getRFList().isEmpty())
            outFlag |= MessageFlag.DIR_RF;
        else if(!getVertexValue().getRRList().isEmpty())
            outFlag |= MessageFlag.DIR_RR;
    }
    
    /**
     * set adjMessage to successor(from predecessor)
     */
    public void setSuccessorAdjMsg(){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(!getVertexValue().getFFList().isEmpty())
            outFlag |= MessageFlag.DIR_FF;
        else if(!getVertexValue().getFRList().isEmpty())
            outFlag |= MessageFlag.DIR_FR;
    }
    
    /**
     * set state as no_merge
     */
    public void setStateAsNoMerge(){
    	byte state = getVertexValue().getState();
    	//state |= State.SHOULD_MERGE_CLEAR;
        state |= State.NO_MERGE;
        getVertexValue().setState(state);
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
     * broadcast kill self to all neighbers  Pre-condition: vertex is a path vertex ***
     */
    public void broadcaseKillself(){
        outFlag = 0;
        outFlag |= MessageFlag.KILL;
        outFlag |= MessageFlag.DIR_FROM_DEADVERTEX;
        
        sendSettledMsgToAllNeighborNodes();
        
        deleteVertex(getVertexId());
        this.activate();
    }
    
    /**
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertex(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        getVertexValue().getEdgeList(neighborToMeDir).remove(incomingMsg.getSourceVertexId());
    }
    
    @Override
    public void compute(Iterator<M> msgIterator) {
        
    }
}