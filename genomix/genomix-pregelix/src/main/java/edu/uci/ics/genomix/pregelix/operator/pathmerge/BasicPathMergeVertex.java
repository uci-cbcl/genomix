package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.PositionWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.AdjMessage;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/**
 * Naive Algorithm for path merge graph
 */
public class BasicPathMergeVertex extends
        Vertex<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "BasicPathMergeVertex.kmerSize";
    public static final String ITERATIONS = "BasicPathMergeVertex.iteration";
    public static int kmerSize = -1;
    protected int maxIteration = -1;
    
    protected MessageWritable incomingMsg = new MessageWritable();
    protected MessageWritable outgoingMsg = new MessageWritable();
    protected PositionWritable destVertexId = new PositionWritable();
    protected Iterator<PositionWritable> posIterator;
    private PositionWritable pos = new PositionWritable();
    byte headFlag;
    protected byte outFlag;
    protected byte inFlag;
    protected byte selfFlag;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        outFlag = (byte)0;
        outgoingMsg.reset();
        headFlag = (byte)(getVertexValue().getState() & State.IS_HEAD);
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
     * get destination vertex
     */
    public PositionWritable getNextDestVertexId(VertexValueWritable value) {
        if (value.getFFList().getCountOfPosition() > 0){ // #FFList() > 0
            posIterator = value.getFFList().iterator();
            outFlag |= MessageFlag.DIR_FF;
            return posIterator.next();
        } else if (value.getFRList().getCountOfPosition() > 0){ // #FRList() > 0
            posIterator = value.getFRList().iterator();
            outFlag |= MessageFlag.DIR_FR;
            return posIterator.next();
        } else {
          return null;  
        }
        
    }

    public PositionWritable getPreDestVertexId(VertexValueWritable value) {
        if (value.getRFList().getCountOfPosition() > 0){ // #RFList() > 0
            posIterator = value.getRFList().iterator();
            outFlag |= MessageFlag.DIR_RF;
            return posIterator.next();
        } else if (value.getRRList().getCountOfPosition() > 0){ // #RRList() > 0
            posIterator = value.getRRList().iterator();
            outFlag |= MessageFlag.DIR_RR;
            return posIterator.next();
        } else {
            return null;
        }
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
     * one vertex send message to previous and next vertices (neighbor)
     */
    public void sendMsgToAllNeighborNodes(VertexValueWritable value){
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
     * head send message to all next nodes
     */
    public void sendSettledMsgToAllNextNodes(VertexValueWritable value) {
        posIterator = value.getFFList().iterator(); // FFList
        while(posIterator.hasNext()){
            outgoingMsg.setFlag(AdjMessage.FROMFF);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getFRList().iterator(); // FRList
        while(posIterator.hasNext()){
            outgoingMsg.setFlag(AdjMessage.FROMFR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    /**
     * head send message to all previous nodes
     */
    public void sendSettledMsgToAllPreviousNodes(VertexValueWritable value) {
        posIterator = value.getRFList().iterator(); // RFList
        while(posIterator.hasNext()){
            outgoingMsg.setFlag(AdjMessage.FROMRF);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getRRList().iterator(); // RRList
        while(posIterator.hasNext()){
            outgoingMsg.setFlag(AdjMessage.FROMRR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * start sending message
     */
    public void startSendMsg() {
        if (VertexUtil.isHeadVertexWithIndegree(getVertexValue())) {
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD | MessageFlag.SHOULD_MERGEWITHNEXT));
            sendMsgToAllNextNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isRearVertexWithOutdegree(getVertexValue())) {
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD | MessageFlag.SHOULD_MERGEWITHPREV));
            sendMsgToAllPreviousNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isHeadWithoutIndegree(getVertexValue())){
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD | MessageFlag.SHOULD_MERGEWITHNEXT));
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
        if (VertexUtil.isRearWithoutOutdegree(getVertexValue())){
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD | MessageFlag.SHOULD_MERGEWITHPREV));
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
                if(getHeadFlag() > 0)
                    voteToHalt();
                else 
                    getVertexValue().setState(incomingMsg.getFlag());
            }
        }
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
        else if(getVertexValue().getFRList().getLength() > 0)
            outFlag |= MessageFlag.DIR_FR;
        else
            outFlag |= MessageFlag.DIR_NO;
    }
    
    /**
     * set adjMessage to predecessor(from successor)
     */
    public void setPredecessorAdjMsg(){
        if(getVertexValue().getRFList().getLength() > 0)
            outFlag |= MessageFlag.DIR_RF;
        else if(getVertexValue().getRRList().getLength() > 0)
            outFlag |= MessageFlag.DIR_RR;
        else
            outFlag |= MessageFlag.DIR_NO;
    }
    
    /**
     * send update message to neighber
     * @throws IOException 
     */
    public void broadcastUpdateMsg(){
        if((getVertexValue().getState() & MessageFlag.IS_HEAD) > 0)
            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & MessageFlag.SHOULD_MERGE_MASK){
            case MessageFlag.SHOULD_MERGEWITHPREV:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outgoingMsg.setFlip(true);
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                if(getNextDestVertexId(getVertexValue()) != null)
                    sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
                break;
            case MessageFlag.SHOULD_MERGEWITHNEXT:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outgoingMsg.setFlip(true);
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                if(getPreDestVertexId(getVertexValue()) != null)
                    sendMsg(getPreDestVertexId(getVertexValue()), outgoingMsg);
                break; 
        }
    }
    
    /**
     * send merge message to neighber for P2
     * @throws IOException 
     */
    public void sendMergeMsg(){
        if(selfFlag == MessageFlag.IS_HEAD){
            byte newState = getVertexValue().getState(); 
            newState &= ~State.IS_HEAD;
            newState |= State.IS_OLDHEAD;
            getVertexValue().setState(newState);
            resetSelfFlag();
            outFlag |= MessageFlag.IS_HEAD;
        } else if(selfFlag == MessageFlag.IS_OLDHEAD){
            outFlag |= MessageFlag.IS_OLDHEAD;
            voteToHalt();
        }
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outgoingMsg.setFlip(true);
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setChainVertexId(getVertexValue().getKmer());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); //getNextDestVertexId(getVertexValue())
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outgoingMsg.setFlip(true);
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setChainVertexId(getVertexValue().getKmer());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); //getPreDestVertexId(getVertexValue())
                break; 
        }
    }
    
    /**
     * send merge message to neighber for P4
     * @throws IOException 
     */
    public void broadcastMergeMsg(){
        if(headFlag > 0)
            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK) {
            case MessageFlag.SHOULD_MERGEWITHNEXT:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outgoingMsg.setFlip(true);
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setChainVertexId(getVertexValue().getKmer());
                sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
                deleteVertex(getVertexId());
                break;
            case MessageFlag.SHOULD_MERGEWITHPREV:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outgoingMsg.setFlip(true);
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setChainVertexId(getVertexValue().getKmer());
                sendMsg(getPreDestVertexId(getVertexValue()), outgoingMsg);
                deleteVertex(getVertexId());
                break; 
        }
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to neighber
     * @throws IOException 
     */
    public void sendUpdateMsgToPredecessor(){
        byte state = getVertexValue().getState();
        state |= State.SHOULD_MERGEWITHNEXT;
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
    public void sendUpdateMsgToSuccessor(){
        byte state = getVertexValue().getState();
        state |= State.SHOULD_MERGEWITHPREV;
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
        inFlag = incomingMsg.getFlag();
        byte meToNeighborDir = (byte) (inFlag & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        byte neighborToMergeDir = flipDirection(neighborToMeDir, incomingMsg.isFlip());
        
        getVertexValue().processUpdates(neighborToMeDir, incomingMsg.getSourceVertexId(), 
                neighborToMergeDir, VertexUtil.getNodeIdFromAdjacencyList(incomingMsg.getNeighberNode()));
    }
    
    /**
     * merge and updateAdjList merge with one neighbor
     */
    public void processMerge(){
        inFlag = incomingMsg.getFlag();
        byte meToNeighborDir = (byte) (inFlag & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        if((inFlag & MessageFlag.IS_HEAD) > 0){
            byte state = getVertexValue().getState();
            state |= State.IS_HEAD;
            getVertexValue().setState(state);
        }
        
        byte neighborToMergeDir = flipDirection(neighborToMeDir, incomingMsg.isFlip());
        
        getVertexValue().processMerges(neighborToMeDir, incomingMsg.getSourceVertexId(), 
                neighborToMergeDir, VertexUtil.getNodeIdFromAdjacencyList(incomingMsg.getNeighberNode()),
                kmerSize, incomingMsg.getKmer());
    }
    
    /**
     * merge and updateAdjList  having parameter
     */
    public void processMerge(MessageWritable msg){
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);

        byte neighborToMergeDir = flipDirection(neighborToMeDir, incomingMsg.isFlip());
        
        getVertexValue().processMerges(neighborToMeDir, msg.getSourceVertexId(), 
                neighborToMergeDir, VertexUtil.getNodeIdFromAdjacencyList(msg.getNeighberNode()),
                kmerSize, msg.getKmer());
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
     * set final state
     */
    public void setStopFlag(){
        byte state = incomingMsg.getFlag();
        state &= State.VERTEX_CLEAR;
        state |= State.IS_STOP;
        getVertexValue().setState(state);
    }
    
    /**
     * get Vertex state
     */
    public byte getMsgFlag(){
        return (byte)(incomingMsg.getFlag() & MessageFlag.VERTEX_MASK);
    }
    
    /**
     * reset selfFlag
     */
    public void resetSelfFlag(){
        selfFlag =(byte)(getVertexValue().getState() & MessageFlag.VERTEX_MASK);
    }
    
    /**
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertex(Iterator<MessageWritable> msgIterator){
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(incomingMsg.getFlag() == AdjMessage.FROMFF){
                //remove incomingMsg.getSourceId from RR positionList
                posIterator = getVertexValue().getRRList().iterator();
                while(posIterator.hasNext()){
                    pos = posIterator.next();
                    if(pos.equals(incomingMsg.getSourceVertexId())){
                        posIterator.remove();
                        break;
                    }
                }
            } else if(incomingMsg.getFlag() == AdjMessage.FROMFR){
                //remove incomingMsg.getSourceId from FR positionList
                posIterator = getVertexValue().getFRList().iterator();
                while(posIterator.hasNext()){
                    pos = posIterator.next();
                    if(pos.equals(incomingMsg.getSourceVertexId())){
                        posIterator.remove();
                        break;
                    }
                }
            } else if(incomingMsg.getFlag() == AdjMessage.FROMRF){
                //remove incomingMsg.getSourceId from RF positionList
                posIterator = getVertexValue().getRFList().iterator();
                while(posIterator.hasNext()){
                    pos = posIterator.next();
                    if(pos.equals(incomingMsg.getSourceVertexId())){
                        posIterator.remove();
                        break;
                    }
                }
            } else{ //incomingMsg.getFlag() == AdjMessage.FROMRR
                //remove incomingMsg.getSourceId from FF positionList
                posIterator = getVertexValue().getFFList().iterator();
                while(posIterator.hasNext()){
                    pos = posIterator.next();
                    if(pos.equals(incomingMsg.getSourceVertexId())){
                        posIterator.remove();
                        break;
                    }
                }
            }
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
    }
}