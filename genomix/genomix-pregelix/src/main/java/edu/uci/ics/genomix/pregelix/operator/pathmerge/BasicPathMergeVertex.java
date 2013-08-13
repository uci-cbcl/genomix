package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;

public abstract class BasicPathMergeVertex extends
	BasicGraphCleanVertex<PathMergeMessageWritable>{
	
    public void setStateAsMergeWithPrev(){
        byte state = getVertexValue().getState();
        state &= State.SHOULD_MERGE_CLEAR;
        state |= State.SHOULD_MERGEWITHPREV;
        getVertexValue().setState(state);
    }
    
    public void setStateAsMergeWithNext(){
        byte state = getVertexValue().getState();
        state &= State.SHOULD_MERGE_CLEAR;
        state |= State.SHOULD_MERGEWITHNEXT;
        getVertexValue().setState(state);
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
                neighborToMergeDir, incomingMsg.getNeighborEdge());
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
                neighborToMergeDir, incomingMsg.getNeighborEdge(),
                kmerSize, incomingMsg.getInternalKmer());
    }
    
    /**
     * merge and updateAdjList  having parameter
     */
    public void processMerge(PathMergeMessageWritable msg){
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);

        byte neighborToMergeDir = flipDirection(neighborToMeDir, msg.isFlip());
        
        getVertexValue().processMerges(neighborToMeDir, msg.getSourceVertexId(), 
                neighborToMergeDir, msg.getNeighborEdge(),
                kmerSize, msg.getInternalKmer());
    }
    
    /**
     * final merge and updateAdjList  having parameter for p2
     */
    public void processFinalMerge(PathMergeMessageWritable msg){
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);

        byte neighborToMergeDir = flipDirection(neighborToMeDir, msg.isFlip());
        
        String selfString;
        String match;
        String msgString;
        int index;
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
                selfString = getVertexValue().getInternalKmer().toString();
                match = selfString.substring(selfString.length() - kmerSize + 1,selfString.length()); 
                msgString = msg.getInternalKmer().toString();
                index = msgString.indexOf(match);
                tmpKmer.setByRead(msgString.length() - index, msgString.substring(index).getBytes(), 0);
                break;
            case MessageFlag.DIR_FR:
                selfString = getVertexValue().getInternalKmer().toString();
                match = selfString.substring(selfString.length() - kmerSize + 1,selfString.length()); 
                msgString = GeneCode.reverseComplement(msg.getInternalKmer().toString());
                index = msgString.indexOf(match);
                tmpKmer.setByReadReverse(msgString.length() - index, msgString.substring(index).getBytes(), 0);
                break;
            case MessageFlag.DIR_RF:
                selfString = getVertexValue().getInternalKmer().toString();
                match = selfString.substring(0, kmerSize - 1); 
                msgString = GeneCode.reverseComplement(msg.getInternalKmer().toString());
                index = msgString.lastIndexOf(match) + kmerSize - 2;
                tmpKmer.setByReadReverse(index + 1, msgString.substring(0, index + 1).getBytes(), 0);
                break;
            case MessageFlag.DIR_RR:
                selfString = getVertexValue().getInternalKmer().toString();
                match = selfString.substring(0, kmerSize - 1); 
                msgString = msg.getInternalKmer().toString();
                index = msgString.lastIndexOf(match) + kmerSize - 2;
                tmpKmer.setByRead(index + 1, msgString.substring(0, index + 1).getBytes(), 0);
                break;
        }
       
        getVertexValue().processMerges(neighborToMeDir, msg.getSourceVertexId(), 
                neighborToMergeDir, msg.getNeighborEdge(),
                kmerSize, tmpKmer);
    }
    
    /**
     * configure UPDATE msg
     */
    public void configureUpdateMsgForPredecessor(){
        if(getPrevDestVertexId() != null){
            setPredecessorAdjMsg();
            if(ifFilpWithSuccessor())
                outgoingMsg.setFlip(true);
            outgoingMsg.setFlag(outFlag);
            for(byte d: OutgoingListFlag.values)
                outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
            outgoingMsg.setSourceVertexId(getVertexId());
            sendMsg(getPrevDestVertexId(), outgoingMsg);
        }
    }
    
    public void configureUpdateMsgForSuccessor(){
        if(getNextDestVertexId() != null){
            setSuccessorAdjMsg();
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setFlip(ifFlipWithPredecessor());  
            for(byte d: IncomingListFlag.values)
                outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
            sendMsg(getNextDestVertexId(), outgoingMsg);
        }
    }
	/**
     * send update message to neighber
     */
    public void broadcastUpdateMsg(){
        if((getVertexValue().getState() & State.IS_HEAD) > 0)
            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK){
            case State.SHOULD_MERGEWITHPREV:
                /** confugure updateMsg for successor **/
                configureUpdateMsgForSuccessor();
                break;
            case State.SHOULD_MERGEWITHNEXT:
                /** confugure updateMsg for predecessor **/
                configureUpdateMsgForPredecessor();
                break; 
        }
    }

    /**
     * This vertex tries to merge with next vertex and send update msg to predecesspr
     */
    public void sendUpdateMsgToPredecessor(){
        if(getVertexValue().hasNextDest()){
            setStateAsMergeWithNext();
            broadcastUpdateMsg();   
        }
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to successor
     */
    public void sendUpdateMsgToSuccessor(){
        if(getVertexValue().hasPrevDest()){
            setStateAsMergeWithPrev();
            broadcastUpdateMsg();
        }
    }
    
    /**
     * send update message to neighber for P2
     */
    public void sendUpdateMsg(MessageWritable msg){
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(true);
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                sendUpdateMsgToPredecessor();
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR: 
                sendUpdateMsgToSuccessor();
                break;
        }
    }
    
    public void sendMergeMsgToSuccessor(){
        setSuccessorAdjMsg();
        if(ifFlipWithPredecessor())
            outgoingMsg.setFlip(true);
        else
            outgoingMsg.setFlip(false);
        outgoingMsg.setFlag(outFlag);
//        outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
        for(byte d: IncomingListFlag.values)
        	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
        sendMsg(getNextDestVertexId(), outgoingMsg);
    }
    
    public boolean canMergeWithHead(MessageWritable msg){
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                return getVertexValue().outDegree() == 1;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                return getVertexValue().inDegree() == 1;    
        }
        return false;
    }
    /**
     * send merge message to neighber for P1
     * @throws IOException 
     */
    public void sendMergeMsgForP1(MessageWritable msg){
        outgoingMsg.setUpdateMsg(false);
        if (getHeadFlag() > 0)//is_tail, for P1
            outFlag |= MessageFlag.STOP;
        sendMergeMsgByIncomingMsgDir();
    }
    
    public void sendMergeMsgByIncomingMsgDir(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                configureMergeMsgForSuccessor(); //sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); 
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                configureMergeMsgForPredecessor(); //sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); 
                break; 
        }
    }
    
    /**
     * send merge message to neighber for P2
     * @throws IOException 
     */
    public void sendMergeMsg(){
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(false);
        if(selfFlag == State.IS_HEAD){
            byte newState = getVertexValue().getState(); 
            newState &= ~State.IS_HEAD;
            newState |= State.IS_OLDHEAD;
            getVertexValue().setState(newState);
            resetSelfFlag();
            outFlag |= MessageFlag.IS_HEAD;  
        } else if(selfFlag == State.IS_OLDHEAD){
            outFlag |= MessageFlag.IS_OLDHEAD;
            voteToHalt();
        }
        sendMergeMsgByIncomingMsgDir();
    }
    
    /**
     * send final merge message to neighber for P2
     */
    public void sendFinalMergeMsg(){
        outFlag |= MessageFlag.IS_FINAL;
        sendMergeMsgByIncomingMsgDir();
    }
    /**
     * send merge message to neighber for P1
     */
    public void responseMergeMsgToPrevNode(){
        outFlag = 0;
        if (getVertexValue().getState() == State.IS_HEAD)//is_tail
            outFlag |= MessageFlag.STOP;
        configureMergeMsgForPredecessor();
        deleteVertex(getVertexId());
    }
    
    /**
     *  for P1
     */
    public void responseMergeMsgToNextNode(){
        outFlag = 0;
        if (getVertexValue().getState() == State.IS_HEAD)//is_tail
            outFlag |= MessageFlag.STOP;
        configureMergeMsgForSuccessor();
        deleteVertex(getVertexId());
    }
    
    /**
     * configure MERGE msg
     */
    public void configureMergeMsgForPredecessor(){
        setPredecessorAdjMsg();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setFlip(ifFilpWithSuccessor());
        for(byte d: OutgoingListFlag.values)
            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
        sendMsg(getPrevDestVertexId(), outgoingMsg);
//        deleteVertex(getVertexId());
    }
    
    public void configureMergeMsgForSuccessor(){
        setSuccessorAdjMsg();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setFlip(ifFlipWithPredecessor());
        for(byte d: IncomingListFlag.values)
            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
        sendMsg(getNextDestVertexId(), outgoingMsg);
//        deleteVertex(getVertexId());
    }
    /**
     * send merge message to neighber for P4
     */
    public void broadcastMergeMsg(){
        if(headFlag > 0)
            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK) {
            case State.SHOULD_MERGEWITHNEXT:
                /** configure merge msg for successor **/
                configureMergeMsgForSuccessor();
                deleteVertex(getVertexId());
                break;
            case State.SHOULD_MERGEWITHPREV:
                /** configure merge msg for predecessor **/
                configureMergeMsgForPredecessor();
                deleteVertex(getVertexId());
                break; 
        }
    }
}
