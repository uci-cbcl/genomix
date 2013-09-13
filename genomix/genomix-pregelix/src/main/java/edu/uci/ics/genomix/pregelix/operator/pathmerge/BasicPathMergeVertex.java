package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.log.LogUtil;
import edu.uci.ics.genomix.pregelix.log.LoggingType;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public abstract class BasicPathMergeVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
	BasicGraphCleanVertex<V, M>{
	
    public void setStateAsMergeWithPrev(){
        byte state = getVertexValue().getState();
        state &= State.SHOULD_MERGE_CLEAR;
        state |= State.SHOULD_MERGEWITHPREV;
        getVertexValue().setState(state);
        activate();
    }
    
    public void setStateAsMergeWithNext(){
        byte state = getVertexValue().getState();
        state &= State.SHOULD_MERGE_CLEAR;
        state |= State.SHOULD_MERGEWITHNEXT;
        getVertexValue().setState(state);
        activate();
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
                neighborToMergeDir, incomingMsg.getNode());
    }
    
    /**
     * final updateAdjList
     */
    public void processFinalUpdate(){
        inFlag = incomingMsg.getFlag();
        byte meToNeighborDir = (byte) (inFlag & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        byte neighborToMergeDir = flipDirection(neighborToMeDir, incomingMsg.isFlip());
        getVertexValue().processFinalUpdates(neighborToMeDir, neighborToMergeDir, incomingMsg.getNode());
    }
    
    /**
     * final updateAdjList
     */
    public void processFinalUpdate2(){
        inFlag = incomingMsg.getFlag();
        byte meToNeighborDir = (byte) (inFlag & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        EdgeWritable edge = new EdgeWritable();
        edge.setKey(incomingMsg.getSourceVertexId());
        edge.setReadIDs(incomingMsg.getNode().getEdgeList(meToNeighborDir).getReadIDs(getVertexId()));
        getVertexValue().getEdgeList(neighborToMeDir).unionAdd(edge);
    }
    
    /**
     * merge and updateAdjList merge with one neighbor
     */
    public void processMerge(){
        processMerge(incomingMsg);
    }
    
    public byte flipHeadMergeDir(byte d, boolean isFlip){
        if(isFlip){
            switch(d){
                case State.HEAD_SHOULD_MERGEWITHPREV:
                    return State.HEAD_SHOULD_MERGEWITHNEXT;
                case State.HEAD_SHOULD_MERGEWITHNEXT:
                    return State.HEAD_SHOULD_MERGEWITHPREV;
                    default:
                        return 0;
            }
        } else
            return d;
    }
    
    public boolean isDifferentDirWithMergeKmer(byte neighborToMeDir){
        return neighborToMeDir == MessageFlag.DIR_FR || neighborToMeDir == MessageFlag.DIR_RF;
    }
    
    /**
     * merge and updateAdjList  having parameter
     */
    public void processMerge(PathMergeMessageWritable msg){
        inFlag = msg.getFlag();
        byte meToNeighborDir = (byte) (inFlag & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);

        if((inFlag & MessageFlag.IS_HEAD) > 0){
            byte state = getVertexValue().getState();
            state &= State.HEAD_SHOULD_MERGE_CLEAR;
            state |= State.IS_HEAD;
            byte headMergeDir = flipHeadMergeDir((byte)(inFlag & MessageFlag.HEAD_SHOULD_MERGE_MASK), isDifferentDirWithMergeKmer(neighborToMeDir));
            state |= headMergeDir;
            getVertexValue().setState(state);
        }
        
        getVertexValue().processMerges(neighborToMeDir, msg.getNode(), kmerSize);
    }
    
    /**
     * configure UPDATE msg   boolean: true == P4, false == P2
     */
    public void configureUpdateMsgForPredecessor(boolean flag){
        outgoingMsg.setSourceVertexId(getVertexId());
        for(byte d: OutgoingListFlag.values)
            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        
        if(flag)
            outgoingMsg.setFlip(ifFilpWithSuccessor());
        else 
            outgoingMsg.setFlip(ifFilpWithSuccessor(incomingMsg.getSourceVertexId()));
        
        kmerIterator = getVertexValue().getRFList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setPredecessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getRRList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setPredecessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    public void configureUpdateMsgForSuccessor(boolean flag){
        outgoingMsg.setSourceVertexId(getVertexId());
        for(byte d: IncomingListFlag.values)
            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        
        if(flag)
            outgoingMsg.setFlip(ifFlipWithPredecessor()); 
        else
            outgoingMsg.setFlip(ifFlipWithPredecessor(incomingMsg.getSourceVertexId()));
        
        kmerIterator = getVertexValue().getFFList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setSuccessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getFRList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setSuccessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
	/**
     * send update message to neighber  boolean: true == P4, false == P2
     */
    public void broadcastUpdateMsg(boolean flag){
        if((getVertexValue().getState() & State.VERTEX_MASK) == State.IS_HEAD && (outFlag & State.VERTEX_MASK) != State.IS_FINAL)
            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK){
            case State.SHOULD_MERGEWITHPREV:
                /** confugure updateMsg for successor **/
                configureUpdateMsgForSuccessor(flag);
                break;
            case State.SHOULD_MERGEWITHNEXT:
                /** confugure updateMsg for predecessor **/
                configureUpdateMsgForPredecessor(flag);
                break; 
        }
    }
    

    /**
     * This vertex tries to merge with next vertex and send update msg to predecesspr
     */
    public void sendUpdateMsgToPredecessor(boolean flag){
        setStateAsMergeWithNext();
        if(getVertexValue().hasNextDest())
            broadcastUpdateMsg(flag);   
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to successor
     */
    public void sendUpdateMsgToSuccessor(boolean flag){
        setStateAsMergeWithPrev();
        if(getVertexValue().hasPrevDest())
            broadcastUpdateMsg(flag);
    }
    
    /**
     * override sendUpdateMsg and use incomingMsg as parameter automatically
     */
    public void sendUpdateMsg(){
        sendUpdateMsg(incomingMsg);
    }
    
    public void sendFinalUpdateMsg(){
        outFlag |= MessageFlag.IS_FINAL;
        sendUpdateMsg(incomingMsg);
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
                sendUpdateMsgToPredecessor(false);
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR: 
                sendUpdateMsgToSuccessor(false);
                break;
        }
    }
    
    public void headSendUpdateMsg(){
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(true);
        switch(getVertexValue().getState() & MessageFlag.HEAD_SHOULD_MERGE_MASK){
            case MessageFlag.HEAD_SHOULD_MERGEWITHPREV:
                sendUpdateMsgToSuccessor(false);
                break;
            case MessageFlag.HEAD_SHOULD_MERGEWITHNEXT:
                sendUpdateMsgToPredecessor(false);
                break;
        }
    }
    
    public void sendMergeMsgToSuccessor(){
        setSuccessorToMeDir();
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
    
    public void sendMergeMsgByIncomingMsgDir(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                configureMergeMsgForSuccessor(incomingMsg.getSourceVertexId());
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                configureMergeMsgForPredecessor(incomingMsg.getSourceVertexId()); 
                break; 
        }
    }
    
    /**
     * configure MERGE msg  TODO: delete edgelist, merge configureMergeMsgForPredecessor and configureMergeMsgForPredecessorByIn...
     */
    public void configureMergeMsgForPredecessor(VKmerBytesWritable mergeDest){
        setPredecessorToMeDir();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setFlip(ifFilpWithSuccessor());
//        for(byte d: OutgoingListFlag.values)
//            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(mergeDest, outgoingMsg);
    }
    
    public void configureMergeMsgForSuccessor(VKmerBytesWritable mergeDest){
        setSuccessorToMeDir();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setFlip(ifFlipWithPredecessor());
//        for(byte d: IncomingListFlag.values)
//            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(mergeDest, outgoingMsg);
    }
    
//    /**
//     * configure MERGE msg
//     */
//    public void configureMergeMsgForPredecessorByIncomingMsg(){
//        setPredecessorToMeDir();
//        outgoingMsg.setFlag(outFlag);        
//        outgoingMsg.setFlip(ifFilpWithSuccessor());
//        outgoingMsg.setSourceVertexId(getVertexId());
//        for(byte d: OutgoingListFlag.values)
//            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
//        outgoingMsg.setNode(getVertexValue().getNode());
////        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
//        sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
//    }
//    
//    public void configureMergeMsgForSuccessorByIncomingMsg(){
//        setSuccessorToMeDir();
//        outgoingMsg.setFlag(outFlag);
//        outgoingMsg.setFlip(ifFlipWithPredecessor());
//        outgoingMsg.setSourceVertexId(getVertexId());
//        for(byte d: IncomingListFlag.values)
//            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
//        outgoingMsg.setNode(getVertexValue().getNode());
////        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
//        sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
//    }
    /**
     * send merge message to neighber for P4
     */
    public void broadcastMergeMsg(boolean deleteSelf){
        if(headFlag > 0){
            outFlag |= MessageFlag.IS_HEAD;
            outFlag |= headMergeDir;
        }
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK) {
            case State.SHOULD_MERGEWITHNEXT:
                /** configure merge msg for successor **/
                configureMergeMsgForSuccessor(getNextDestVertexId());
                if(deleteSelf)
                    deleteVertex(getVertexId());
                else{
                    getVertexValue().setState(State.IS_DEAD);
                    activate();
                }
                /** logging outgoingMsg **/
//                loggingMessage(LoggingType.SEND_MSG, outgoingMsg, getNextDestVertexId());
                break;
            case State.SHOULD_MERGEWITHPREV:
                /** configure merge msg for predecessor **/
                configureMergeMsgForPredecessor(getPrevDestVertexId());
                if(deleteSelf)
                    deleteVertex(getVertexId());
                else{
                    getVertexValue().setState(State.IS_DEAD);
                    activate();
                }
                /** logging outgoingMsg **/
//                loggingMessage(LoggingType.SEND_MSG, outgoingMsg, getPrevDestVertexId());
                break; 
        }
    }
    
    public byte revertHeadMergeDir(byte headMergeDir){
        switch(headMergeDir){
            case MessageFlag.HEAD_SHOULD_MERGEWITHPREV:
                return MessageFlag.HEAD_SHOULD_MERGEWITHNEXT;
            case MessageFlag.HEAD_SHOULD_MERGEWITHNEXT:
                return MessageFlag.HEAD_SHOULD_MERGEWITHPREV;
        }
        return 0;
        
    }
    
    /**
     * Logging the vertexId and vertexValue 
     */
    public void loggingNode(byte loggingType){
        String logMessage = LogUtil.getVertexLog(loggingType, getSuperstep(), getVertexId(), getVertexValue());
        logger.fine(logMessage);
    }
    
    /**
     * Logging message
     */
    public void loggingMessage(byte loggingType, PathMergeMessageWritable msg, VKmerBytesWritable dest){
        String logMessage = LogUtil.getMessageLog(loggingType, getSuperstep(), getVertexId(), msg, dest);
        logger.fine(logMessage);
    }
}
