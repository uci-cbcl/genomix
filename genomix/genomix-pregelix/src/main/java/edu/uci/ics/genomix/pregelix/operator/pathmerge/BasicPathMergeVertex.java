package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.log.LogUtil;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public abstract class BasicPathMergeVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
	BasicGraphCleanVertex<V, M>{
	
    public void setStateAsMergeWithPrev(){
        short state = getVertexValue().getState();
        state &= State.CAN_MERGE_CLEAR;
        state |= State.CAN_MERGEWITHPREV;
        getVertexValue().setState(state);
        activate();
    }
    
    public void setStateAsMergeWithNext(){
        short state = getVertexValue().getState();
        state &= State.CAN_MERGE_CLEAR;
        state |= State.CAN_MERGEWITHNEXT;
        getVertexValue().setState(state);
        activate();
    }
    
    /**
     * updateAdjList
     */
    public void processUpdate(){
    	// A -> B -> C with B merging with C
        inFlag = incomingMsg.getFlag();
        byte meToNeighborDir = (byte) (inFlag & MessageFlag.DIR_MASK);  // A -> B dir
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);  // B -> A dir
        
        // TODO if you want, this logic could be figured out when sending the update from B
        byte neighborToMergeDir = flipDirection(neighborToMeDir, incomingMsg.isFlip());  // A -> C after the merge
         // TODO add C -> A dir and call node.updateEdges directly
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
    public void processMerge(){ // TODO remove me
        processMerge(incomingMsg);
    }
    
    public byte flipHeadMergeDir(byte d, boolean isFlip){
        if(isFlip){
            switch(d){
                case State.HEAD_CAN_MERGEWITHPREV:
                    return State.HEAD_CAN_MERGEWITHNEXT;
                case State.HEAD_CAN_MERGEWITHNEXT:
                    return State.HEAD_CAN_MERGEWITHPREV;
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
     * check if head receives message from head
     */
    public boolean isHeadMeetsHead(boolean selfFlag){
        boolean msgFlag = (getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHPREV || getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHNEXT);
        return selfFlag && msgFlag;
    }
    
    /**
     * check if non-head receives message from head 
     */
    public boolean isNonHeadReceivedFromHead(){
        boolean selfFlag = (getHeadMergeDir() == State.HEAD_CAN_MERGEWITHPREV || getHeadMergeDir() == State.HEAD_CAN_MERGEWITHNEXT);
        boolean msgFlag = (getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHPREV || getMsgMergeDir() == MessageFlag.HEAD_CAN_MERGEWITHNEXT);
        return selfFlag == false && msgFlag == true;
    }
    /**
     * merge and updateAdjList  having parameter
     */
    public void processMerge(PathMergeMessageWritable msg){
        inFlag = msg.getFlag();
        byte meToNeighborDir = (byte) (inFlag & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);

        if(isNonHeadReceivedFromHead()){ // TODO? why sepcial-case the path vs heads?  just aggregate your state flags
            short state = getVertexValue().getState();
            state &= State.HEAD_CAN_MERGE_CLEAR;
            byte headMergeDir = flipHeadMergeDir((byte)(inFlag & MessageFlag.HEAD_CAN_MERGE_MASK), isDifferentDirWithMergeKmer(neighborToMeDir));
            state |= headMergeDir;
            getVertexValue().setState(state);
        }
        
        getVertexValue().processMerges(neighborToMeDir, msg.getNode(), kmerSize);
    }
    
    
    /**
     * send UPDATE msg   boolean: true == P4, false == P2
     */
    public void sendUpdateMsg(boolean isP4, boolean toPredecessor){ 
        outgoingMsg.setSourceVertexId(getVertexId());
        // TODO pass in the vertexId rather than isP4 (removes this block）
        if(isP4)
            outgoingMsg.setFlip(ifFlipWithNeighbor(!toPredecessor)); //ifFilpWithSuccessor()
        else 
            outgoingMsg.setFlip(ifFilpWithSuccessor(incomingMsg.getSourceVertexId()));
        
        byte[] mergeDirs = toPredecessor ? OutgoingListFlag.values : IncomingListFlag.values;
        byte[] updateDirs = toPredecessor ? IncomingListFlag.values : OutgoingListFlag.values;
        
        for(byte dir : mergeDirs)
            outgoingMsg.getNode().setEdgeList(dir, getVertexValue().getEdgeList(dir));  // TODO check
        
        for(byte dir : updateDirs){
            kmerIterator = getVertexValue().getEdgeList(dir).getKeys();
            while(kmerIterator.hasNext()){
                outFlag &= MessageFlag.DIR_CLEAR;
                outFlag |= dir;
                outgoingMsg.setFlag(outFlag);
                destVertexId.setAsCopy(kmerIterator.next()); //TODO does destVertexId need deep copy?
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    /**
     * configure UPDATE msg   boolean: true == P4, false == P2
     */
    public void configureUpdateMsgForPredecessor(boolean isP4){ 
        outgoingMsg.setSourceVertexId(getVertexId());
        // TODO pass in isForward
        for(byte d: OutgoingListFlag.values)
            outgoingMsg.getNode().setEdgeList(d, getVertexValue().getEdgeList(d));  // TODO check
        
        // TODO pass in the vertexId rather than isP4 (removes this block）
        if(isP4)
            outgoingMsg.setFlip(ifFilpWithSuccessor());
        else 
            outgoingMsg.setFlip(ifFilpWithSuccessor(incomingMsg.getSourceVertexId()));
        
        kmerIterator = getVertexValue().getRFList().getKeys();
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RF;
            outgoingMsg.setFlag(outFlag);
            destVertexId.setAsCopy(kmerIterator.next());
            // TODO DON'T NEED TO SEARCH for this
//            setPredecessorToMeDir(destVertexId); 
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getRRList().getKeys();
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RR;
            outgoingMsg.setFlag(outFlag);
            destVertexId.setAsCopy(kmerIterator.next());
//            setPredecessorToMeDir(destVertexId);
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
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FF;
            outgoingMsg.setFlag(outFlag);
            destVertexId.setAsCopy(kmerIterator.next());
//            setSuccessorToMeDir(destVertexId);
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getFRList().getKeys();
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FR;
            outgoingMsg.setFlag(outFlag);
            destVertexId.setAsCopy(kmerIterator.next());
//            setSuccessorToMeDir(destVertexId);
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
	/**
     * send update message to neighber  boolean: true == P4, false == P2
     */
    public void broadcastUpdateMsg(boolean flag){
//        if((getVertexValue().getState() & State.VERTEX_MASK) == State.IS_HEAD && (outFlag & State.VERTEX_MASK) != State.IS_FINAL)
//            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & State.CAN_MERGE_MASK){
            case State.CAN_MERGEWITHPREV:
                /** confugure updateMsg for successor **/
                configureUpdateMsgForSuccessor(flag);
                break;
            case State.CAN_MERGEWITHNEXT:
                /** confugure updateMsg for predecessor **/
                configureUpdateMsgForPredecessor(flag);
                break; 
        }
    }
    

    /**
     * This vertex tries to merge with next vertex and send update msg to predecesspr
     */
    public void sendUpdateMsgToPredecessor(boolean flag){
        if(getVertexValue().hasNextDest())  //TODO delete
            broadcastUpdateMsg(flag);   
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to successor
     */
    public void sendUpdateMsgToSuccessor(boolean flag){
        if(getVertexValue().hasPrevDest())
            broadcastUpdateMsg(flag);
    }
    
    /**
     * override sendUpdateMsg and use incomingMsg as parameter automatically
     */
    public void sendUpdateMsg(){
        sendUpdateMsgForP2(incomingMsg);
    }
    
    public void sendFinalUpdateMsg(){
        outFlag |= MessageFlag.IS_FINAL;
        sendUpdateMsgForP2(incomingMsg);
    }
    
    /**
     * send update message to neighber for P2
     */
    public void sendUpdateMsgForP2(MessageWritable msg){
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
        switch(getVertexValue().getState() & MessageFlag.HEAD_CAN_MERGE_MASK){
            case MessageFlag.HEAD_CAN_MERGEWITHPREV:
                sendUpdateMsgToSuccessor(false);
                break;
            case MessageFlag.HEAD_CAN_MERGEWITHNEXT:
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
        outgoingMsg.setFlip(ifFlipWithPredecessor()); // TODO seems incorrect for outgoing... why predecessor?  //TODO REMOVE this flip boolean completely
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
        outFlag |= getHeadMergeDir();
        switch(getVertexValue().getState() & State.CAN_MERGE_MASK) {
            case State.CAN_MERGEWITHNEXT:
                // configure merge msg for successor
                configureMergeMsgForSuccessor(getNextDestVertexId()); // TODO getDestVertexId(DIRECTION), then remove the switch statement, sendMergeMsg(DIRECTION)
                if(deleteSelf)
                    deleteVertex(getVertexId());
                else{
                    getVertexValue().setState(State.IS_DEAD);
                    activate();
                }
                break;
            case State.CAN_MERGEWITHPREV:
                // configure merge msg for predecessor
                configureMergeMsgForPredecessor(getPrevDestVertexId());
                if(deleteSelf)
                    deleteVertex(getVertexId());
                else{
                    getVertexValue().setState(State.IS_DEAD);
                    activate();
                }
                break; 
        }
    }
    
    public byte revertHeadMergeDir(byte headMergeDir){
        switch(headMergeDir){
            case MessageFlag.HEAD_CAN_MERGEWITHPREV:
                return MessageFlag.HEAD_CAN_MERGEWITHNEXT;
            case MessageFlag.HEAD_CAN_MERGEWITHNEXT:
                return MessageFlag.HEAD_CAN_MERGEWITHPREV;
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
