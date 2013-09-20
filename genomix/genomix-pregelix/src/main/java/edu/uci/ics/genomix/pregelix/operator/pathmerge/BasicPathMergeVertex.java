package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.log.LogUtil;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public abstract class BasicPathMergeVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
	BasicGraphCleanVertex<V, M>{
    protected static final boolean isP1 = true;
    protected static final boolean isP2 = false;
    protected static final boolean isP4 = true;
    
    public byte getHeadMergeDir(){
        return (byte) (getVertexValue().getState() & State.HEAD_CAN_MERGE_MASK);
    }
    
    public byte getMsgMergeDir(){
        return (byte) (incomingMsg.getFlag() & MessageFlag.HEAD_CAN_MERGE_MASK);
    }
    
    public byte getAggregatingMsgMergeDir(){
        return (byte) (aggregatingMsg.getFlag() & MessageFlag.HEAD_CAN_MERGE_MASK);
    }
    
    public boolean isHeadUnableToMerge(){
        byte state = (byte) (getVertexValue().getState() & State.HEAD_CAN_MERGE_MASK);
        return state == State.HEAD_CANNOT_MERGE;
    }
    
    public DIR revert(DIR direction){
        return direction == DIR.PREVIOUS ? DIR.NEXT : DIR.PREVIOUS;
    }

    /**
     * send UPDATE msg   boolean: true == P4, false == P2
     */
    public void sendUpdateMsg(boolean isP4, DIR direction){ 
     // TODO pass in the vertexId rather than isP4 (removes this block）
//        if(isP4)
//            outgoingMsg.setFlip(ifFlipWithNeighbor(revertDirection)); //ifFilpWithSuccessor()
//        else 
//            outgoingMsg.setFlip(ifFilpWithSuccessor(incomingMsg.getSourceVertexId()));
        
        DIR revertDirection = revert(direction);
        byte[] mergeDirs = direction == DIR.PREVIOUS ? OutgoingListFlag.values : IncomingListFlag.values;
        byte[] updateDirs = direction == DIR.PREVIOUS ? IncomingListFlag.values : OutgoingListFlag.values;
        
        //set deleteKmer
        outgoingMsg.setSourceVertexId(getVertexId());
        
        //set replaceDir
        setReplaceDir(mergeDirs);
                
        for(byte dir : updateDirs){
            kmerIterator = getVertexValue().getEdgeList(dir).getKeyIterator();
            while(kmerIterator.hasNext()){
                //set deleteDir
                byte deleteDir = setDeleteDir(dir);
                //set mergeDir, so it won't need flip
                setMergeDir(deleteDir, revertDirection);
                outgoingMsg.setFlag(outFlag);
                destVertexId = kmerIterator.next(); //TODO does destVertexId need deep copy?
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    /**
     * updateAdjList
     */
    public void processUpdate(M msg){
    	// A -> B -> C with B merging with C
        inFlag = msg.getFlag();
        byte deleteDir = (byte) ((inFlag & MessageFlag.DELETE_DIR_MASK) >> 11); // B -> A dir
        byte mergeDir = (byte) ((inFlag & MessageFlag.MERGE_DIR_MASK) >> 9); // C -> A dir
        byte replaceDir = (byte) ((inFlag & MessageFlag.REPLACE_DIR_MASK)); // C -> B dir
        
        getVertexValue().getNode().updateEdges(deleteDir, msg.getSourceVertexId(), 
                mergeDir, replaceDir, msg.getNode(), true);
    }
    
    /**
     * send MERGE msg
     */
    public void sendMergeMsg(boolean isP4){
        byte restrictedDirs = (byte)(getVertexValue().getState() & DIR.MASK);
        outFlag |= restrictedDirs;
        
        DIR direction = null;
        byte mergeDir = (byte)(getVertexValue().getState() & State.CAN_MERGE_MASK);
        if(mergeDir == State.CAN_MERGEWITHPREV)
            direction = DIR.PREVIOUS;
        else if(mergeDir == State.CAN_MERGEWITHNEXT)
            direction = DIR.NEXT;
        if(direction != null){
            setNeighborToMeDir(direction);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(getVertexValue().getNode()); //half of edges are enough
            destVertexId = getDestVertexId(direction);
            sendMsg(destVertexId, outgoingMsg);
            
            if(isP4)
                deleteVertex(getVertexId());
            else{
                getVertexValue().setState(State.IS_DEAD);
                activate();
            }
        }
    }
    
    public void aggregateMsg(Iterator<M> msgIterator){
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            //aggregate all the incomingMsgs first
            switch(getAggregatingMsgMergeDir()){
                case State.PATH_NON_HEAD:
                    aggregatingMsg.setFlag(getMsgMergeDir());
                    activate();
                    break;
                case State.HEAD_CAN_MERGEWITHPREV:
                case State.HEAD_CAN_MERGEWITHNEXT:
                    if (getAggregatingMsgMergeDir() != getMsgMergeDir())
                        aggregatingMsg.setFlag(State.HEAD_CANNOT_MERGE);
                    break;
                case State.HEAD_CANNOT_MERGE:
                    break;
            }
        }
    }
    
    public void updateState(){
        switch(getHeadMergeDir()){
            case State.PATH_NON_HEAD:
                getVertexValue().setState(getAggregatingMsgMergeDir());
                activate();
                break;
            case State.HEAD_CAN_MERGEWITHPREV:
            case State.HEAD_CAN_MERGEWITHNEXT:
                if (getHeadMergeDir() != getAggregatingMsgMergeDir()){
                    getVertexValue().setState(State.HEAD_CANNOT_MERGE);
                    voteToHalt();
                }
                break;
            case State.HEAD_CANNOT_MERGE:
                voteToHalt();
                break;
        }
    }
    
    public void setReplaceDir(byte[] mergeDirs){
        byte replaceDir = 0;
        for(byte dir : mergeDirs){
            int num = getVertexValue().getEdgeList(dir).getCountOfPosition();
            if(num > 0){
                if(num != 1)
                    throw new IllegalStateException("Only can sendUpdateMsg to degree = 1 direction!");
                outgoingMsg.getNode().setEdgeList(dir, getVertexValue().getEdgeList(dir));
                replaceDir = dir;
                break;
            }
        }
        outFlag &= MessageFlag.REPLACE_DIR_CLEAR;
        outFlag |= replaceDir;
    }
    
    public byte setDeleteDir(byte dir){
        byte deleteDir = mirrorDirection(dir);
        outFlag &= MessageFlag.DELETE_DIR_CLEAR;
        outFlag |= (deleteDir << 11);
        return deleteDir;
    }
    
    public void setMergeDir(byte deleteDir, DIR revertDirection){
        byte mergeDir = flipDirection(deleteDir, ifFlipWithNeighbor(revertDirection));
        outFlag &= MessageFlag.MERGE_DIR_CLEAR;
        outFlag |= (mergeDir << 9);
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

        short state = getVertexValue().getState();  
//        state |= flipHeadMergeDir((byte)(inFlag & DIR.MASK), isDifferentDirWithMergeKmer(neighborToMeDir));
        getVertexValue().setState(state);
        
        getVertexValue().processMerges(neighborToMeDir, msg.getNode(), kmerSize);
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
                sendUpdateMsg(isP2, DIR.PREVIOUS);
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR: 
                sendUpdateMsg(isP2, DIR.NEXT);
                break;
        }
    }
    
    public void headSendUpdateMsg(){
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(true);
        switch(getVertexValue().getState() & MessageFlag.HEAD_CAN_MERGE_MASK){
            case MessageFlag.HEAD_CAN_MERGEWITHPREV:
                sendUpdateMsg(isP2, DIR.NEXT);
                break;
            case MessageFlag.HEAD_CAN_MERGEWITHNEXT:
                sendUpdateMsg(isP2, DIR.PREVIOUS);
                break;
        }
    }
    
    public void sendMergeMsgToSuccessor(){
        setNeighborToMeDir(DIR.NEXT);
        if(ifFlipWithPredecessor())
            outgoingMsg.setFlip(true);
        else
            outgoingMsg.setFlip(false);
        outgoingMsg.setFlag(outFlag);
        for(byte d: IncomingListFlag.values)
        	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
        sendMsg(getDestVertexId(DIR.NEXT), outgoingMsg);
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
        setNeighborToMeDir(DIR.PREVIOUS);
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
//        for(byte d: OutgoingListFlag.values)
//            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(mergeDest, outgoingMsg);
    }
    
    public void configureMergeMsgForSuccessor(VKmerBytesWritable mergeDest){
        setNeighborToMeDir(DIR.NEXT);
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
//        for(byte d: IncomingListFlag.values)
//            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setNode(getVertexValue().getNode());
        sendMsg(mergeDest, outgoingMsg);
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
    
    /*
     * garbage
     */
    public void setHeadMergeDir(){
        byte state = 0;
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                state |= State.HEAD_CAN_MERGEWITHPREV;
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                state |= State.HEAD_CAN_MERGEWITHNEXT;
                break;
        }
        getVertexValue().setState(state);
    }
}
