package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.GeneCode;
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
        
//        byte neighborToMergeDir = flipDirection(neighborToMeDir, msg.isFlip());
        
        getVertexValue().processMerges(neighborToMeDir, msg.getNode(), kmerSize);
//        getVertexValue().processMerges(neighborToMeDir, msg.getSourceVertexId(), 
//                neighborToMergeDir, msg.getNeighborEdge(),
//                kmerSize, msg.getNode());
    }
    
    /**
     * final merge and updateAdjList  having parameter for p2
     */
    public void processFinalMerge(PathMergeMessageWritable msg){
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK); 
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
//        byte neighborToMergeDir = 0;
//        if(getMsgFlag() == MessageFlag.IS_FINAL)
//            neighborToMergeDir = neighborToMeDir;
//        else
//            neighborToMergeDir = flipDirection(neighborToMeDir, msg.isFlip());
        
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
       
        //TODO: fix mergeWithNode
//        getVertexValue().processMerges(neighborToMeDir, msg.getSourceVertexId(), 
//                neighborToMergeDir, msg.getNeighborEdge(),
//                kmerSize, tmpKmer);
    }
    
    /**
     * configure UPDATE msg
     */
    public void configureUpdateMsgForPredecessor(){
        outgoingMsg.setSourceVertexId(getVertexId());
        for(byte d: OutgoingListFlag.values)
            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        
        kmerIterator = getVertexValue().getRFList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setPredecessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setFlip(ifFilpWithSuccessor(incomingMsg.getSourceVertexId()));
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getRRList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setPredecessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setFlip(ifFilpWithSuccessor(incomingMsg.getSourceVertexId()));
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    public void configureUpdateMsgForSuccessor(){
        outgoingMsg.setSourceVertexId(getVertexId());
        for(byte d: IncomingListFlag.values)
            outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        
        kmerIterator = getVertexValue().getFFList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setSuccessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setFlip(ifFlipWithPredecessor(incomingMsg.getSourceVertexId()));  
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = getVertexValue().getFRList().getKeys();
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            setSuccessorToMeDir(destVertexId);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setFlip(ifFlipWithPredecessor(incomingMsg.getSourceVertexId()));  //destVertexId
            sendMsg(destVertexId, outgoingMsg);
        }
    }
	/**
     * send update message to neighber
     */
    public void broadcastUpdateMsg(){
        if((getVertexValue().getState() & State.VERTEX_MASK) == State.IS_HEAD)
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
     * override sendUpdateMsg and use incomingMsg as parameter automatically
     */
    public void sendUpdateMsg(){
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
                sendUpdateMsgToPredecessor();
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR: 
                sendUpdateMsgToSuccessor();
                break;
        }
    }
    
    public void headSendUpdateMsg(){
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(true);
        switch(getVertexValue().getState() & MessageFlag.HEAD_SHOULD_MERGE_MASK){
            case MessageFlag.HEAD_SHOULD_MERGEWITHPREV:
                sendUpdateMsgToSuccessor();
                break;
            case MessageFlag.HEAD_SHOULD_MERGEWITHNEXT:
                sendUpdateMsgToPredecessor();
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
                configureMergeMsgForSuccessor(incomingMsg.getSourceVertexId());
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                configureMergeMsgForPredecessor(incomingMsg.getSourceVertexId()); 
                break; 
        }
    }
    
    /**
     * send merge message to neighber for P1
     */
    public void responseMergeMsgToPrevNode(){
        outFlag = 0;
        if (getVertexValue().getState() == State.IS_HEAD)//is_tail
            outFlag |= MessageFlag.STOP;
        configureMergeMsgForPredecessor(getPrevDestVertexId());
        deleteVertex(getVertexId());
    }
    
    /**
     *  for P1
     */
    public void responseMergeMsgToNextNode(){
        outFlag = 0;
        if (getVertexValue().getState() == State.IS_HEAD)//is_tail
            outFlag |= MessageFlag.STOP;
        configureMergeMsgForSuccessor(getNextDestVertexId());
        deleteVertex(getVertexId());
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
    public void broadcastMergeMsg(){
        if(headFlag > 0){
            outFlag |= MessageFlag.IS_HEAD;
            outFlag |= headMergeDir;
        }
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK) {
            case State.SHOULD_MERGEWITHNEXT:
                /** configure merge msg for successor **/
                configureMergeMsgForSuccessor(getNextDestVertexId());
                deleteVertex(getVertexId());
                break;
            case State.SHOULD_MERGEWITHPREV:
                /** configure merge msg for predecessor **/
                configureMergeMsgForPredecessor(getPrevDestVertexId());
                deleteVertex(getVertexId());
                break; 
        }
    }
}
