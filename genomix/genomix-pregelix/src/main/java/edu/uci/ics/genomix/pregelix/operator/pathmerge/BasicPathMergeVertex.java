package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;

public class BasicPathMergeVertex extends
	BasicGraphCleanVertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable>{
	
	protected PathMergeMessageWritable incomingMsg; 
    protected PathMergeMessageWritable outgoingMsg;
    
	/**
     * send update message to neighber
     * @throws IOException 
     */
    public void broadcastUpdateMsg(){
        if((getVertexValue().getState() & State.IS_HEAD) > 0)
            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK){
            case State.SHOULD_MERGEWITHPREV:
                if(getNextDestVertexId(getVertexValue()) != null){
                    setSuccessorAdjMsg();
                    if(ifFlipWithPredecessor())
                        outgoingMsg.setFlip(true);  
                    outgoingMsg.setFlag(outFlag);
//                    outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                    for(byte d: IncomingListFlag.values)
                    	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                    outgoingMsg.setSourceVertexId(getVertexId());
                    sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
                }
                break;
            case State.SHOULD_MERGEWITHNEXT:
                if(getPrevDestVertexId(getVertexValue()) != null){
                    setPredecessorAdjMsg();
                    if(ifFilpWithSuccessor())
                        outgoingMsg.setFlip(true);
                    outgoingMsg.setFlag(outFlag);
//                    outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                    for(byte d: OutgoingListFlag.values)
                    	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                    outgoingMsg.setSourceVertexId(getVertexId());
                    sendMsg(getPrevDestVertexId(getVertexValue()), outgoingMsg);
                }
                break; 
        }
    }
    
    /**
     * send update message to neighber for P2
     * @throws IOException 
     */
    public void sendUpdateMsg(){
        sendUpdateMsg(incomingMsg);
    }
    
    public void sendUpdateMsg(MessageWritable msg){
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
        sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
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
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                for(byte d: IncomingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(msg.getSourceVertexId(), outgoingMsg); //getNextDestVertexId(getVertexValue())
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                for(byte d: OutgoingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(msg.getSourceVertexId(), outgoingMsg); //getPreDestVertexId(getVertexValue())
                break; 
        }
    }
    
    /**
     * send merge message to neighber for P2
     * @throws IOException 
     */
    public void sendMergeMsg(){
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
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                for(byte d: IncomingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); //getNextDestVertexId(getVertexValue())
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                for(byte d: OutgoingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); //getPreDestVertexId(getVertexValue())
                break; 
        }
    }
    
    /**
     * send final merge message to neighber for P2
     * @throws IOException 
     */
    public void sendFinalMergeMsg(){
        outFlag |= MessageFlag.IS_FINAL;
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                for(byte d: IncomingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); //getNextDestVertexId(getVertexValue())
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                for(byte d: OutgoingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg); //getPreDestVertexId(getVertexValue())
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
        setPredecessorAdjMsg();
        if(ifFilpWithSuccessor())
            outgoingMsg.setFlip(true);
        else
            outgoingMsg.setFlip(false);
        outgoingMsg.setFlag(outFlag);
//        outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
        for(byte d: OutgoingListFlag.values)
        	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
        sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
        deleteVertex(getVertexId());
    }
    
    public void responseMergeMsgToNextNode(){
        outFlag = 0;
        if (getVertexValue().getState() == State.IS_HEAD)//is_tail
            outFlag |= MessageFlag.STOP;
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
        sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
        deleteVertex(getVertexId());
    }
    
    /**
     * send merge message to neighber for P4
     * @throws IOException 
     */
    public void broadcastMergeMsg(){
        if(headFlag > 0)
            outFlag |= MessageFlag.IS_HEAD;
        switch(getVertexValue().getState() & State.SHOULD_MERGE_MASK) {
            case State.SHOULD_MERGEWITHNEXT:
                setSuccessorAdjMsg();
                if(ifFlipWithPredecessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
                for(byte d: IncomingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
                deleteVertex(getVertexId());
                break;
            case State.SHOULD_MERGEWITHPREV:
                setPredecessorAdjMsg();
                if(ifFilpWithSuccessor())
                    outgoingMsg.setFlip(true);
                else
                    outgoingMsg.setFlip(false);
                outgoingMsg.setFlag(outFlag);
//                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                for(byte d: OutgoingListFlag.values)
                	outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(getPrevDestVertexId(getVertexValue()), outgoingMsg);
                deleteVertex(getVertexId());
                break; 
        }
    }
	
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
    
    }
}
