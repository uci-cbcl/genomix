package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * Naive Algorithm for path merge graph
 */
public class BasicGraphCleanVertex extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "BasicGraphCleanVertex.kmerSize";
    public static final String ITERATIONS = "BasicGraphCleanVertex.iteration";
    public static int kmerSize = -1;
    public static int maxIteration = -1;
    
    protected MessageWritable incomingMsg = null; 
    protected MessageWritable outgoingMsg = null; 
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
     * check if prev/next destination exists
     */
    public boolean hasPrevDest(VertexValueWritable value){
        return value.getRFList().getCountOfPosition() > 0 || value.getRRList().getCountOfPosition() > 0;
    }
    
    public boolean hasNextDest(VertexValueWritable value){
        return value.getFFList().getCountOfPosition() > 0 || value.getFRList().getCountOfPosition() > 0;
    }
    
    /**
     * get destination vertex
     */
    public VKmerBytesWritable getNextDestVertexId(VertexValueWritable value) {
        if (value.getFFList().getCountOfPosition() > 0){ //#FFList() > 0
            kmerIterator = value.getFFList().iterator();
            return kmerIterator.next();
        } else if (value.getFRList().getCountOfPosition() > 0){ //#FRList() > 0
            kmerIterator = value.getFRList().iterator();
            return kmerIterator.next();
        } else {
            return null;  
        }
    }

    public VKmerBytesWritable getPrevDestVertexId(VertexValueWritable value) {
        if (value.getRFList().getCountOfPosition() > 0){ //#RFList() > 0
            kmerIterator = value.getRFList().iterator();
            return kmerIterator.next();
        } else if (value.getRRList().getCountOfPosition() > 0){ //#RRList() > 0
            kmerIterator = value.getRRList().iterator();
            return kmerIterator.next();
        } else {
            return null;
        }
    }
    
    /**
     * get destination vertex
     */
    public VKmerBytesWritable getNextDestVertexIdAndSetFlag(VertexValueWritable value) {
        if (value.getFFList().getCountOfPosition() > 0){ // #FFList() > 0
            kmerIterator = value.getFFList().iterator();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FF;
            return kmerIterator.next();
        } else if (value.getFRList().getCountOfPosition() > 0){ // #FRList() > 0
            kmerIterator = value.getFRList().iterator();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FR;
            return kmerIterator.next();
        } else {
          return null;  
        }
        
    }

    public VKmerBytesWritable getPrevDestVertexIdAndSetFlag(VertexValueWritable value) {
        if (value.getRFList().getCountOfPosition() > 0){ // #RFList() > 0
            kmerIterator = value.getRFList().iterator();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RF;
            return kmerIterator.next();
        } else if (value.getRRList().getCountOfPosition() > 0){ // #RRList() > 0
            kmerIterator = value.getRRList().iterator();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RR;
            return kmerIterator.next();
        } else {
            return null;
        }
    }
    
    /**
     * head send message to all previous nodes
     */
    public void sendMsgToAllPreviousNodes(VertexValueWritable value) {
        kmerIterator = value.getRFList().iterator(); // RFList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = value.getRRList().iterator(); // RRList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes(VertexValueWritable value) {
        kmerIterator = value.getFFList().iterator(); // FFList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = value.getFRList().iterator(); // FRList
        while(kmerIterator.hasNext()){
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    /**
     * one vertex send message to previous and next vertices (neighbor)
     */
    public void sendMsgToAllNeighborNodes(VertexValueWritable value){
        sendMsgToAllNextNodes(value);
        sendMsgToAllPreviousNodes(value);
    }
    
    /**
     * tip send message with sourceId and dir to previous node 
     * tip only has one incoming
     */
    public void sendSettledMsgToPrevNode(){
        if(hasPrevDest(getVertexValue())){
            if(getVertexValue().getRFList().getCountOfPosition() > 0)
                outgoingMsg.setFlag(MessageFlag.DIR_RF);
            else if(getVertexValue().getRRList().getCountOfPosition() > 0)
                outgoingMsg.setFlag(MessageFlag.DIR_RR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(getPrevDestVertexId(getVertexValue()));
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * tip send message with sourceId and dir to next node 
     * tip only has one outgoing
     */
    public void sendSettledMsgToNextNode(){
        if(hasNextDest(getVertexValue())){
            if(getVertexValue().getFFList().getCountOfPosition() > 0)
                outgoingMsg.setFlag(MessageFlag.DIR_FF);
            else if(getVertexValue().getFRList().getCountOfPosition() > 0)
                outgoingMsg.setFlag(MessageFlag.DIR_FR);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(getNextDestVertexId(getVertexValue()));
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * head send message to all previous nodes
     */
    public void sendSettledMsgToAllPreviousNodes(VertexValueWritable value) {
        kmerIterator = value.getRFList().iterator(); // RFList
        while(kmerIterator.hasNext()){
            outFlag |= MessageFlag.DIR_RF;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = value.getRRList().iterator(); // RRList
        while(kmerIterator.hasNext()){
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
    public void sendSettledMsgToAllNextNodes(VertexValueWritable value) {
        kmerIterator = value.getFFList().iterator(); // FFList
        while(kmerIterator.hasNext()){
            outFlag |= MessageFlag.DIR_FF;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = value.getFRList().iterator(); // FRList
        while(kmerIterator.hasNext()){
            outFlag |= MessageFlag.DIR_FR;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    public void sendSettledMsgToAllNeighborNodes() {
        sendSettledMsgToAllPreviousNodes(getVertexValue());
        sendSettledMsgToAllNextNodes(getVertexValue());
    }
    
    /**
     * start sending message
     */
    public void startSendMsg() {
        if (VertexUtil.isHeadVertexWithIndegree(getVertexValue())) {
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD));
            sendMsgToAllNextNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isRearVertexWithOutdegree(getVertexValue())) {
            outgoingMsg.setFlag((byte)(MessageFlag.IS_HEAD));
            sendMsgToAllPreviousNodes(getVertexValue());
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
        if(getVertexValue().getFRList().getCountOfPosition() > 0)
            return true;
        else
            return false;
    }
    
    /**
     * check if A need to be filpped with predecessor
     */
    public boolean ifFlipWithPredecessor(){
        if(getVertexValue().getRFList().getCountOfPosition() > 0)
            return true;
        else
            return false;
    }
    
    /**
     * set adjMessage to successor(from predecessor)
     */
    public void setSuccessorAdjMsg(){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(getVertexValue().getFFList().getCountOfPosition() > 0)
            outFlag |= MessageFlag.DIR_FF;
        else if(getVertexValue().getFRList().getCountOfPosition() > 0)
            outFlag |= MessageFlag.DIR_FR;
    }
    
    /**
     * set adjMessage to predecessor(from successor)
     */
    public void setPredecessorAdjMsg(){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(getVertexValue().getRFList().getCountOfPosition() > 0)
            outFlag |= MessageFlag.DIR_RF;
        else if(getVertexValue().getRRList().getCountOfPosition() > 0)
            outFlag |= MessageFlag.DIR_RR;
    }
    
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
                    outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
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
                    outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
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
        outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
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
        outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
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
        outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getIncomingList());
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
                outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
                sendMsg(getPrevDestVertexId(getVertexValue()), outgoingMsg);
                deleteVertex(getVertexId());
                break; 
        }
    }
    
    public void setStateAsMergeWithNext(){
    	byte state = getVertexValue().getState();
    	state &= State.SHOULD_MERGE_CLEAR;
        state |= State.SHOULD_MERGEWITHNEXT;
        getVertexValue().setState(state);
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to neighber
     * @throws IOException 
     */
    public void sendUpdateMsgToPredecessor(){
	    setStateAsMergeWithNext();
	    broadcastUpdateMsg();
    }
    
    public void setStateAsMergeWithPrev(){
        byte state = getVertexValue().getState();
        state &= State.SHOULD_MERGE_CLEAR;
        state |= State.SHOULD_MERGEWITHPREV;
        getVertexValue().setState(state);
    }
    
    /**
     * This vertex tries to merge with next vertex and send update msg to neighber
     * @throws IOException 
     */
    public void sendUpdateMsgToSuccessor(){
    	if(hasPrevDest(getVertexValue())){
    	    setStateAsMergeWithPrev();
		    broadcastUpdateMsg();
    	}
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
                kmerSize, incomingMsg.getInternalKmer());
    }
    
    /**
     * merge and updateAdjList  having parameter
     */
    public void processMerge(MessageWritable msg){
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);

        byte neighborToMergeDir = flipDirection(neighborToMeDir, msg.isFlip());
        
        getVertexValue().processMerges(neighborToMeDir, msg.getSourceVertexId(), 
                neighborToMergeDir, VertexUtil.getNodeIdFromAdjacencyList(msg.getNeighberNode()),
                kmerSize, msg.getInternalKmer());
    }
    
    /**
     * final merge and updateAdjList  having parameter for p2
     */
    public void processFinalMerge(MessageWritable msg){
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
                neighborToMergeDir, VertexUtil.getNodeIdFromAdjacencyList(msg.getNeighberNode()),
                kmerSize, tmpKmer);
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
     * broadcast kill self to all neighbers  Pre-condition: vertex is a path vertex 
     */
    public void broadcaseKillself(){
        outFlag = 0;
        outFlag |= MessageFlag.KILL;
        outFlag |= MessageFlag.DIR_FROM_DEADVERTEX;
        outgoingMsg.setSourceVertexId(getVertexId());
        
        if(getVertexValue().getFFList().getCountOfPosition() > 0){//#FFList() > 0
            outFlag |= MessageFlag.DIR_FF;
            outgoingMsg.setFlag(outFlag);
            sendMsg(getVertexValue().getFFList().getPosition(0), outgoingMsg);
        }
        else if(getVertexValue().getFRList().getCountOfPosition() > 0){//#FRList() > 0
            outFlag |= MessageFlag.DIR_FR;
            outgoingMsg.setFlag(outFlag);
            sendMsg(getVertexValue().getFRList().getPosition(0), outgoingMsg);
        }
        
        
        if(getVertexValue().getRFList().getCountOfPosition() > 0){//#RFList() > 0
            outFlag |= MessageFlag.DIR_RF;
            outgoingMsg.setFlag(outFlag);
            sendMsg(getVertexValue().getRFList().getPosition(0), outgoingMsg);
        }
        else if(getVertexValue().getRRList().getCountOfPosition() > 0){//#RRList() > 0
            outFlag |= MessageFlag.DIR_RR;
            outgoingMsg.setFlag(outFlag);
            sendMsg(getVertexValue().getRRList().getPosition(0), outgoingMsg);
        }
        
        deleteVertex(getVertexId());
    }
    
    /**
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertex(){
        switch(incomingMsg.getFlag() & MessageFlag.DIR_MASK){
            case MessageFlag.DIR_FF:
                //remove incomingMsg.getSourceId from RR positionList
                kmerIterator = getVertexValue().getRRList().iterator();
                while(kmerIterator.hasNext()){
                    tmpKmer = kmerIterator.next();
                    if(tmpKmer.equals(incomingMsg.getSourceVertexId())){
                        kmerIterator.remove();
                        break;
                    }
                }
                break;
            case MessageFlag.DIR_FR:
                //remove incomingMsg.getSourceId from FR positionList
                kmerIterator = getVertexValue().getFRList().iterator();
                while(kmerIterator.hasNext()){
                    tmpKmer = kmerIterator.next();
                    if(tmpKmer.equals(incomingMsg.getSourceVertexId())){
                        kmerIterator.remove();
                        break;
                    }
                }
                break;
            case MessageFlag.DIR_RF:
                //remove incomingMsg.getSourceId from RF positionList
                kmerIterator = getVertexValue().getRFList().iterator();
                while(kmerIterator.hasNext()){
                    tmpKmer = kmerIterator.next();
                    if(tmpKmer.equals(incomingMsg.getSourceVertexId())){
                        kmerIterator.remove();
                        break;
                    }
                }
                break;
            case MessageFlag.DIR_RR:
                //remove incomingMsg.getSourceId from FF positionList
                kmerIterator = getVertexValue().getFFList().iterator();
                while(kmerIterator.hasNext()){
                    tmpKmer = kmerIterator.next();
                    if(tmpKmer.equals(incomingMsg.getSourceVertexId())){
                        kmerIterator.remove();
                        break;
                    }
                }
                break;
        }
    }
    
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
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
    }
}