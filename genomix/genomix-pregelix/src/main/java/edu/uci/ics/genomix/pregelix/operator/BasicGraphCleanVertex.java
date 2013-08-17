package edu.uci.ics.genomix.pregelix.operator;

import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

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
    protected VKmerListWritable kmerList = null;
    protected VKmerBytesWritable curKmer = null; //for detect tandemRepeat
    protected byte repeatDir; //for detect tandemRepeat
    protected VKmerBytesWritable tmpKmer = null;
    protected byte headFlag;
    protected byte outFlag;
    protected byte inFlag;
    protected byte selfFlag;
    protected byte headMergeDir;
    
    public static boolean fakeVertexExist = false;
    protected static VKmerBytesWritable fakeVertex = null;
    
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
    
    public byte getHeadMergeDir(){
        return (byte) (getVertexValue().getState() & State.HEAD_SHOULD_MERGE_MASK);
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
        this.activate();
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
    
    public boolean isHaltNode(){
        return getVertexValue().getState() == State.IS_HALT;
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
    
    public boolean isHeadNode(){
        return selfFlag == State.IS_HEAD;
    }
    
    public boolean isPathNode(){
        return selfFlag != State.IS_HEAD && selfFlag != State.IS_OLDHEAD;
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
    public void sendSettledMsgToAllPrevNodes() {
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
        sendSettledMsgToAllPrevNodes();
        sendSettledMsgToAllNextNodes();
    }
    
    /**
     * start sending message
     */
    public void startSendMsg() {
//        if(isTandemRepeat())
        
        if (VertexUtil.isVertexWithOnlyOneIncoming(getVertexValue())){
            outFlag = 0;
            outFlag |= MessageFlag.IS_HEAD;
            outFlag |= MessageFlag.HEAD_SHOULD_MERGEWITHPREV;
            getVertexValue().setState(outFlag);
            activate();
        }
        if (VertexUtil.isVertexWithOnlyOneOutgoing(getVertexValue())){
            outFlag = 0;
            outFlag |= MessageFlag.IS_HEAD;
            outFlag |= MessageFlag.HEAD_SHOULD_MERGEWITHNEXT;
            getVertexValue().setState(outFlag);
            activate();
        }
        if (VertexUtil.isVertexWithManyIncoming(getVertexValue())) {
            outFlag = 0;
            outFlag |= MessageFlag.IS_HEAD;
            sendSettledMsgToAllPrevNodes();
        }
        if (VertexUtil.isVertexWithManyOutgoing(getVertexValue())) {
            outFlag = 0;
            outFlag |= MessageFlag.IS_HEAD;
            sendSettledMsgToAllNextNodes();
        }
        if(!VertexUtil.isActiveVertex(getVertexValue())){
            getVertexValue().setState(MessageFlag.IS_HALT);
            voteToHalt();
        }
    }

    public void setHeadMergeDir(){
        byte state = MessageFlag.IS_HEAD;
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                state |= MessageFlag.HEAD_SHOULD_MERGEWITHPREV;
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                state |= MessageFlag.HEAD_SHOULD_MERGEWITHNEXT;
                break;
        }
        getVertexValue().setState(state);
    }
    /**
     * initiate head, rear and path node
     */
    public void initState(Iterator<M> msgIterator) {
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(getHeadFlag() != MessageFlag.IS_HEAD){
                setHeadMergeDir();
                activate();
            } else{ /** already set up **/
                /** if headMergeDir are not the same **/
                getVertexValue().setState(MessageFlag.IS_HALT);
                voteToHalt();
            }
        }
//            byte headMergeDir = (byte) (incomingMsg.getFlag() & MessageFlag.HEAD_SHOULD_MERGE_MASK);
//            switch(headMergeDir){
//                case MessageFlag.HEAD_SHOULD_MERGEWITHPREV:
//                    /** not set up yet **/
//                    if(getHeadFlag() != MessageFlag.IS_HEAD){
//                        getVertexValue().setState(incomingMsg.getFlag());
//                    } else{ /** already set up **/
//                        /** if headMergeDir are not the same **/
//                        if(getHeadMergeDir() != headMergeDir){
//                            getVertexValue().setState(MessageFlag.IS_HALT);
//                            voteToHalt();
//                        }
//                    }
//                    break;
//                case MessageFlag.HEAD_SHOULD_MERGEWITHNEXT:
//                    /** not set up yet **/
//                    if(getHeadFlag() != MessageFlag.IS_HEAD){
//                        getVertexValue().setState(incomingMsg.getFlag());
//                    } else{ /** already set up **/
//                        /** if headMergeDir are not the same **/
//                        if(getHeadMergeDir() != headMergeDir){
//                            getVertexValue().setState(MessageFlag.IS_HALT);
//                            voteToHalt();
//                        }
//                    }
//                    break;
//            }
//            if (!VertexUtil.isPathVertex(getVertexValue())
//                    && !VertexUtil.isHeadWithoutIndegree(getVertexValue())
//                    && !VertexUtil.isRearWithoutOutdegree(getVertexValue())) {
//                msgIterator.next();
//                voteToHalt();
//            } else {
//                incomingMsg = msgIterator.next();
//                if(getHeadFlag() > 0)
//                    voteToHalt();
//                else 
//                    getVertexValue().setState(incomingMsg.getFlag());
//            }
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
    
    public boolean ifFlipWithPredecessor(VKmerBytesWritable toFind){
        if(getVertexValue().getRFList().contains(toFind))
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
    
    public boolean ifFilpWithSuccessor(VKmerBytesWritable toFind){
        if(getVertexValue().getFRList().contains(toFind))
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
    
    public void setPredecessorAdjMsg(VKmerBytesWritable toFind){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(getVertexValue().getRFList().contains(toFind))
            outFlag |= MessageFlag.DIR_RF;
        else if(getVertexValue().getRRList().contains(toFind))
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
    
    public void setSuccessorAdjMsg(VKmerBytesWritable toFind){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(getVertexValue().getFFList().contains(toFind))
            outFlag |= MessageFlag.DIR_FF;
        else if(getVertexValue().getFRList().contains(toFind))
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
     * broadcast kill self to all neighbers ***
     */
    public void broadcaseKillself(){
        outFlag = 0;
        outFlag |= MessageFlag.KILL;
        outFlag |= MessageFlag.DIR_FROM_DEADVERTEX;
        
        sendSettledMsgToAllNeighborNodes();
        
        deleteVertex(getVertexId());
    }
    
    /**
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertex(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        getVertexValue().getEdgeList(neighborToMeDir).remove(incomingMsg.getSourceVertexId());
    }
    
    /**
     * Generate random string from [ACGT]
     */
    public String generaterRandomString(int n){
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }
    /**
     * add fake vertex
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void addFakeVertex(){
        if(!fakeVertexExist){
            //add a fake vertex
            Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
            vertex.getMsgList().clear();
            vertex.getEdges().clear();
            VertexValueWritable vertexValue = new VertexValueWritable();//kmerSize + 1
            vertexValue.setState(State.IS_FAKE);
            vertexValue.setFakeVertex(true);
            
            vertex.setVertexId(fakeVertex);
            vertex.setVertexValue(vertexValue);
            
            addVertex(fakeVertex, vertex);
            fakeVertexExist = true;
        }
    }
    
    /**
     * check if it is a tandem repeat
     */
    public boolean isTandemRepeat(){
        for(byte d : DirectionFlag.values){
            Iterator<VKmerBytesWritable> it = getVertexValue().getEdgeList(d).getKeys();
            while(it.hasNext()){
                curKmer.setAsCopy(it.next());
                if(curKmer.equals(getVertexId())){
                    repeatDir = d;
                    return true;
                }
            }
        }
        return false;
    }
    
    public byte flipDir(byte dir){
        switch(dir){
            case DirectionFlag.DIR_FF:
                return DirectionFlag.DIR_RF;
            case DirectionFlag.DIR_FR:
                return DirectionFlag.DIR_RR;
            case DirectionFlag.DIR_RF:
                return DirectionFlag.DIR_FF;
            case DirectionFlag.DIR_RR:
                return DirectionFlag.DIR_FR;
            default:
                throw new RuntimeException("Unrecognized direction in flipDirection: " + dir);
        }
    }
    
}