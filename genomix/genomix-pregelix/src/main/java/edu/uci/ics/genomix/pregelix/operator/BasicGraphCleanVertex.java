package edu.uci.ics.genomix.pregelix.operator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

public abstract class BasicGraphCleanVertex<V extends VertexValueWritable, M extends MessageWritable> extends
        Vertex<VKmerBytesWritable, V, NullWritable, M> {
    public static int kmerSize = -1;
    public static int maxIteration = -1;
    
    public static Object lock = new Object();
    public static boolean fakeVertexExist = false;
    public static VKmerBytesWritable fakeVertex = null;
    
    public byte[][] connectedTable = new byte[][]{
            {MessageFlag.DIR_RF, MessageFlag.DIR_FF},
            {MessageFlag.DIR_RF, MessageFlag.DIR_FR},
            {MessageFlag.DIR_RR, MessageFlag.DIR_FF},
            {MessageFlag.DIR_RR, MessageFlag.DIR_FR}
    };
    
    protected M incomingMsg = null; 
    protected M outgoingMsg = null; 
    protected VKmerBytesWritable destVertexId = null;
    protected VertexValueWritable tmpValue = new VertexValueWritable(); 
    protected Iterator<VKmerBytesWritable> kmerIterator;
    protected VKmerListWritable kmerList = null;
    protected VKmerBytesWritable repeatKmer = null; //for detect tandemRepeat
    protected byte repeatDir; //for detect tandemRepeat
    protected VKmerBytesWritable tmpKmer = null;
    protected byte headFlag;
    protected byte outFlag;
    protected byte inFlag;
    protected byte selfFlag;
    protected byte headMergeDir;
    
    protected EdgeListWritable incomingEdgeList = null; //SplitRepeat
    protected EdgeListWritable outgoingEdgeList = null; //SplitRepeat
    protected byte incomingEdgeDir = 0; //SplitRepeat
    protected byte outgoingEdgeDir = 0; //SplitRepeat
    
    protected HashMapWritable<ByteWritable, VLongWritable> counters = new HashMapWritable<ByteWritable, VLongWritable>();
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());

        
//        if (getSuperstep() == 1) {
//            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
//            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
//            GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
//        }
    }
    
    /**
     * reset headFlag
     */
    public void resetHeadFlag(){
        headFlag = (byte)(getVertexValue().getState() & State.IS_HEAD);
    }
    
    public byte getHeadFlag(){
        if(getVertexValue().getState() == MessageFlag.IS_HALT)
            return 0;
        return (byte)(getVertexValue().getState() & State.IS_HEAD);
    }
    
    public boolean isHeadNode(){
        return getHeadFlag() > 0;
    }
    
    /**
     * reset selfFlag
     */
    public void resetSelfFlag(){
        selfFlag = (byte)(getVertexValue().getState() & MessageFlag.VERTEX_MASK);
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
    
    public byte getHeadFlagAndMergeDir(){
        byte flagAndMergeDir = (byte)(getVertexValue().getState() & State.IS_HEAD);
        flagAndMergeDir |= (byte)(getVertexValue().getState() & State.HEAD_SHOULD_MERGE_MASK);
        return flagAndMergeDir;
    }
    
    public byte getMsgFlagAndMergeDir(){
        byte flagAndMergeDir = (byte)(getVertexValue().getState() & State.IS_HEAD);
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                flagAndMergeDir |= MessageFlag.HEAD_SHOULD_MERGEWITHPREV;
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                flagAndMergeDir |= MessageFlag.HEAD_SHOULD_MERGEWITHNEXT;
                break;
        }
        return flagAndMergeDir;
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
    
    public boolean isDeadNode(){
        return getVertexValue().getState() == State.IS_DEAD;
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
    public void sendSettledMsgToAllPrevNodes(VertexValueWritable value) {
        kmerIterator = value.getRFList().getKeys(); // RFList
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RF;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = value.getRRList().getKeys(); // RRList
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
    public void sendSettledMsgToAllNextNodes(VertexValueWritable value) {
        kmerIterator = value.getFFList().getKeys(); // FFList
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FF;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        kmerIterator = value.getFRList().getKeys(); // FRList
        while(kmerIterator.hasNext()){
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FR;
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.setAsCopy(kmerIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    public void sendSettledMsgToAllNeighborNodes(VertexValueWritable value) {
        sendSettledMsgToAllPrevNodes(value);
        sendSettledMsgToAllNextNodes(value);
    }
    
    /**
     * start sending message
     */
    public void startSendMsg() {
        if(isTandemRepeat()){
            tmpValue.setAsCopy(getVertexValue());
            tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
            while(isTandemRepeat(tmpValue))
                tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
            outFlag = 0;
            outFlag |= MessageFlag.IS_HEAD;
            sendSettledMsgToAllNeighborNodes(tmpValue);
        } else{
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
                sendSettledMsgToAllPrevNodes(getVertexValue());
            }
            if (VertexUtil.isVertexWithManyOutgoing(getVertexValue())) {
                outFlag = 0;
                outFlag |= MessageFlag.IS_HEAD;
                sendSettledMsgToAllNextNodes(getVertexValue());
            }
        }
        if(!VertexUtil.isActiveVertex(getVertexValue())
                || isTandemRepeat()){
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
            if(isHaltNode())
                voteToHalt();
            else if(getHeadFlag() != MessageFlag.IS_HEAD && !isTandemRepeat()){
                if(isValidPath()){
                    setHeadMergeDir();
                    activate();
                } else{
                    getVertexValue().setState(MessageFlag.IS_HALT);
                    voteToHalt();
                }
            } else if(getHeadFlagAndMergeDir() == getMsgFlagAndMergeDir()){
                activate();
            } else{ /** already set up **/
                /** if headMergeDir are not the same **/
                getVertexValue().setState(MessageFlag.IS_HALT);
                voteToHalt();
            }
        }
    }
    
    /**
     * check if it is valid path
     */
    public boolean isValidPath(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                return getVertexValue().inDegree() == 1;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                return getVertexValue().outDegree() == 1;
        }
        return true;
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
    public void setPredecessorToMeDir(){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(!getVertexValue().getRFList().isEmpty())
            outFlag |= MessageFlag.DIR_RF;
        else if(!getVertexValue().getRRList().isEmpty())
            outFlag |= MessageFlag.DIR_RR;
    }
    
    public void setPredecessorToMeDir(VKmerBytesWritable toFind){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(getVertexValue().getRFList().contains(toFind))
            outFlag |= MessageFlag.DIR_RF;
        else if(getVertexValue().getRRList().contains(toFind))
            outFlag |= MessageFlag.DIR_RR;
    }
    
    /**
     * set adjMessage to successor(from predecessor)
     */
    public void setSuccessorToMeDir(){
        outFlag &= MessageFlag.DIR_CLEAR;
        if(!getVertexValue().getFFList().isEmpty())
            outFlag |= MessageFlag.DIR_FF;
        else if(!getVertexValue().getFRList().isEmpty())
            outFlag |= MessageFlag.DIR_FR;
    }
    
    public void setSuccessorToMeDir(VKmerBytesWritable toFind){
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
        
        sendSettledMsgToAllNeighborNodes(getVertexValue());
        
        deleteVertex(getVertexId());
    }
    
    /**
     * broadcast kill self to all neighbers ***
     */
    public void broadcaseReallyKillself(){
        outFlag = 0;
        outFlag |= MessageFlag.KILL;
        outFlag |= MessageFlag.DIR_FROM_DEADVERTEX;
        
        sendSettledMsgToAllNeighborNodes(getVertexValue());
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
    
    public boolean isFakeVertex(){
        return ((byte)getVertexValue().getState() & State.FAKEFLAG_MASK) > 0;
    }
    
    /**
     * check if it is a tandem repeat
     */
    public boolean isTandemRepeat(){
        for(byte d : DirectionFlag.values){
            Iterator<VKmerBytesWritable> it = getVertexValue().getEdgeList(d).getKeys();
            while(it.hasNext()){
                repeatKmer.setAsCopy(it.next());
                if(repeatKmer.equals(getVertexId())){
                    repeatDir = d;
                    return true;
                }
            }
        }
        return false;
    }
    
    public boolean isTandemRepeat(VertexValueWritable value){
        for(byte d : DirectionFlag.values){
            Iterator<VKmerBytesWritable> it = value.getEdgeList(d).getKeys();
            while(it.hasNext()){
                repeatKmer.setAsCopy(it.next());
                if(repeatKmer.equals(getVertexId())){
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
    
    /**
     * use for SplitRepeatVertex
     * @param i
     */
    public void setEdgeListAndEdgeDir(int i){
        incomingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][0]));
        incomingEdgeDir = connectedTable[i][0];
        
        outgoingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][1]));
        outgoingEdgeDir = connectedTable[i][1];
    }
    
    
    /**
     * set statistics counter
     */
    public void updateStatisticsCounter(byte counterName){
        ByteWritable counterNameWritable = new ByteWritable(counterName);
        if(counters.containsKey(counterNameWritable))
            counters.get(counterNameWritable).set(counters.get(counterNameWritable).get() + 1);
        else
            counters.put(counterNameWritable, new VLongWritable(1));
    }
    
    /**
     * read statistics counters
     * @param conf
     * @return
     */
    public static HashMapWritable<ByteWritable, VLongWritable> readStatisticsCounterResult(Configuration conf) {
        try {
            VertexValueWritable value = (VertexValueWritable) IterationUtils
                    .readGlobalAggregateValue(conf, BspUtils.getJobId(conf));
            return value.getCounters();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf, Class<? extends BasicGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass) throws IOException {
        // the following class weirdness is because java won't let me get the runtime class in a static context :(
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(vertexClass.getSimpleName());
        else
            job = new PregelixJob(conf, vertexClass.getSimpleName());
        job.setGlobalAggregatorClass(StatisticsAggregator.class);
        job.setVertexClass(vertexClass);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
    
    /**
     * start sending message for P2
     */
    public void startSendMsgForP2() {
        if(isTandemRepeat()){
            tmpValue.setAsCopy(getVertexValue());
            tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
            while(isTandemRepeat(tmpValue))
                tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
            outFlag = 0;
            outFlag |= MessageFlag.IS_HEAD;
            sendSettledMsgToAllNeighborNodes(tmpValue);
        } else{
            if (VertexUtil.isVertexWithOnlyOneIncoming(getVertexValue()) && getVertexValue().outDegree() == 0){
                outFlag = 0;
                outFlag |= MessageFlag.IS_HEAD;
                outFlag |= MessageFlag.HEAD_SHOULD_MERGEWITHPREV;
                getVertexValue().setState(outFlag);
                activate();
            }
            if (VertexUtil.isVertexWithOnlyOneOutgoing(getVertexValue()) && getVertexValue().inDegree() == 0){
                outFlag = 0;
                outFlag |= MessageFlag.IS_HEAD;
                outFlag |= MessageFlag.HEAD_SHOULD_MERGEWITHNEXT;
                getVertexValue().setState(outFlag);
                activate();
            }
            if(getVertexValue().inDegree() > 1 || getVertexValue().outDegree() > 1){
                outFlag = 0;
                outFlag |= MessageFlag.IS_HEAD;
                sendSettledMsgToAllNeighborNodes(getVertexValue());
                getVertexValue().setState(MessageFlag.IS_HALT);
                voteToHalt();
            }
        }
        if(!VertexUtil.isActiveVertex(getVertexValue())
                || isTandemRepeat()){
            getVertexValue().setState(MessageFlag.IS_HALT);
            voteToHalt();
        }
    }
    
}