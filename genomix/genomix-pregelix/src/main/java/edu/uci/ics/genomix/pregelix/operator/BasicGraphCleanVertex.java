package edu.uci.ics.genomix.pregelix.operator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.logging.Logger;

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
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;

public abstract class BasicGraphCleanVertex<V extends VertexValueWritable, M extends MessageWritable> extends
        Vertex<VKmerBytesWritable, V, NullWritable, M> {
	
	//logger
    public Logger logger = Logger.getLogger(BasicGraphCleanVertex.class.getName());
    
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
    protected M aggregatingMsg = null;
    protected VKmerBytesWritable destVertexId = null;
    protected VertexValueWritable tmpValue = new VertexValueWritable(); 
    protected Iterator<VKmerBytesWritable> kmerIterator;
    protected VKmerListWritable kmerList = null;
    protected VKmerBytesWritable repeatKmer = null; //for detect tandemRepeat
    protected byte repeatDir; //for detect tandemRepeat
    protected VKmerBytesWritable tmpKmer = null;
    protected short outFlag;
    protected short inFlag;
    protected short selfFlag;
    
    protected EdgeListWritable incomingEdgeList = null; //SplitRepeat and BubbleMerge
    protected EdgeListWritable outgoingEdgeList = null; //SplitRepeat and BubbleMerge
    protected byte incomingEdgeDir = 0; //SplitRepeat and BubbleMerge
    protected byte outgoingEdgeDir = 0; //SplitRepeat and BubbleMerge
    
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
    }
    
    public boolean isHeadNode(){
        byte state = (byte)(getVertexValue().getState() & State.VERTEX_MASK);
        return state == State.IS_HEAD;
    }
    
    public boolean isHaltNode(){
        byte state = (byte) (getVertexValue().getState() & State.VERTEX_MASK);
        return state == State.IS_HALT;
    }
    
    public boolean isDeadNode(){
        byte state = (byte) (getVertexValue().getState() & State.VERTEX_MASK);
        return state == State.IS_DEAD;
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
    
    public byte getHeadFlagAndMergeDir(){
        byte flagAndMergeDir = (byte)(getVertexValue().getState() & State.IS_HEAD);
        flagAndMergeDir |= (byte)(getVertexValue().getState() & State.HEAD_CAN_MERGE_MASK);
        return flagAndMergeDir;
    }
    
    public byte getMsgFlagAndMergeDir(){
        byte flagAndMergeDir = (byte)(getVertexValue().getState() & State.IS_HEAD);
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                flagAndMergeDir |= MessageFlag.HEAD_CAN_MERGEWITHPREV;
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                flagAndMergeDir |= MessageFlag.HEAD_CAN_MERGEWITHNEXT;
                break;
        }
        return flagAndMergeDir;
    }
    
    /**
     * set head state
     */
    public void setHeadState(){
        short state = getVertexValue().getState();
        state &= State.VERTEX_CLEAR;
        state |= State.IS_HEAD;
        getVertexValue().setState(state);
    }
    
    /**
     * set final state
     */
    public void setFinalState(){
        short state = getVertexValue().getState();
        state &= State.VERTEX_CLEAR;
        state |= State.IS_FINAL;
        getVertexValue().setState(state);
        this.activate();
    }
    
    /**
     * set stop flag
     */
    public void setStopFlag(){
        short state = getVertexValue().getState();
        state &= State.VERTEX_CLEAR;
        state |= State.IS_FINAL;
        getVertexValue().setState(state);
    }
    
    /**
     * check the message type
     */
    public boolean isReceiveKillMsg(){
        byte killFlag = (byte) (incomingMsg.getFlag() & MessageFlag.KILL_MASK);
        byte deadFlag = (byte) (incomingMsg.getFlag() & MessageFlag.DEAD_MASK);
        return killFlag == MessageFlag.KILL & deadFlag != MessageFlag.DIR_FROM_DEADVERTEX;
    }
    
    public boolean isReceiveUpdateMsg(){
        byte updateFlag = (byte) (incomingMsg.getFlag() & MessageFlag.UPDATE_MASK);
        return updateFlag == MessageFlag.UPDATE;
        
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
    	// TODO check length of RF and RR == 1; throw exception otherwise
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
     * send message to all neighbor nodes
     */
    public void sendSettledMsgs(DIR direction, VertexValueWritable value){
        //TODO THE less context you send, the better  (send simple messages)
        byte dirs[] = direction == DIR.PREVIOUS ? IncomingListFlag.values : OutgoingListFlag.values;
        for(byte dir : dirs){
            kmerIterator = value.getEdgeList(dir).getKeys();
            while(kmerIterator.hasNext()){
                outFlag &= MessageFlag.DIR_CLEAR;
                outFlag |= dir;
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                destVertexId.setAsCopy(kmerIterator.next());
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    public void sendSettledMsgToAllNeighborNodes(VertexValueWritable value) {
        sendSettledMsgs(DIR.PREVIOUS, value);
        sendSettledMsgs(DIR.NEXT, value);
    }
    
    /**
     * head send message to all neighbor nodes
     */
    public void headSendSettledMsgs(DIR direction, VertexValueWritable value){
        //TODO THE less context you send, the better  (send simple messages)
        byte dirs[] = direction == DIR.PREVIOUS ? IncomingListFlag.values : OutgoingListFlag.values;
        for(byte dir : dirs){
            kmerIterator = value.getEdgeList(dir).getKeys();
            while(kmerIterator.hasNext()){
                outFlag &= MessageFlag.HEAD_CAN_MERGE_CLEAR;
                byte meToNeighborDir = mirrorDirection(dir);
                switch(meToNeighborDir){
                    case MessageFlag.DIR_FF:
                    case MessageFlag.DIR_FR:
                        outFlag |= MessageFlag.HEAD_CAN_MERGEWITHPREV;
                        break;
                    case MessageFlag.DIR_RF:
                    case MessageFlag.DIR_RR:
                        outFlag |= MessageFlag.HEAD_CAN_MERGEWITHNEXT;
                        break;  
                }
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                destVertexId.setAsCopy(kmerIterator.next());
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    public void headSendSettledMsgToAllNeighborNodes(VertexValueWritable value) {
        headSendSettledMsgs(DIR.PREVIOUS, value);
        headSendSettledMsgs(DIR.NEXT, value);
    }
    
    /**
     * get a copy of the original Kmer without TandemRepeat
     */
    public void getCopyWithoutTandemRepeats(V vertexValue){
        tmpValue.setAsCopy(vertexValue);
        tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
        while(isTandemRepeat(tmpValue))
            tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
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
     * check if A need to be filpped with neighbor
     */
    public boolean ifFlipWithNeighbor(DIR direction){
        if(direction == DIR.PREVIOUS){
            if(getVertexValue().getRRList().isEmpty())
                return true;
            else
                return false;
        } else{
            if(getVertexValue().getFFList().isEmpty())
                return true;
            else
                return false;
        }
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
     * set neighborToMe Dir
     */
    public void setNeighborToMeDir(DIR predecessorToMe){
        if(getVertexValue().getDegree(predecessorToMe) != 1)
            throw new IllegalArgumentException("In merge dir, the degree is not 1");
        byte[] dirs = predecessorToMe == DIR.PREVIOUS ? IncomingListFlag.values : OutgoingListFlag.values;
        outFlag &= MessageFlag.DIR_CLEAR;
        
        if(getVertexValue().getEdgeList(dirs[0]).getCountOfPosition() == 1){
            outFlag |= dirs[0];
        } else if(getVertexValue().getEdgeList(dirs[1]).getCountOfPosition() == 1){
            outFlag |= dirs[1];
        }
    }
    
    /**
     * set state as no_merge
     */
    public void setStateAsNoMerge(){
        short state = getVertexValue().getState();
    	state &= State.CAN_MERGE_CLEAR;
        state |= State.NO_MERGE;
        getVertexValue().setState(state);
        activate();  //TODO could we be more careful about activate?
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
    public byte flipDirection(byte neighborDir, boolean flip){ // TODO use NodeWritable
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
        synchronized(lock){
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
    }
    
    public boolean isFakeVertex(){
        return ((byte)getVertexValue().getState() & State.FAKEFLAG_MASK) > 0;
    }
    
    public boolean isTandemRepeat(VertexValueWritable value){
        VKmerBytesWritable kmerToCheck;
        for(byte d : DirectionFlag.values){
            Iterator<VKmerBytesWritable> it = value.getEdgeList(d).getKeys();
            while(it.hasNext()){
                kmerToCheck = it.next();
                if(kmerToCheck.equals(getVertexId())){
                    repeatDir = d;
                    repeatKmer.setAsCopy(kmerToCheck);
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
        if(isTandemRepeat(getVertexValue())){
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
                outFlag |= MessageFlag.HEAD_CAN_MERGEWITHPREV;
                getVertexValue().setState(outFlag);
                activate();
            }
            if (VertexUtil.isVertexWithOnlyOneOutgoing(getVertexValue()) && getVertexValue().inDegree() == 0){
                outFlag = 0;
                outFlag |= MessageFlag.IS_HEAD;
                outFlag |= MessageFlag.HEAD_CAN_MERGEWITHNEXT;
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
        if(!VertexUtil.isCanMergeVertex(getVertexValue())
                || isTandemRepeat(getVertexValue())){
            getVertexValue().setState(MessageFlag.IS_HALT);
            voteToHalt();
        }
    }
    
    /**
     * non-head && non-path 
     */
    public boolean isInactiveNode(){
        return !VertexUtil.isCanMergeVertex(getVertexValue()) || isTandemRepeat(getVertexValue());
    }
    
    /**
     * head and path 
     */
    public boolean isActiveNode(){
        return !isInactiveNode();
    }
    
    /**
     * use for SplitRepeatVertex and BubbleMerge
     * @param i
     */
    public void setEdgeListAndEdgeDir(int i){
        incomingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][0]));
        incomingEdgeDir = connectedTable[i][0];
        
        outgoingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][1]));
        outgoingEdgeDir = connectedTable[i][1];
    }
    
}