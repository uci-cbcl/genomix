package edu.uci.ics.genomix.pregelix.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
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
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;

public abstract class BasicGraphCleanVertex<V extends VertexValueWritable, M extends MessageWritable> extends
        Vertex<VKmerBytesWritable, V, NullWritable, M> {
	
	//logger
    public Logger logger = Logger.getLogger(BasicGraphCleanVertex.class.getName());
    
    public static int kmerSize = -1;
    public static int maxIteration = -1;
    
    public static Object lock = new Object();
    public static boolean fakeVertexExist = false;
    public static VKmerBytesWritable fakeVertex = null;
    
    public EDGETYPE[][] connectedTable = new EDGETYPE[][]{
            {EDGETYPE.RF, EDGETYPE.FF},
            {EDGETYPE.RF, EDGETYPE.FR},
            {EDGETYPE.RR, EDGETYPE.FF},
            {EDGETYPE.RR, EDGETYPE.FR}
    };
    
    protected M incomingMsg = null; 
    protected M outgoingMsg = null; 
    protected M aggregatingMsg = null;
    protected VKmerBytesWritable destVertexId = null;
    protected VertexValueWritable tmpValue = new VertexValueWritable(); 
    protected Iterator<VKmerBytesWritable> kmerIterator;
    protected VKmerListWritable kmerList = null;
    protected VKmerBytesWritable repeatKmer = null; //for detect tandemRepeat
    protected EDGETYPE repeatEdgetype; //for detect tandemRepeat
    protected VKmerBytesWritable tmpKmer = null;
    protected short outFlag;
    protected short inFlag;
    protected short selfFlag;
    
    protected EdgeListWritable incomingEdgeList = null; //SplitRepeat and BubbleMerge
    protected EdgeListWritable outgoingEdgeList = null; //SplitRepeat and BubbleMerge
    protected EDGETYPE incomingEdgeType; //SplitRepeat and BubbleMerge
    protected EDGETYPE outgoingEdgeType; //SplitRepeat and BubbleMerge
    
    protected static List<VKmerBytesWritable> problemKmers = null;
    protected boolean debug = false;
    protected boolean verbose = false;
    
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
        
        if (problemKmers == null) {
            problemKmers = new ArrayList<VKmerBytesWritable>();
            if (getContext().getConfiguration().get(GenomixJobConf.DEBUG_KMERS) != null) {
                debug = true;
                for (String kmer : getContext().getConfiguration().get(GenomixJobConf.DEBUG_KMERS).split(","))
                    problemKmers.add(new VKmerBytesWritable(kmer));
            }
        }
                
        verbose = false;
        for (VKmerBytesWritable problemKmer : problemKmers)
            verbose |= debug && (getVertexValue().getNode().findEdge(problemKmer) != null || getVertexId().equals(problemKmer));
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
    
//    public byte getMsgFlagAndMergeDir(){
//        byte flagAndMergeDir = (byte)(getVertexValue().getState() & State.IS_HEAD);
//        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
//        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
//        switch(neighborToMeDir){
//            case MessageFlag.DIR_FF:
//            case MessageFlag.DIR_FR:
//                flagAndMergeDir |= MessageFlag.HEAD_CAN_MERGEWITHPREV;
//                break;
//            case MessageFlag.DIR_RF:
//            case MessageFlag.DIR_RR:
//                flagAndMergeDir |= MessageFlag.HEAD_CAN_MERGEWITHNEXT;
//                break;
//        }
//        return flagAndMergeDir;
//    }
    
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
    public VKmerBytesWritable getPrevDestVertexIdAndSetFlag() {
        if (!getVertexValue().getRFList().isEmpty()){ // #RFList() > 0
            kmerIterator = getVertexValue().getRFList().getKeyIterator();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RF;
            return kmerIterator.next();
        } else if (!getVertexValue().getRRList().isEmpty()){ // #RRList() > 0
            kmerIterator = getVertexValue().getRRList().getKeyIterator();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_RR;
            return kmerIterator.next();
        } else {
            return null;
        }
    }
    
    public VKmerBytesWritable getNextDestVertexIdAndSetFlag() {
        if (!getVertexValue().getFFList().isEmpty()){ // #FFList() > 0
            kmerIterator = getVertexValue().getFFList().getKeyIterator();
            outFlag &= MessageFlag.DIR_CLEAR;
            outFlag |= MessageFlag.DIR_FF;
            return kmerIterator.next();
        } else if (!getVertexValue().getFRList().isEmpty()){ // #FRList() > 0
            kmerIterator = getVertexValue().getFRList().getKeyIterator();
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
            destVertexId.setAsCopy(getDestVertexId(DIR.PREVIOUS));
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
            destVertexId.setAsCopy(getDestVertexId(DIR.NEXT));
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * send message to all neighbor nodes
     */
    public void sendSettledMsgs(DIR direction, VertexValueWritable value){
        //TODO THE less context you send, the better  (send simple messages)
        EnumSet<EDGETYPE> edgeTypes = (direction == DIR.PREVIOUS ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING);
        for(EDGETYPE e : edgeTypes){
            kmerIterator = value.getEdgeList(e).getKeyIterator();
            while(kmerIterator.hasNext()){
                outFlag &= MessageFlag.DIR_CLEAR;
                outFlag |= e.get();
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
        EnumSet<EDGETYPE> edgeTypes = (direction == DIR.PREVIOUS ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING);
        for(EDGETYPE e : edgeTypes){
            kmerIterator = value.getEdgeList(e).getKeyIterator();
            while(kmerIterator.hasNext()){
                outFlag &= MessageFlag.HEAD_CAN_MERGE_CLEAR;
                EDGETYPE meToNeighborEdgeType = e.mirror();
                switch(meToNeighborEdgeType){
                    case FF:
                    case FR:
                        outFlag |= MessageFlag.HEAD_CAN_MERGEWITHPREV;
                        break;
                    case RF:
                    case RR:
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
     * @param destVertex 
     */
    public void copyWithoutTandemRepeats(V srcVertex, VertexValueWritable destVertex){
        destVertex.setAsCopy(srcVertex);
        destVertex.getEdgeList(repeatEdgetype).remove(repeatKmer);
        while(isTandemRepeat(destVertex))
            destVertex.getEdgeList(repeatEdgetype).remove(repeatKmer);
    }
    
//    /**
//     * check if it is valid path
//     */
//    public boolean isValidPath(){
//        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
//        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
//        switch(neighborToMeDir){
//            case MessageFlag.DIR_FF:
//            case MessageFlag.DIR_FR:
//                return getVertexValue().inDegree() == 1;
//            case MessageFlag.DIR_RF:
//            case MessageFlag.DIR_RR:
//                return getVertexValue().outDegree() == 1;
//        }
//        return true;
//    }
    
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
    public void setNeighborToMeDir(DIR direction){
        if(getVertexValue().getDegree(direction) != 1)
            throw new IllegalArgumentException("In merge dir, the degree is not 1");
        EnumSet<EDGETYPE> edgeTypes = direction == DIR.PREVIOUS ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING;
        outFlag &= MessageFlag.DIR_CLEAR;
        
        for (EDGETYPE et : edgeTypes)
            outFlag |= et.get();
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
     * set state as no_merge
     */
    public void setMerge(byte mergeState){
        short state = getVertexValue().getState();
        state &= P4State.MERGE_CLEAR;
        state |= (mergeState & P4State.MERGE_MASK);
        getVertexValue().setState(state);
        activate();
    }
    
    /**
     * Returns the edge dir for B->A when the A->B edge is type @dir
     */
    public byte meToNeighborDir(byte dir) {
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
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        
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
    
    /**
     * Look inside of vertex for the given edge, returning the direction we found it in (or null) 
     */
    public static EDGETYPE findEdge(VKmerBytesWritable id, VertexValueWritable vertex){
        // TODO move into Node?
        for(EDGETYPE e : EnumSet.allOf(EDGETYPE.class)){
            for (VKmerBytesWritable curKey : vertex.getEdgeList(e).getKeys()) {
                if(curKey.equals(id)) // points to self
                    return e;
            }
        }
        return null;
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
            tmpValue.getEdgeList(repeatEdgetype).remove(repeatKmer);
            while(isTandemRepeat(tmpValue))
                tmpValue.getEdgeList(repeatEdgetype).remove(repeatKmer);
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
    public void setEdgeListAndEdgeType(int i){
        incomingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][0]));
        incomingEdgeType = connectedTable[i][0];
        
        outgoingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][1]));
        outgoingEdgeType = connectedTable[i][1];
    }
    
//2013.9.21 ------------------------------------------------------------------//
    /**
     * get destination vertex ex. RemoveTip
     */
    public VKmerBytesWritable getDestVertexId(DIR direction){
        int degree = getVertexValue().getDegree(direction);
        if(degree > 1)
            throw new IllegalArgumentException("degree > 1, getDestVertexId(DIR direction) only can use for degree == 1 + \n" + getVertexValue().toString());
        
        if(degree == 1){
            EnumSet<EDGETYPE> edgeTypes = direction.edgeType();
            for(EDGETYPE et : edgeTypes){
                if(getVertexValue().getEdgeList(et).getCountOfPosition() > 0)
                    return getVertexValue().getEdgeList(et).get(0).getKey();
            }
        }
        //degree in this direction == 0
        throw new IllegalArgumentException("degree > 0, getDestVertexId(DIR direction) only can use for degree == 1 + \n" + getVertexValue().toString());
    }
    
    /**
     * check if I am a tandemRepeat 
     */
    public boolean isTandemRepeat(VertexValueWritable value){
        VKmerBytesWritable kmerToCheck;
        for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
            Iterator<VKmerBytesWritable> it = value.getEdgeList(et).getKeyIterator();
            while(it.hasNext()){
                kmerToCheck = it.next();
                if(kmerToCheck.equals(getVertexId())){
                    repeatEdgetype = et;
                    repeatKmer.setAsCopy(kmerToCheck);
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * broadcastKillself ex. RemoveLow
     */
    public void broadcastKillself(){
        VertexValueWritable vertex = getVertexValue();
        for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
            for(VKmerBytesWritable kmer : vertex.getEdgeList(et).getKeys()){
                outFlag &= EDGETYPE.CLEAR;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                destVertexId = kmer;
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
}