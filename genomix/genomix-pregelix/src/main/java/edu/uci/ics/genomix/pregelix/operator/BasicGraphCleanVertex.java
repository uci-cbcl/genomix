package edu.uci.ics.genomix.pregelix.operator;

import java.io.IOException;
import java.util.ArrayList;
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
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public abstract class BasicGraphCleanVertex<V extends VertexValueWritable, M extends MessageWritable> extends
        Vertex<VKmerBytesWritable, V, NullWritable, M> {
	
	//logger
    public Logger LOG = Logger.getLogger(BasicGraphCleanVertex.class.getName());
    
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
    
    protected M outgoingMsg = null; 
    protected VertexValueWritable tmpValue = new VertexValueWritable(); 
    protected VKmerBytesWritable repeatKmer = null; //for detect tandemRepeat
    protected EDGETYPE repeatEdgetype; //for detect tandemRepeat
    protected short outFlag;
    protected short inFlag;
    protected short selfFlag;
    
//    protected EdgeListWritable incomingEdgeList = null; //SplitRepeat and BubbleMerge
//    protected EdgeListWritable outgoingEdgeList = null; //SplitRepeat and BubbleMerge
//    protected EDGETYPE incomingEdgeType; //SplitRepeat and BubbleMerge
//    protected EDGETYPE outgoingEdgeType; //SplitRepeat and BubbleMerge
    
    protected static List<VKmerBytesWritable> problemKmers = null;
    protected boolean debug = false;
    protected boolean verbose = false;
    protected boolean logReadIds = false;
    
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
        
        configureDebugOption();
        //TODO fix globalAggregator
    }
    
    public void configureDebugOption(){
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
    
    /**
     * check the message type
     */
    public boolean isReceiveKillMsg(M incomingMsg){
        byte killFlag = (byte) (incomingMsg.getFlag() & MessageFlag.KILL_MASK);
        byte deadFlag = (byte) (incomingMsg.getFlag() & MessageFlag.DEAD_MASK);
        return killFlag == MessageFlag.KILL2 & deadFlag != MessageFlag.DIR_FROM_DEADVERTEX;
    }
    
    public boolean isReceiveUpdateMsg(M incomingMsg){
        byte updateFlag = (byte) (incomingMsg.getFlag() & MessageFlag.UPDATE_MASK);
        return updateFlag == MessageFlag.UPDATE;
        
    }
    
    public boolean isResponseKillMsg(M incomingMsg){
        byte killFlag = (byte) (incomingMsg.getFlag() & MessageFlag.KILL_MASK);
        byte deadFlag = (byte) (incomingMsg.getFlag() & MessageFlag.DEAD_MASK);
        return killFlag == MessageFlag.KILL2 & deadFlag == MessageFlag.DIR_FROM_DEADVERTEX; 
    }
    
    /**
     * send message to all neighbor nodes
     */
    public void sendSettledMsgs(DIR direction, VertexValueWritable value){
        //TODO THE less context you send, the better  (send simple messages)
        EnumSet<EDGETYPE> edgeTypes = (direction == DIR.REVERSE ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING);
        Iterator<VKmerBytesWritable> kmerIterator;
        for(EDGETYPE e : edgeTypes){
            kmerIterator = value.getEdgeList(e).getKeyIterator();
            while(kmerIterator.hasNext()){
                outFlag &= MessageFlag.DIR_CLEAR;
                outFlag |= e.get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                VKmerBytesWritable destVertexId = kmerIterator.next();
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    public void sendSettledMsgToAllNeighborNodes(VertexValueWritable value) {
        sendSettledMsgs(DIR.REVERSE, value);
        sendSettledMsgs(DIR.FORWARD, value);
    }
    
    /**
     * set neighborToMe Dir
     */
    public void setNeighborToMeDir(DIR direction){
        if(getVertexValue().getDegree(direction) != 1)
            throw new IllegalArgumentException("In merge dir, the degree is not 1");
        EnumSet<EDGETYPE> edgeTypes = direction == DIR.REVERSE ? EDGETYPE.INCOMING : EDGETYPE.OUTGOING;
        outFlag &= MessageFlag.DIR_CLEAR;
        
        for (EDGETYPE et : edgeTypes)
            outFlag |= et.get();
    }
    
    /**
     * broadcast kill self to all neighbers ***
     */
    public void broadcaseKillself(){
        outFlag = 0;
        outFlag |= MessageFlag.KILL2;
        outFlag |= MessageFlag.DIR_FROM_DEADVERTEX;
        
        sendSettledMsgToAllNeighborNodes(getVertexValue());
        
        deleteVertex(getVertexId());
    }
    
    /**
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertex(M incomingMsg){
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
     * set statistics counter
     */
    public void incrementCounter(byte counterName){
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
    
//    /**
//     * use for SplitRepeatVertex and BubbleMerge
//     * @param i
//     */
//    public void setEdgeListAndEdgeType(int i){
//        incomingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][0]));
//        incomingEdgeType = connectedTable[i][0];
//        
//        outgoingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][1]));
//        outgoingEdgeType = connectedTable[i][1];
//    }
    
//2013.9.21 ------------------------------------------------------------------//
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
     * get destination vertex ex. RemoveTip
     */
    public VKmerBytesWritable getDestVertexId(DIR direction){
        int degree = getVertexValue().getDegree(direction);
        if(degree > 1)
            throw new IllegalArgumentException("degree > 1, getDestVertexId(DIR direction) only can use for degree == 1 + \n" + getVertexValue().toString());
        
        if(degree == 1){
            EnumSet<EDGETYPE> edgeTypes = direction.edgeType();
            for(EDGETYPE et : edgeTypes){
                if(getVertexValue().getEdgeList(et).size() > 0)
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
        for(EDGETYPE et : EDGETYPE.values()){
            for(VKmerBytesWritable dest : vertex.getEdgeList(et).getKeys()){
                outgoingMsg.reset();
                outFlag &= EDGETYPE.CLEAR;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(dest, outgoingMsg);
                if(verbose){
                	LOG.fine("Iteration " + getSuperstep() + "\r\n"
                			+ "Vertex Id: " + getVertexId() + "\r\n"
                			+ "Vertex Value: " + getVertexValue() + "\r\n\n"); // TODO killSelf in log
                }
            }
        }
    }
    
    /**
     * response to dead node  //TODO use general remove and process update function
     */
    public void responseToDeadNode(Iterator<M> msgIterator){
        if(verbose){
            LOG.fine("Before update " + "\r\n"
                    + "My vertexId is " + getVertexId() + "\r\n"
                    + "My vertexValue is " + getVertexValue() + "\r\n\n");
        }
        MessageWritable incomingMsg;
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            EDGETYPE meToNeighborEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            getVertexValue().getEdgeList(meToNeighborEdgetype).remove(incomingMsg.getSourceVertexId());
            
            if(verbose){
                LOG.fine("Receive message from dead node!" + incomingMsg.getSourceVertexId() + "\r\n"
                        + "The deadToMeEdgetype in message is: " + meToNeighborEdgetype + "\r\n\n");
            }
        }
        if(verbose){
            LOG.fine("After update " + "\r\n"
                    + "My vertexId is " + getVertexId() + "\r\n"
                    + "My vertexValue is " + getVertexValue() + "\r\n\n");
        }
    }
}