package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;
import java.util.Random;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable.IncomingListFlag;
import edu.uci.ics.genomix.type.NodeWritable.OutgoingListFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * Graph clean pattern: P4(Smart-algorithm) for path merge 
 * @author anbangx
 *
 */
public class P4ForPathMergeVertex extends
    BasicPathMergeVertex<VertexValueWritable, PathMergeMessageWritable> {
    
    private static long randSeed = 1; //static for save memory
    private float probBeingRandomHead = -1;
    private Random randGenerator = null;
    
    private VKmerBytesWritable curKmer = new VKmerBytesWritable();
    private VKmerBytesWritable nextKmer = new VKmerBytesWritable();
    private VKmerBytesWritable prevKmer = new VKmerBytesWritable();
    private boolean hasNext;
    private boolean hasPrev;
    private boolean curHead;
    private boolean nextHead;
    private boolean prevHead;
    
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(incomingMsg == null)
            incomingMsg = new PathMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new PathMergeMessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        randSeed = Long.parseLong(getContext().getConfiguration().get(GenomixJobConf.P4_RANDOM_SEED)); // also can use getSuperstep(), because it is better to debug under deterministically random
        if(randGenerator == null)
            randGenerator = new Random(randSeed); 
        if (probBeingRandomHead < 0)
            probBeingRandomHead = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD));
        hasNext = false;
        hasPrev = false;
        curHead = false;
        nextHead = false;
        prevHead = false;
        outFlag = (byte)0;
        inFlag = (byte)0;
        // Node may be marked as head b/c it's a real head or a real tail
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        tmpValue.reset();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    protected boolean isNodeRandomHead(VKmerBytesWritable nodeKmer) {
        // "deterministically random", based on node id
        randGenerator.setSeed((randSeed ^ nodeKmer.hashCode()) * 10000 * getSuperstep());//randSeed + nodeID.hashCode()
        for(int i = 0; i < 500; i++)
            randGenerator.nextFloat();
        return randGenerator.nextFloat() < probBeingRandomHead;
    }
    
    /**
     * set prevKmer to the element that's previous (in the node's RR or RF list), returning true when there is a previous neighbor
     */
    protected boolean setPrevInfo(VertexValueWritable value) {
        if(getHeadMergeDir() == State.HEAD_CAN_MERGEWITHNEXT)
            return false;
        for(byte dir : IncomingListFlag.values){
            if(value.getEdgeList(dir).getCountOfPosition() > 0){
                prevKmer = value.getEdgeList(dir).get(0).getKey(); 
                prevHead = isNodeRandomHead(prevKmer);
                return true;
            }
        }
        return false;
    }
    
    /**
     * set nextKmer to the element that's next (in the node's FF or FR list), returning true when there is a next neighbor
     */
    protected boolean setNextInfo(VertexValueWritable value) {
        if(getHeadMergeDir() == State.HEAD_CAN_MERGEWITHPREV)
            return false;
    	// TODO make sure the degree is correct
        for(byte dir : OutgoingListFlag.values){
            if(value.getEdgeList(dir).getCountOfPosition() > 0){
                nextKmer = value.getEdgeList(dir).get(0).getKey(); 
                nextHead = isNodeRandomHead(nextKmer);
                return true;
            }
        }
        return false;
    }
    
    /**
     * step1 : sendUpdates
     */
    public void sendUpdates(){
        //initiate merge_dir
        setStateAsNoMerge();
        
        // only PATH vertices are present. Find the ID's for my neighbors
        curKmer = getVertexId();
        curHead = isNodeRandomHead(curKmer);
        
        // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
        // We prevent merging towards non-path nodes
        hasNext = setNextInfo(getVertexValue());  // TODO make this false if the node is restricted by its neighbors or by structure(when you combine steps 2 and 3) 
        hasPrev = setPrevInfo(getVertexValue());
        if (hasNext || hasPrev) {
            if (curHead) {
                if (hasNext && !nextHead) {
                    // compress this head to the forward tail
                    setStateAsMergeDir(mergeWithNext);
                    sendUpdateMsg(isP4, toPredecessor);
                } else if (hasPrev && !prevHead) {
                    // compress this head to the reverse tail
                    setStateAsMergeDir(mergeWithPrev);
                    sendUpdateMsg(isP4, toSuccessor);
                } 
            }
            else {
                // I'm a tail
                if (hasNext && hasPrev) {
                     if ((!nextHead && !prevHead) && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
                        // tails on both sides, and I'm the "local minimum"
                        // compress me towards the tail in forward dir
                        setStateAsMergeDir(mergeWithNext);
                        sendUpdateMsg(isP4, toPredecessor);
                    }
                } else if (!hasPrev) {
                    // no previous node
                    if (!nextHead && curKmer.compareTo(nextKmer) < 0) {
                        // merge towards tail in forward dir
                        setStateAsMergeDir(mergeWithNext);
                        sendUpdateMsg(isP4, toPredecessor);
                    }
                } else if (!hasNext) {
                    // no next node
                    if (!prevHead && curKmer.compareTo(prevKmer) < 0) {
                        // merge towards tail in reverse dir
                        setStateAsMergeDir(mergeWithPrev);
                        sendUpdateMsg(isP4, toSuccessor);
                    }
                }
            }
        }  // TODO else voteToHalt (when I combine steps 2 and 3)
        this.activate();
    }
    
    /**
     * step2: receiveUpdates
     */
    public void receiveUpdates(Iterator<PathMergeMessageWritable> msgIterator){
        //update neighber
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            processUpdate(incomingMsg);
        }
        if(isInactiveNode() || isHeadUnableToMerge()) // check structure and neighbor restriction 
            voteToHalt();
        else
            activate();
    }
    
    /**
     * step4: processMerges 
     */
    public void receiveMerges(Iterator<PathMergeMessageWritable> msgIterator){
        //merge tmpKmer
        while (msgIterator.hasNext()) {
            boolean selfFlag = (getHeadMergeDir() == State.HEAD_CAN_MERGEWITHPREV || getHeadMergeDir() == State.HEAD_CAN_MERGEWITHNEXT);
            incomingMsg = msgIterator.next();
            /** process merge **/
            processMerge(incomingMsg);
            // set statistics counter: Num_MergedNodes
            updateStatisticsCounter(StatisticsCounter.Num_MergedNodes);
            /** if it's a tandem repeat, which means detecting cycle **/
            if(isTandemRepeat(getVertexValue())){
                // set statistics counter: Num_Cycles
                updateStatisticsCounter(StatisticsCounter.Num_Cycles); 
                voteToHalt();  // TODO make sure you're checking structure to preclude tandem repeats
            }/** head meets head, stop **/ 
            else if(!VertexUtil.isCanMergeVertex(getVertexValue()) || isHeadMeetsHead(selfFlag)){
                getVertexValue().setState(State.HEAD_CANNOT_MERGE);
                // set statistics counter: Num_MergedPaths
                updateStatisticsCounter(StatisticsCounter.Num_MergedPaths);
                voteToHalt();
            }else{
                activate();
            }
            getVertexValue().setCounters(counters);
        }
    }
    
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 4 == 3)
            sendUpdates();
        else if (getSuperstep() % 4 == 0)  
            receiveUpdates(msgIterator);
        else if (getSuperstep() % 4 == 1){
            broadcastMergeMsg(true);
        } else if (getSuperstep() % 4 == 2)
            receiveMerges(msgIterator);
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P4ForPathMergeVertex.class));
    }
}
