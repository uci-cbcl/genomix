package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;
import java.util.Random;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.log.LoggingType;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
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
        randSeed = getSuperstep(); // TODO use the updated version from this morning
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
        randGenerator.setSeed((randSeed ^ nodeKmer.hashCode()) * getSuperstep());//randSeed + nodeID.hashCode()
        for(int i = 0; i < 500; i++)
            randGenerator.nextFloat();
        return randGenerator.nextFloat() < probBeingRandomHead;
    }
    
    /**
     * set nextKmer to the element that's next (in the node's FF or FR list), returning true when there is a next neighbor
     */
    protected boolean setNextInfo(VertexValueWritable value) {
        if((!isHeadNode() || (isHeadNode() && getHeadMergeDir() == MessageFlag.HEAD_SHOULD_MERGEWITHNEXT)))
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
     * set prevKmer to the element that's previous (in the node's RR or RF list), returning true when there is a previous neighbor
     */
    protected boolean setPrevInfo(VertexValueWritable value) {
        if((!isHeadNode() || (isHeadNode() && getHeadMergeDir() == MessageFlag.HEAD_SHOULD_MERGEWITHPREV)))
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
    
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 4 == 3){
            setStateAsNoMerge();
            
            // only PATH vertices are present. Find the ID's for my neighbors
            curKmer = getVertexId();
            
            curHead = isNodeRandomHead(curKmer);
            
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            hasNext = setNextInfo(getVertexValue()); // TODO HEAD CAN MERGE 
            hasPrev = setPrevInfo(getVertexValue());
            if (hasNext || hasPrev) {
                if (curHead) {
                    if (hasNext && !nextHead) {
                        // compress this head to the forward tail
                		sendUpdateMsgToPredecessor(true); 
                    } else if (hasPrev && !prevHead) {
                        // compress this head to the reverse tail
                        sendUpdateMsgToSuccessor(true);
                    } 
                }
                else {
                    // I'm a tail
                    if (hasNext && hasPrev) {
                         if ((!nextHead && !prevHead) && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
                            // tails on both sides, and I'm the "local minimum"
                            // compress me towards the tail in forward dir
                            sendUpdateMsgToPredecessor(true);
                        }
                    } else if (!hasPrev) {
                        // no previous node
                        if (!nextHead && curKmer.compareTo(nextKmer) < 0) {
                            // merge towards tail in forward dir
                            sendUpdateMsgToPredecessor(true);
                        }
                    } else if (!hasNext) {
                        // no next node
                        if (!prevHead && curKmer.compareTo(prevKmer) < 0) {
                            // merge towards tail in reverse dir
                            sendUpdateMsgToSuccessor(true);
                        }
                    }
                }
            }
            this.activate();
        }
        else if (getSuperstep() % 4 == 0){
            //update neighber
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                /** logging incomingMsg **/
                loggingNode(LoggingType.BEFORE_OPERATIONS);
                loggingMessage(LoggingType.RECEIVE_MSG, incomingMsg, null);
                processUpdate();
                loggingNode(LoggingType.AFTER_UPDATE);
                if(isHaltNode())
                    voteToHalt();
                else
                    activate();
            }
        } else if (getSuperstep() % 4 == 1){
            //send message to the merge object and kill self
            broadcastMergeMsg(true);
        } else if (getSuperstep() % 4 == 2){
            //merge tmpKmer
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                selfFlag = (byte) (State.VERTEX_MASK & getVertexValue().getState());
                /** process merge **/
                processMerge();
                // set statistics counter: Num_MergedNodes
                updateStatisticsCounter(StatisticsCounter.Num_MergedNodes);
                /** if it's a tandem repeat, which means detecting cycle **/
                if(isTandemRepeat()){
                    for(byte d : DirectionFlag.values)
                        getVertexValue().getEdgeList(d).reset();
                    getVertexValue().setState(MessageFlag.IS_HALT);
                    // set statistics counter: Num_TandemRepeats
                    updateStatisticsCounter(StatisticsCounter.Num_TandemRepeats);
                    getVertexValue().setCounters(counters);
                    voteToHalt();
                }/** head meets head, stop **/ 
                else if((getMsgFlag() == MessageFlag.IS_HEAD && selfFlag == MessageFlag.IS_HEAD)){
                    getVertexValue().setState(MessageFlag.IS_HALT);
                    // set statistics counter: Num_MergedPaths
                    updateStatisticsCounter(StatisticsCounter.Num_MergedPaths);
                    getVertexValue().setCounters(counters);
                    voteToHalt();
                }
                else{
                    getVertexValue().setCounters(counters);
                    activate();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P4ForPathMergeVertex.class));
    }
}
