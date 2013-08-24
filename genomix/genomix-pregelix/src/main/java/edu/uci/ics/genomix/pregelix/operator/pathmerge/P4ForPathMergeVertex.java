package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: MessageWritable
 * 
 * DNA:
 * A: 00
 * C: 01
 * G: 10
 * T: 11
 * 
 * succeed node
 *  A 00000001 1
 *  G 00000010 2
 *  C 00000100 4
 *  T 00001000 8
 * precursor node
 *  A 00010000 16
 *  G 00100000 32
 *  C 01000000 64
 *  T 10000000 128
 *  
 * For example, ONE LINE in input file: 00,01,10    0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 * Naive Algorithm for path merge graph
 */
public class P4ForPathMergeVertex extends
    BasicPathMergeVertex {

    private static long randSeed = 1;
    private float probBeingRandomHead = -1;
    private Random randGenerator;
    
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
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
        if(incomingMsg == null)
            incomingMsg = new PathMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new PathMergeMessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        randSeed = getSuperstep();
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
        headFlag = getHeadFlag();
        headMergeDir = getHeadMergeDir();
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        tmpValue.reset();
    }

    protected boolean isNodeRandomHead(VKmerBytesWritable nodeKmer) {
        // "deterministically random", based on node id
        randGenerator.setSeed((randSeed ^ nodeKmer.hashCode()) * 100000 * getSuperstep());//randSeed + nodeID.hashCode()
        for(int i = 0; i < 500; i++)
            randGenerator.nextFloat();
        return randGenerator.nextFloat() < probBeingRandomHead;
    }
    
    /**
     * set nextKmer to the element that's next (in the node's FF or FR list), returning true when there is a next neighbor
     */
    protected boolean setNextInfo(VertexValueWritable value) {
        if (value.getFFList().getCountOfPosition() > 0) {
            nextKmer.setAsCopy(value.getFFList().get(0).getKey());
            nextHead = isNodeRandomHead(nextKmer);
            return true;
        }
        if (value.getFRList().getCountOfPosition() > 0) {
            nextKmer.setAsCopy(value.getFRList().get(0).getKey());
            nextHead = isNodeRandomHead(nextKmer);
            return true;
        }
        return false;
    }

    /**
     * set prevKmer to the element that's previous (in the node's RR or RF list), returning true when there is a previous neighbor
     */
    protected boolean setPrevInfo(VertexValueWritable value) {
        if (value.getRRList().getCountOfPosition() > 0) {
            prevKmer.setAsCopy(value.getRRList().get(0).getKey());
            prevHead = isNodeRandomHead(prevKmer);
            return true;
        }
        if (value.getRFList().getCountOfPosition() > 0) {
            prevKmer.setAsCopy(value.getRFList().get(0).getKey());
            prevHead = isNodeRandomHead(prevKmer);
            return true;
        }
        return false;
    }
    
    /**
     * set statistics counter
     */
    public void updateStatisticsCounter(byte counterName){
        ByteWritable counterNameWritable = new ByteWritable(counterName);
        HashMapWritable<ByteWritable, VLongWritable> counters = getVertexValue().getCounters();
        if(counters.containsKey(counterNameWritable))
            counters.get(counterNameWritable).set(counters.get(counterNameWritable).get() + 1);
        else
            counters.put(counterNameWritable, new VLongWritable(1));
    }
    
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 4 == 3){
            outFlag |= headFlag;
            
            outFlag |= MessageFlag.NO_MERGE;
            setStateAsNoMerge();
            
            // only PATH vertices are present. Find the ID's for my neighbors
            curKmer.setAsCopy(getVertexId());
            
            curHead = isNodeRandomHead(curKmer);
            
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            hasNext = setNextInfo(getVertexValue()) && (headFlag == 0 || (headFlag > 0 && headMergeDir == MessageFlag.HEAD_SHOULD_MERGEWITHNEXT));
            hasPrev = setPrevInfo(getVertexValue()) && (headFlag == 0 || (headFlag > 0 && headMergeDir == MessageFlag.HEAD_SHOULD_MERGEWITHPREV));
            if (hasNext || hasPrev) {
                if (curHead) {
                    if (hasNext && !nextHead) {
                        // compress this head to the forward tail
                		sendUpdateMsgToPredecessor(); 
                    } else if (hasPrev && !prevHead) {
                        // compress this head to the reverse tail
                        sendUpdateMsgToSuccessor();
                    } 
                }
                else {
                    // I'm a tail
                    if (hasNext && hasPrev) {
                         if ((!nextHead && !prevHead) && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
                            // tails on both sides, and I'm the "local minimum"
                            // compress me towards the tail in forward dir
                            sendUpdateMsgToPredecessor();
                        }
                    } else if (!hasPrev) {
                        // no previous node
                        if (!nextHead && curKmer.compareTo(nextKmer) < 0) {
                            // merge towards tail in forward dir
                            sendUpdateMsgToPredecessor();
                        }
                    } else if (!hasNext) {
                        // no next node
                        if (!prevHead && curKmer.compareTo(prevKmer) < 0) {
                            // merge towards tail in reverse dir
                            sendUpdateMsgToSuccessor();
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
                processUpdate();
                if(isHaltNode())
                    voteToHalt();
                else
                    this.activate();
            }
        } else if (getSuperstep() % 4 == 1){
            //send message to the merge object and kill self
            broadcastMergeMsg();
        } else if (getSuperstep() % 4 == 2){
            //merge tmpKmer
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                selfFlag = (byte) (State.VERTEX_MASK & getVertexValue().getState());
                /** process merge **/
                processMerge();
                // set statistics counter: MergedNodes
                updateStatisticsCounter(StatisticsCounter.MergedNodes);
                /** if it's a tandem repeat, which means detecting cycle **/
                if(isTandemRepeat()){
                    for(byte d : DirectionFlag.values)
                        getVertexValue().getEdgeList(d).reset();
                    getVertexValue().setState(MessageFlag.IS_HALT);
                    // set statistics counter: TandemRepeats
                    updateStatisticsCounter(StatisticsCounter.TandemRepeats);
                    voteToHalt();
                }/** head meets head, stop **/ 
                else if((getMsgFlag() == MessageFlag.IS_HEAD && selfFlag == MessageFlag.IS_HEAD)){
                    getVertexValue().setState(MessageFlag.IS_HALT);
                    // set statistics counter: MergedPaths
                    updateStatisticsCounter(StatisticsCounter.MergedPaths);
                    voteToHalt();
                }
                else
                    this.activate();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf) throws IOException {
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(P4ForPathMergeVertex.class.getSimpleName());
        else
            job = new PregelixJob(conf, P4ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P4ForPathMergeVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
