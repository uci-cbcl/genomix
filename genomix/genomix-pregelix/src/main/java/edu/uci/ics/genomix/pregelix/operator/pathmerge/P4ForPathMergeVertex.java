package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;
import java.util.Random;


import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

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
    public static final String RANDSEED = "P4ForPathMergeVertex.randSeed";
    public static final String PROBBEINGRANDOMHEAD = "P4ForPathMergeVertex.probBeingRandomHead";

    private static long randSeed = 1;
    private float probBeingRandomHead = -1;
    private Random randGenerator;
    
    private PositionWritable curID = new PositionWritable();
    private PositionWritable nextID = new PositionWritable();
    private PositionWritable prevID = new PositionWritable();
    private boolean hasNext;
    private boolean hasPrev;
    private boolean curHead;
    private boolean nextHead;
    private boolean prevHead;
    private byte selfFlag;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        //if (randSeed < 0)
        //    randSeed = getContext().getConfiguration().getLong("randomSeed", 0);
        randSeed = getSuperstep();
        randGenerator = new Random(randSeed);
        if (probBeingRandomHead < 0)
            probBeingRandomHead = getContext().getConfiguration().getFloat("probBeingRandomHead", 0.5f);
        hasNext = false;
        hasPrev = false;
        curHead = false;
        nextHead = false;
        prevHead = false;
        outFlag = (byte)0;
        inFlag = (byte)0;
        // Node may be marked as head b/c it's a real head or a real tail
        headFlag = (byte) (State.IS_HEAD & getVertexValue().getState());
        outgoingMsg.reset();
    }

    protected boolean isNodeRandomHead(PositionWritable nodeID) {
        // "deterministically random", based on node id
        //randGenerator.setSeed(randSeed);
        //randSeed = randGenerator.nextInt();
        randGenerator.setSeed((randSeed ^ nodeID.hashCode()) * 100000 * getSuperstep());//randSeed + nodeID.hashCode()
        return randGenerator.nextFloat() < probBeingRandomHead;
    }
    
    /**
     * set nextID to the element that's next (in the node's FF or FR list), returning true when there is a next neighbor
     */


    protected boolean setNextInfo(VertexValueWritable value) {
        if (value.getFFList().getCountOfPosition() > 0) {
            nextID.set(value.getFFList().getPosition(0));
            nextHead = isNodeRandomHead(nextID);
            return true;
        }
        if (value.getFRList().getCountOfPosition() > 0) {
            nextID.set(value.getFRList().getPosition(0));
            nextHead = isNodeRandomHead(nextID);
            return true;
        }
        return false;
    }

    /**
     * set prevID to the element that's previous (in the node's RR or RF list), returning true when there is a previous neighbor
     */
     
    protected boolean setPrevInfo(VertexValueWritable value) {
        if (value.getRRList().getCountOfPosition() > 0) {
            prevID.set(value.getRRList().getPosition(0));
            prevHead = isNodeRandomHead(prevID);
            return true;
        }
        if (value.getRFList().getCountOfPosition() > 0) {
            prevID.set(value.getRFList().getPosition(0));
            prevHead = isNodeRandomHead(prevID);
            return true;
        }
        return false;
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 4 == 3){
            //tailFlag = (byte) (MessageFlag.IS_TAIL & getVertexValue().getState());
            //outFlag = (byte) (headFlag | tailFlag);
            outFlag |= headFlag;
            
            outFlag |= MessageFlag.NO_MERGE;
            setStateAsNoMerge();
            
            // only PATH vertices are present. Find the ID's for my neighbors
            curID.set(getVertexId());
            
            curHead = isNodeRandomHead(curID);
            
            
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            hasNext = setNextInfo(getVertexValue());//&& headFlag == 0;
            hasPrev = setPrevInfo(getVertexValue());//&& headFlag == 0;
            if (hasNext || hasPrev) {
                if (curHead) {
                    if (hasNext && !nextHead) {
                        // compress this head to the forward tail
                		sendUpdateMsgToPredecessor(); //TODO up -> update  From -> to
                    } else if (hasPrev && !prevHead) {
                        // compress this head to the reverse tail
                        sendUpdateMsgToSuccessor();
                    } //else
                        //voteToHalt();
                }
            }else {
                    // I'm a tail
                    if (hasNext && hasPrev) {
                         if ((!nextHead && !prevHead) && (curID.compareTo(nextID) < 0 && curID.compareTo(prevID) < 0)) {
                            // tails on both sides, and I'm the "local minimum"
                            // compress me towards the tail in forward dir
                            sendUpdateMsgToPredecessor();
                        } //else
                            //voteToHalt();
                    } else if (!hasPrev) {
                        // no previous node
                        if (!nextHead && curID.compareTo(nextID) < 0) {
                            // merge towards tail in forward dir
                            sendUpdateMsgToPredecessor();
                        } //else
                            //voteToHalt();
                    } else if (!hasNext) {
                        // no next node
                        if (!prevHead && curID.compareTo(prevID) < 0) {
                            // merge towards tail in reverse dir
                            sendUpdateMsgToSuccessor();
                        } //else
                            //voteToHalt();
                    } //else
                        //voteToHalt();
                }
        }
        else if (getSuperstep() % 4 == 0){
            //update neighber
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                processUpdate();
                if(VertexUtil.isHeadOrRearVertexWithDegree(getVertexValue()))
                    voteToHalt();
            }
        } else if (getSuperstep() % 4 == 1){
            //send message to the merge object and kill self
            broadcastMergeMsg();
        } else if (getSuperstep() % 4 == 2){
            //merge kmer
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                selfFlag = (byte) (State.VERTEX_MASK & getVertexValue().getState());
                processMerge();
                
                //head meets head, stop
                if(getMsgFlag() == MessageFlag.IS_HEAD && selfFlag == MessageFlag.IS_HEAD)
                    voteToHalt();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(P4ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P4ForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
