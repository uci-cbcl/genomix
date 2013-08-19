package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.io.PathMergeMessageWritable;
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
 * For example, ONE LINE in input file: 00,01,10	0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 * Naive Algorithm for path merge graph
 */
public class P1ForPathMergeVertex extends
    BasicPathMergeVertex {
    
    private ArrayList<PathMergeMessageWritable> receivedMsgList = new ArrayList<PathMergeMessageWritable>();
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(incomingMsg == null)
            incomingMsg = new PathMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new PathMergeMessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        inFlag = 0;
        outFlag = 0;
        receivedMsgList.clear();
    }
    
    public void chooseDirAndSendMsg(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                sendSettledMsgToNextNode();
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                sendSettledMsgToPrevNode();
                break;
        }
    }
    /**
     * head node sends message to path node
     */
    public void sendMsgToPathVertex(Iterator<PathMergeMessageWritable> msgIterator) {
        if (getSuperstep() == 3) {
            if(getVertexValue().getState() == State.IS_HEAD)
                outFlag |= MessageFlag.IS_HEAD;
            sendSettledMsgToAllNextNodes(getVertexValue());
        } else {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                byte stopFlag = (byte) (incomingMsg.getFlag() & MessageFlag.STOP_MASK);
                if (stopFlag != MessageFlag.STOP) {
                    if(incomingMsg.isUpdateMsg())
                        processUpdate();
                    else{
                        processMerge();
                        /** after merge with next node, keep merge with next one **/
                        chooseDirAndSendMsg();
                    }
                } else {
                    processMerge();
                    getVertexValue().setState(State.IS_FINAL);
                }
            }
        }
    }

    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            startSendMsg();
            voteToHalt();
        } else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 2 == 1 && getSuperstep() <= maxIteration) {
            sendMsgToPathVertex(msgIterator);
            voteToHalt();
        } else if (getSuperstep() % 2 == 0 && getSuperstep() > 2 && getSuperstep() <= maxIteration) {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                receivedMsgList.add(incomingMsg);
            }
            /** if receive two messages, break the symmetric **/
            if(receivedMsgList.size() > 0){
                if(VertexUtil.isPathVertex(getVertexValue()) || canMergeWithHead(receivedMsgList.get(0))){
                    /** choose update and merge direction **/
                    sendUpdateMsg(receivedMsgList.get(0));
                    outFlag = 0;
                    sendMergeMsgForP1(receivedMsgList.get(0));
                    deleteVertex(getVertexId());
                }
            }
            voteToHalt();
        } else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(P1ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P1ForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
