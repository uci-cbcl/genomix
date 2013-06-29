package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;
import edu.uci.ics.genomix.type.PositionWritable;
/*
 * vertexId: BytesWritable
 * vertexValue: VertexValueWritable
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
public class LogAlgorithmForPathMergeVertex extends
    BasicPathMergeVertex {

    private KmerBytesWritableFactory kmerFactory = new KmerBytesWritableFactory(1);
    private KmerBytesWritable lastKmer = new KmerBytesWritable(1);

    byte headFlag;
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        headFlag = (byte)0;
        outgoingMsg.reset();
    }

    /**
     * head send message to path
     */
    public void sendOutMsg() {
        //send wantToMerge to next
        destVertexId.set(getNextDestVertexId(getVertexValue()));
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        sendMsg(destVertexId, outgoingMsg);
        
        ////send wantToMerge to prev
        destVertexId.set(getPreDestVertexId(getVertexValue()));
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        sendMsg(destVertexId, outgoingMsg);
    }

    /**
     * head send message to path
     */
    public void sendMsgToPathVertex(Iterator<MessageWritable> msgIterator) {
        if(getSuperstep() == 3){
            headFlag = (byte)(getVertexValue().getState() | MessageFlag.IS_HEAD);
            if(headFlag == 0)
                sendOutMsg();
        } else {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                processMerge();
                headFlag = (byte)(incomingMsg.getFlag() | MessageFlag.IS_HEAD);
                if(headFlag > 0)
                    getVertexValue().setState(MessageFlag.IS_HEAD);
            }
        }
    }

    /**
     * path response message to head
     */
    public void responseMsgToHeadVertex(Iterator<MessageWritable> msgIterator) {
        sendMergeMsg();
    }

    /**
     * merge chainVertex and store in vertexVal.chainVertexId
     */
    public boolean mergeChainVertex() {
        //merge chain
        lastKmer.set(kmerFactory.getLastKmerFromChain(incomingMsg.getLengthOfChain() - kmerSize + 1,
                incomingMsg.getChainVertexId()));
        KmerBytesWritable chainVertexId = kmerFactory.mergeTwoKmer(getVertexValue().getKmer(), lastKmer);
        getVertexValue().setKmer(chainVertexId);
        getVertexValue().setOutgoingList(incomingMsg.getNeighberNode());
        if (VertexUtil.isCycle(kmerFactory.getFirstKmerFromChain(kmerSize, getVertexValue().getKmer()),
                chainVertexId, kmerSize)) {
            getVertexValue().setState(State.CYCLE);
            return false;
        } 
        return true;
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 2 == 1 && getSuperstep() <= maxIteration) {
            sendMsgToPathVertex(msgIterator);
            voteToHalt();
        } else if (getSuperstep() % 2 == 0 && getSuperstep() <= maxIteration) {
            responseMsgToHeadVertex(msgIterator);
            voteToHalt();
        } else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(LogAlgorithmForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(LogAlgorithmForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput~/
         */
        job.setVertexInputFormatClass(LogAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(LogAlgorithmForPathMergeOutputFormat.class);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        Client.run(args, job);
    }
}
