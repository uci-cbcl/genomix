package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.MessageFromHead;
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
    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
    
    byte headFlag;
    byte oldHeadFlag;
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
     * check received message
     */
    public byte checkNumOfMsgsFromHead(){
        int countHead = 0;
        int countOldHead = 0;
        for(int i = 0; i < receivedMsgList.size(); i++){
            if((byte)(receivedMsgList.get(i).getFlag() | MessageFlag.IS_HEAD) > 0)
                countHead++;
            if((byte)(receivedMsgList.get(i).getFlag() | MessageFlag.IS_OLDHEAD) > 0)
                countOldHead++;
        }
        if(countHead == 0 && countOldHead == 0)
            return MessageFromHead.BothMsgsFromNonHead;
        else if(countHead == 2)
            return MessageFromHead.BothMsgsFromHead;
        else if(countOldHead == 2)
            return MessageFromHead.BothMsgsFromOldHead;
        else if(countHead == 1)
            return MessageFromHead.OneMsgFromHead;
        else if(countOldHead == 1)
            return MessageFromHead.OneMsgFromNonHead;
        
        return MessageFromHead.NO_INFO;
    }

    /**
     * head send message to path
     */
    public void sendMsgToPathVertex(Iterator<MessageWritable> msgIterator) {
        //process merge when receiving msg
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            receivedMsgList.add(incomingMsg);
        }
        if(receivedMsgList.size() != 0){
            byte numOfMsgsFromHead = checkNumOfMsgsFromHead();
            switch(numOfMsgsFromHead){
                case MessageFromHead.BothMsgsFromNonHead:
                    for(int i = 0; i < 2; i++)
                        processMerge(receivedMsgList.get(i));
                    break;
                case MessageFromHead.BothMsgsFromHead:
                    for(int i = 0; i < 2; i++)
                        processMerge(receivedMsgList.get(i));
                    getVertexValue().setState(MessageFlag.IS_FINAL);
                    break;
                case MessageFromHead.BothMsgsFromOldHead:
                    deleteVertex(getVertexId());
                    break;
                case MessageFromHead.OneMsgFromHead:
                    for(int i = 0; i < 2; i++)
                        processMerge(receivedMsgList.get(i));
                    getVertexValue().setState(MessageFlag.IS_HEAD);
                    break;
                case MessageFromHead.OneMsgFromNonHead:
                    //halt
                    voteToHalt();
                    break;
            }
        } else 
            voteToHalt();
        //send out wantToMerge msg
        headFlag = (byte)(getVertexValue().getState() | MessageFlag.IS_HEAD);
        oldHeadFlag = (byte)(getVertexValue().getState() | MessageFlag.IS_OLDHEAD);
        outFlag = (byte)(headFlag | oldHeadFlag);
        if(outFlag == 0)
            sendOutMsg();
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
            //voteToHalt();
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
