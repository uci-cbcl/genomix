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

    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
    PositionWritable tempPostition = new PositionWritable();

    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        headFlag = (byte)(getVertexValue().getState() & MessageFlag.IS_HEAD);
        selfFlag =(byte)(getVertexValue().getState() & MessageFlag.VERTEX_MASK);
        outgoingMsg.reset();
        receivedMsgList.clear();
    }

    /**
     * head send message to path
     */
    public void sendOutMsg() {
        //send wantToMerge to next
        tempPostition = getNextDestVertexIdAndSetFlag(getVertexValue());
        if(tempPostition != null){
            destVertexId.set(tempPostition);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            sendMsg(destVertexId, outgoingMsg);
        }
        
        ////send wantToMerge to prev
        tempPostition = getPreDestVertexIdAndSetFlag(getVertexValue());
        if(tempPostition != null){
            destVertexId.set(tempPostition);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    /**
     * check received message
     */
    public byte checkNumOfMsgsFromHead(){
        int countHead = 0;
        int countOldHead = 0;
        for(int i = 0; i < receivedMsgList.size(); i++){
            inFlag = receivedMsgList.get(i).getFlag();
            switch(inFlag & MessageFlag.VERTEX_MASK){
                case MessageFlag.IS_HEAD:
                    countHead++;
                    break;
                case MessageFlag.IS_OLDHEAD:
                    countOldHead++;
                    break;
            }
        }
        if(countHead == 2)
            return MessageFromHead.BothMsgsFromHead;
        else if(countHead == 1 && countOldHead == 1)
            return MessageFromHead.OneMsgFromOldHeadAndOneFromHead;
        else if(countHead == 1 && countOldHead == 0)
            return MessageFromHead.OneMsgFromHeadAndOneFromNonHead;
        else if(countHead == 0 && countOldHead == 0)
            return MessageFromHead.BothMsgsFromNonHead;
        else
            return MessageFromHead.NO_MSG;
    }

    /**
     * head send message to path
     */
    public void sendMsgToPathVertex(Iterator<MessageWritable> msgIterator) {
        //send out wantToMerge msg
        if(selfFlag != MessageFlag.IS_HEAD){
                sendOutMsg();
        }
    }

    /**
     * path response message to head
     */
    public void responseMsgToHeadVertex(Iterator<MessageWritable> msgIterator) {
        if(!msgIterator.hasNext() && selfFlag == MessageFlag.IS_HEAD){
            getVertexValue().setState(MessageFlag.IS_STOP);
            sendOutMsg();
        }
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(getMsgFlag() == MessageFlag.IS_FINAL){
                processMerge(incomingMsg);
                getVertexValue().setState(MessageFlag.IS_FINAL);
            }else
                sendMergeMsg();
        }
    }

    /**
     * head vertex process merge
     */
    public void processMergeInHeadVertex(Iterator<MessageWritable> msgIterator){
      //process merge when receiving msg
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(getMsgFlag() == MessageFlag.IS_FINAL){
                setStopFlag();
                sendMergeMsg();
                break;
            }
            receivedMsgList.add(incomingMsg);
        }
        if(receivedMsgList.size() != 0){
            byte numOfMsgsFromHead = checkNumOfMsgsFromHead();
             switch(numOfMsgsFromHead){
                case MessageFromHead.BothMsgsFromHead:
                case MessageFromHead.OneMsgFromOldHeadAndOneFromHead:
                    for(int i = 0; i < 2; i++)
                        processMerge(receivedMsgList.get(i));
                    getVertexValue().setState(MessageFlag.IS_FINAL);
                    voteToHalt();
                    break;
                case MessageFromHead.OneMsgFromHeadAndOneFromNonHead:
                    for(int i = 0; i < 2; i++)
                        processMerge(receivedMsgList.get(i));
                    getVertexValue().setState(MessageFlag.IS_HEAD);
                    break;
                case MessageFromHead.BothMsgsFromNonHead:
                    for(int i = 0; i < 2; i++)
                        processMerge(receivedMsgList.get(i));
                    break;
                case MessageFromHead.NO_MSG:
                    //halt
                    deleteVertex(getVertexId());
                    break;
            }
        }
    }
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 3 == 0 && getSuperstep() <= maxIteration) {
            sendMsgToPathVertex(msgIterator);
            if(selfFlag != MessageFlag.IS_HEAD)
                voteToHalt();
        } else if (getSuperstep() % 3 == 1 && getSuperstep() <= maxIteration) {
            responseMsgToHeadVertex(msgIterator);
            if(selfFlag != MessageFlag.IS_HEAD)
                voteToHalt();
        } else if (getSuperstep() % 3 == 2 && getSuperstep() <= maxIteration){
            processMergeInHeadVertex(msgIterator);
        }else
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
