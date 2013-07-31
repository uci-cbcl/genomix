package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.P2PathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.MessageFromHead;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
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
public class P2ForPathMergeVertex extends
    MapReduceVertex {

    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
    KmerBytesWritable tmpKmer = new KmerBytesWritable();
    
    private boolean isFakeVertex = false;
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        headFlag = (byte)(getVertexValue().getState() & State.IS_HEAD);
        selfFlag = (byte)(getVertexValue().getState() & State.VERTEX_MASK);
        if(incomingMsg == null)
            incomingMsg = new MessageWritable(kmerSize);
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable(kmerSize);
        else
            outgoingMsg.reset(kmerSize);
        receivedMsgList.clear();
        if(reverseKmer == null)
            reverseKmer = new VKmerBytesWritable();
        if(kmerList == null)
            kmerList = new VKmerListWritable();
        else
            kmerList.reset();
        if(fakeVertex == null){
//            fakeVertex = new KmerBytesWritable(kmerSize + 1);
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(random.getBytes(), 0); 
        }
        isFakeVertex = ((byte)getVertexValue().getState() & State.FAKEFLAG_MASK) > 0 ? true : false;
    }

    /**
     * head send message to path
     */
    public void sendOutMsg() {
        //send wantToMerge to next
        tmpKmer = getNextDestVertexIdAndSetFlag(getVertexValue());
        if(tmpKmer != null){
            destVertexId.setAsCopy(tmpKmer);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            sendMsg(destVertexId, outgoingMsg);
        }
        
        //send wantToMerge to prev
        tmpKmer = getPreDestVertexIdAndSetFlag(getVertexValue());
        if(tmpKmer != null){
            destVertexId.setAsCopy(tmpKmer);
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
        if(selfFlag != State.IS_HEAD && selfFlag != State.IS_OLDHEAD){
                sendOutMsg();
        }
    }

    /**
     * path response message to head
     */
    public void responseMsgToHeadVertex(Iterator<MessageWritable> msgIterator) {
        if(!msgIterator.hasNext() && selfFlag == State.IS_HEAD){
            outFlag |= MessageFlag.IS_FINAL;
            sendOutMsg();
        }
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            /** final Vertex Responses To FakeVertex **/
            if((byte)(incomingMsg.getFlag() & MessageFlag.KILL_MASK) == MessageFlag.KILL){
                if((byte)(incomingMsg.getFlag() & MessageFlag.DIR_MASK) == MessageFlag.DIR_FROM_DEADVERTEX){
                    responseToDeadVertex();
                } else{
                    broadcaseKillself();
                }
            }else if(getMsgFlag() == MessageFlag.IS_FINAL){
                processMerge(incomingMsg);
                getVertexValue().setState(State.IS_FINAL);
            }else{
                sendUpdateMsg();
                outFlag = 0;
                sendMergeMsg();
            }
        }
    }

    /**
     * head vertex process merge
     */
    public void processMergeInHeadVertex(Iterator<MessageWritable> msgIterator){
      //process merge when receiving msg
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            /** final Vertex Responses To FakeVertex **/
            if((byte)(incomingMsg.getFlag() & MessageFlag.KILL_MASK) == MessageFlag.KILL){
                if((byte)(incomingMsg.getFlag() & MessageFlag.DIR_MASK) == MessageFlag.DIR_FROM_DEADVERTEX){
                    responseToDeadVertex();
                } else{
                    broadcaseKillself();
                }
            } else {
                /** for final processing **/
                if(getMsgFlag() == MessageFlag.IS_FINAL){
                    sendFinalMergeMsg();
                    break;
                }
                if(incomingMsg.isUpdateMsg() && selfFlag == State.IS_OLDHEAD)
                    processUpdate();
                else if(!incomingMsg.isUpdateMsg())
                    receivedMsgList.add(incomingMsg);
            }
        }
        if(receivedMsgList.size() != 0){
            byte numOfMsgsFromHead = checkNumOfMsgsFromHead();
             switch(numOfMsgsFromHead){
                case MessageFromHead.BothMsgsFromHead:
                case MessageFromHead.OneMsgFromOldHeadAndOneFromHead:
                    for(int i = 0; i < 2; i++)
                        processFinalMerge(receivedMsgList.get(i)); //processMerge()
                    getVertexValue().setState(State.IS_FINAL);
                    /** NON-FAKE and Final vertice send msg to FAKE vertex **/
                    sendMsgToFakeVertex();
                    voteToHalt();
                    break;
                case MessageFromHead.OneMsgFromHeadAndOneFromNonHead:
                    for(int i = 0; i < 2; i++)
                        processFinalMerge(receivedMsgList.get(i));
                    setHeadState();
                    break;
                case MessageFromHead.BothMsgsFromNonHead:
                    for(int i = 0; i < 2; i++)
                        processFinalMerge(receivedMsgList.get(i));
                    break;
                case MessageFromHead.NO_MSG:
                    //halt
                    voteToHalt(); //deleteVertex(getVertexId());
                    break;
            }
        }
    }
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1){
            addFakeVertex();
            startSendMsg();
        }
        else if (getSuperstep() == 2){
            if(!msgIterator.hasNext() && isFakeVertex)
                voteToHalt();
            initState(msgIterator);
        }
        else if (getSuperstep() % 3 == 0 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex){
                /** for processing final merge **/
                if(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    if(getMsgFlag() == MessageFlag.IS_FINAL){
                        setFinalState();
                        processFinalMerge(incomingMsg);
                        /** NON-FAKE and Final vertice send msg to FAKE vertex **/
                        sendMsgToFakeVertex();
                    } else if(isKillMsg()){
                        responseToDeadVertex();
                    }
                }
                /** processing general case **/
                else{
                    sendMsgToPathVertex(msgIterator);
                    if(selfFlag != State.IS_HEAD)
                        voteToHalt();
                }
            }
            /** Fake vertex agregates message and group them by actual kmer **/
            else{
                kmerMapper.clear();
                /** Mapper **/
                mapKeyByActualKmer(msgIterator);
                /** Reducer **/
                reduceKeyByActualKmer();
                 voteToHalt();
            }
        } else if (getSuperstep() % 3 == 1 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex){
                responseMsgToHeadVertex(msgIterator);
                if(selfFlag != State.IS_HEAD)
                    voteToHalt();
            } 
            /** Fake vertex agregates message and group them by actual kmer **/
            else{
                kmerMapper.clear();
                /** Mapper **/
                mapKeyByActualKmer(msgIterator);
                /** Reducer **/
                reduceKeyByActualKmer();
                voteToHalt();
            }
        } else if (getSuperstep() % 3 == 2 && getSuperstep() <= maxIteration){
            if(!isFakeVertex)
                processMergeInHeadVertex(msgIterator);
        }else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(P2ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P2ForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput~/
         */
        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(P2PathMergeOutputFormat.class);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        Client.run(args, job);
    }
}
