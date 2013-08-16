package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.P2PathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.MessageTypeFromHead;
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

    private ArrayList<PathMergeMessageWritable> receivedMsgList = new ArrayList<PathMergeMessageWritable>();
    
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
            incomingMsg = new PathMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new PathMergeMessageWritable();
        else
            outgoingMsg.reset();
        receivedMsgList.clear();
        if(reverseKmer == null)
            reverseKmer = new VKmerBytesWritable();
        if(kmerList == null)
            kmerList = new VKmerListWritable();
        else
            kmerList.reset();
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
        isFakeVertex = ((byte)getVertexValue().getState() & State.FAKEFLAG_MASK) > 0 ? true : false;
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
    }

    /**
     * head send message to path
     */
    public void pathNodeSendOutMsg() {
        //send wantToMerge to next
        tmpKmer = getNextDestVertexIdAndSetFlag();
        if(tmpKmer != null){
            destVertexId.setAsCopy(tmpKmer);
            outgoingMsg.setFlag(outFlag);
            outgoingMsg.setSourceVertexId(getVertexId());
            sendMsg(destVertexId, outgoingMsg);
        }
        
        //send wantToMerge to prev
        tmpKmer = getPrevDestVertexIdAndSetFlag();
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
            return MessageTypeFromHead.BothMsgsFromHead;
        else if(countHead == 1 && countOldHead == 1)
            return MessageTypeFromHead.OneMsgFromOldHeadAndOneFromHead;
        else if(countHead == 1 && countOldHead == 0)
            return MessageTypeFromHead.OneMsgFromHeadAndOneFromNonHead;
        else if(countHead == 0 && countOldHead == 0)
            return MessageTypeFromHead.BothMsgsFromNonHead;
        else
            return MessageTypeFromHead.NO_MSG;
    }

    /**
     * path response message to head
     */
    public void responseMergeMsgToHeadVertex() {
    //      sendUpdateMsg();
          outFlag = 0;
          sendMergeMsg();
    }

    /**
     * head vertex process merge
     */
    public void processMergeInHeadVertex(){
        /** process merge when receiving msg **/
        byte numOfMsgsFromHead = checkNumOfMsgsFromHead();
         switch(numOfMsgsFromHead){
            case MessageTypeFromHead.BothMsgsFromHead:
            case MessageTypeFromHead.OneMsgFromOldHeadAndOneFromHead:
                for(int i = 0; i < 2; i++)
                    processFinalMerge(receivedMsgList.get(i)); //processMerge()
                getVertexValue().setState(State.IS_FINAL);
                /** NON-FAKE and Final vertice send msg to FAKE vertex **/
                sendMsgToFakeVertex();
                voteToHalt();
                break;
            case MessageTypeFromHead.OneMsgFromHeadAndOneFromNonHead:
                for(int i = 0; i < 2; i++)
                    processFinalMerge(receivedMsgList.get(i));
                setHeadState();
                this.activate();
                break;
            case MessageTypeFromHead.BothMsgsFromNonHead:
                for(int i = 0; i < 2; i++)
                    processFinalMerge(receivedMsgList.get(i));
                break;
            case MessageTypeFromHead.NO_MSG:
                //halt
                voteToHalt(); //deleteVertex(getVertexId());
                break;
        }
    }
    
    public void aggregateMsgAndGroupInFakeNode(Iterator<PathMergeMessageWritable> msgIterator){
        kmerMapper.clear();
        /** Mapper **/
        mapKeyByInternalKmer(msgIterator);
        /** Reducer **/
        reduceKeyByInternalKmer();
    }
    
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1){
            addFakeVertex();
            startSendMsg();
        } else if (getSuperstep() == 2){
            if(isFakeVertex)
                voteToHalt();
            initState(msgIterator);
        } else if (getSuperstep() % 3 == 0 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex){
                /** for processing final merge (1) **/
                if(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    if(getMsgFlag() == MessageFlag.IS_FINAL){
                        setFinalState();
                        processFinalMerge(incomingMsg);
                        /** NON-FAKE and Final vertice send msg to FAKE vertex **/
                        sendMsgToFakeVertex();
                    } else if(isReceiveKillMsg()){
                        responseToDeadVertex();
                    }
                }
                /** processing general case **/
                else{
                    if(isPathNode())
                        sendSettledMsgToAllNeighborNodes();
                    if(!isHeadNode())
                        voteToHalt();
                }
            }
            else{
                /** Fake vertex agregates message and group them by actual kmer (2) **/
                aggregateMsgAndGroupInFakeNode(msgIterator);
                voteToHalt();
            }
        } else if (getSuperstep() % 3 == 1 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex){
                /** head doesn't receive msg and send out final msg **/
                if(!msgIterator.hasNext() && isHeadNode()){
                    outFlag |= MessageFlag.IS_FINAL;
                    sendSettledMsgToAllNeighborNodes();
                    voteToHalt();
                } else{
                    while (msgIterator.hasNext()) {
                        incomingMsg = msgIterator.next();
                        /** final Vertex Responses To FakeVertex **/
                        if(isReceiveKillMsg()){
                            broadcaseKillself();
                        }else if(isResponseKillMsg()){
                            responseToDeadVertex();
                            voteToHalt();
                        }
                        else{
                            sendUpdateMsg();
                            outFlag = 0;
                            sendMergeMsg();
                            voteToHalt();
                        }
                    }
                }
            } 
            else{
                /** Fake vertex agregates message and group them by actual kmer (1) **/
                aggregateMsgAndGroupInFakeNode(msgIterator);
                voteToHalt();
            }
        } else if (getSuperstep() % 3 == 2 && getSuperstep() <= maxIteration){
            if(!isFakeVertex){
                while (msgIterator.hasNext()) {
                    incomingMsg = msgIterator.next();
                    /** final Vertex Responses To FakeVertex **/
                    if(isReceiveKillMsg()){
                        broadcaseKillself();
                    } else if(isResponseKillMsg()){
                        responseToDeadVertex();
                        voteToHalt();
                    } else if(getMsgFlag() == MessageFlag.IS_FINAL){/** for final processing, receive msg from head, which means final merge (2) ex. 8**/
                        sendFinalMergeMsg();
                        voteToHalt();
                        break;
                    } else if(incomingMsg.isUpdateMsg()&& selfFlag == State.IS_OLDHEAD){
                        processUpdate();
                    } else if(!incomingMsg.isUpdateMsg()){
                       receivedMsgList.add(incomingMsg);
                    }
                }
                if(receivedMsgList.size() != 0)
                    processMergeInHeadVertex();
            }
        } else
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
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        Client.run(args, job);
    }
}
