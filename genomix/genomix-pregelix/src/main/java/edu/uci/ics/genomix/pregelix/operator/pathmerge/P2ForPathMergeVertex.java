package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.P2VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.P2PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.P2PathMergeMessageWritable.P2MessageType;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.MessageType;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
/**
 * Graph clean pattern: P2(Logistics-algorithm) for path merge 
 * @author anbangx
 *
 */
public class P2ForPathMergeVertex extends
    MapReduceVertex<P2VertexValueWritable, P2PathMergeMessageWritable> {

    private ArrayList<P2PathMergeMessageWritable> receivedMsgList = new ArrayList<P2PathMergeMessageWritable>();
    
    private boolean isFakeVertex = false;
    
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        headFlag = (byte)(getVertexValue().getState() & State.IS_HEAD);
        selfFlag = (byte)(getVertexValue().getState() & State.VERTEX_MASK);
        if(incomingMsg == null)
            incomingMsg = new P2PathMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new P2PathMergeMessageWritable();
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
        headMergeDir = getHeadMergeDir();
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        tmpValue.reset();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        else
            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
        if(getSuperstep() == 1){
            getVertexValue().setPrependMergeNode(getVertexValue().getNode());
            getVertexValue().setAppendMergeNode(getVertexValue().getNode());
        }
    }

    /**
     * map reduce in FakeNode
     */
    public void aggregateMsgAndGroupInFakeNode(Iterator<P2PathMergeMessageWritable> msgIterator){
        kmerMapper.clear();
        /** Mapper **/
        mapKeyByInternalKmer(msgIterator);
        /** Reducer **/
        reduceKeyByInternalKmer();
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
            return MessageType.BothMsgsFromHead;
        else if(countHead == 1 && countOldHead == 1)
            return MessageType.OneMsgFromOldHeadAndOneFromHead;
        else if(countHead == 1 && countOldHead == 0)
            return MessageType.OneMsgFromHeadAndOneFromNonHead;
        else if(countHead == 0 && countOldHead == 0)
            return MessageType.BothMsgsFromNonHead;
        else
            return MessageType.NO_MSG;
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
     * send merge message to neighber for P2
     */
    public void sendMergeMsg(){
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(false);
        if(selfFlag == State.IS_HEAD){
            byte state = getVertexValue().getState(); 
            state &= State.VERTEX_CLEAR;
            state |= State.IS_OLDHEAD;
            getVertexValue().setState(state);
            this.activate();
            resetSelfFlag();
            outFlag |= MessageFlag.IS_HEAD;  
        } else if(selfFlag == State.IS_OLDHEAD){
            outFlag |= MessageFlag.IS_OLDHEAD;
            voteToHalt();
        }
        sendP2MergeMsgByIncomingMsgDir();
    }
    
    public void sendP2MergeMsgByIncomingMsgDir(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                configureP2MergeMsgForSuccessor(incomingMsg.getSourceVertexId());
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                configureP2MergeMsgForPredecessor(incomingMsg.getSourceVertexId()); 
                break; 
        }
    }
    
    /**
     * configure MERGE msg For P2
     */
    public void configureP2MergeMsgForPredecessor(VKmerBytesWritable mergeDest){
        setPredecessorToMeDir();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setFlip(ifFilpWithSuccessor());
        outgoingMsg.setNode(getVertexValue().getAppendMergeNode());
        outgoingMsg.setMessageType(P2MessageType.FROM_SUCCESSOR);
        sendMsg(mergeDest, outgoingMsg);
    }
    
    public void configureP2MergeMsgForSuccessor(VKmerBytesWritable mergeDest){
        setSuccessorToMeDir();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setFlip(ifFlipWithPredecessor());
        outgoingMsg.setNode(getVertexValue().getPrependMergeNode());
        outgoingMsg.setMessageType(P2MessageType.FROM_PREDECESSOR);
        sendMsg(mergeDest, outgoingMsg);
    }
    
    /**
     * final merge and updateAdjList  having parameter for p2
     */
    public void processP2Merge(P2PathMergeMessageWritable msg){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK); 
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        getVertexValue().getMergeNode(msg.getMessageType()).mergeWithNode(neighborToMeDir, msg.getNode());
    }
    
    /**
     * head vertex process merge
     */
    public void processMergeInHeadVertex(){
        // process merge when receiving msg 
        byte numOfMsgsFromHead = checkNumOfMsgsFromHead();
         switch(numOfMsgsFromHead){
            case MessageType.BothMsgsFromHead:
            case MessageType.OneMsgFromOldHeadAndOneFromHead:
                for(int i = 0; i < 2; i++)
                    processP2Merge(receivedMsgList.get(i));
                getVertexValue().setState(State.IS_FINAL);
                getVertexValue().processFinalNode();
                // NON-FAKE and Final vertice send msg to FAKE vertex 
                sendMsgToFakeVertex();
                voteToHalt();
                break;
            case MessageType.OneMsgFromHeadAndOneFromNonHead:
                for(int i = 0; i < 2; i++)
                    processP2Merge(receivedMsgList.get(i));
                setHeadState();
                this.activate();
                break;
            case MessageType.BothMsgsFromNonHead:
                for(int i = 0; i < 2; i++)
                    processP2Merge(receivedMsgList.get(i));
                break;
            case MessageType.NO_MSG:
                //halt
                voteToHalt(); //deleteVertex(getVertexId());
                break;
        }
    }
    
    @Override
    public void compute(Iterator<P2PathMergeMessageWritable> msgIterator) {
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
                // for processing final merge (1)
                if(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    if(getMsgFlag() == MessageFlag.IS_FINAL){
                        setFinalState();
                        processP2Merge(incomingMsg);
                        getVertexValue().setState(State.IS_FINAL);
                        getVertexValue().processFinalNode();
                        // NON-FAKE and Final vertice send msg to FAKE vertex 
                        sendMsgToFakeVertex();
                    } else if(isReceiveKillMsg()){
                        responseToDeadVertex();
                    }
                } else{ // processing general case 
                    if(isPathNode())
                        sendSettledMsgToAllNeighborNodes(getVertexValue());
                    if(!isHeadNode())
                        voteToHalt();
                }
            }
            else{
                // Fake vertex agregates message and group them by actual kmer (2)
                aggregateMsgAndGroupInFakeNode(msgIterator);
                voteToHalt();
            }
        } else if (getSuperstep() % 3 == 1 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex){
                // head doesn't receive msg and send out final msg
                if(!msgIterator.hasNext() && isHeadNode()){
                    outFlag |= MessageFlag.IS_FINAL;
                    sendSettledMsgToAllNeighborNodes(getVertexValue());
                    voteToHalt();
                } else{
                    while (msgIterator.hasNext()) {
                        incomingMsg = msgIterator.next();
                        // final Vertex Responses To FakeVertex
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
                // Fake vertex agregates message and group them by actual kmer (1) 
                aggregateMsgAndGroupInFakeNode(msgIterator);
                voteToHalt();
            }
        } else if (getSuperstep() % 3 == 2 && getSuperstep() <= maxIteration){
            if(!isFakeVertex){
                while (msgIterator.hasNext()) {
                    incomingMsg = msgIterator.next();
                    // final Vertex Responses To FakeVertex 
                    if(isReceiveKillMsg()){
                        broadcaseKillself();
                    } else if(isResponseKillMsg()){
                        responseToDeadVertex();
                        voteToHalt();
                    } else if(getMsgFlag() == MessageFlag.IS_FINAL){// for final processing, receive msg from head, which means final merge (2) ex. 8
                        sendFinalMergeMsg();
                        voteToHalt();
                        break;
                    } else if(incomingMsg.isUpdateMsg() && selfFlag == State.IS_OLDHEAD){ // only old head update edges
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
        Client.run(args, getConfiguredJob(null, P2ForPathMergeVertex.class));
    }
}
