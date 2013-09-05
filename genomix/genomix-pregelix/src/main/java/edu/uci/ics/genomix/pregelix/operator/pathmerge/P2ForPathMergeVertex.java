package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.KmerAndDirWritable;
import edu.uci.ics.genomix.pregelix.io.P2VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.P2PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.P2PathMergeMessageWritable.P2MessageType;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.MessageType;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;
/**
 * Graph clean pattern: P2(Logistics-algorithm) for path merge 
 * @author anbangx
 *
 */
public class P2ForPathMergeVertex extends
    MapReduceVertex<P2VertexValueWritable, P2PathMergeMessageWritable> {

    private ArrayList<P2PathMergeMessageWritable> receivedMsgList = new ArrayList<P2PathMergeMessageWritable>();
    
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
        headFlag = (byte)(getVertexValue().getState() & State.IS_HEAD);
        selfFlag = (byte)(getVertexValue().getState() & State.VERTEX_MASK);
        outFlag = 0;
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
        synchronized(lock){
            if(fakeVertex == null){
                fakeVertex = new VKmerBytesWritable();
                String fake = generateString(kmerSize + 1);//generaterRandomString(kmerSize + 1);
                fakeVertex.setByRead(kmerSize + 1, fake.getBytes(), 0); 
            }
        }
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
    }
    
    /**
     * 
     */
    public String generateString(int length){
        StringBuffer outputBuffer = new StringBuffer(length);
        for (int i = 0; i < length; i++){
           outputBuffer.append("A");
        }
        return outputBuffer.toString();
    }
    
    /**
     * add fake vertex
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void addFakeVertex(){
        synchronized(lock){
            if(!fakeVertexExist){
                //add a fake vertex
                Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
                vertex.getMsgList().clear();
                vertex.getEdges().clear();
                
                P2VertexValueWritable vertexValue = new P2VertexValueWritable();//kmerSize + 1
                vertexValue.setState(State.IS_FAKE);
                vertexValue.setFakeVertex(true);
                
                vertex.setVertexId(fakeVertex);
                vertex.setVertexValue(vertexValue);
                
                addVertex(fakeVertex, vertex);
                fakeVertexExist = true;
            }
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
     * initiate prepend and append MergeNode
     */
    public void initPrependAndAppendMergeNode(){
        getVertexValue().setPrependMergeNode(getVertexValue().getNode());
        getVertexValue().setAppendMergeNode(getVertexValue().getNode());
        activate();
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
        outgoingMsg.setApexMap(getVertexValue().getApexMap());
        if(selfFlag == State.IS_HEAD){
            byte state = getVertexValue().getState(); 
            state &= State.VERTEX_CLEAR;
            state |= State.IS_OLDHEAD;
            getVertexValue().setState(state);
            this.activate();
            resetSelfFlag();
            outFlag |= MessageFlag.IS_HEAD;
            outFlag |= getVertexValue().getState() & MessageFlag.HEAD_SHOULD_MERGE_MASK;
        } else if(selfFlag == State.IS_OLDHEAD){
            outFlag |= MessageFlag.IS_OLDHEAD;
            voteToHalt();
        }
        sendP2MergeMsgByIncomingMsgDir();
    }
    
    public void headSendMergeMsg(){
        outgoingMsg.reset();
        outgoingMsg.setUpdateMsg(false);
        switch(getVertexValue().getState() & MessageFlag.HEAD_SHOULD_MERGE_MASK){
            case MessageFlag.HEAD_SHOULD_MERGEWITHPREV:
                sendSettledMsgToAllPrevNodes(getVertexValue());
                break;
            case MessageFlag.HEAD_SHOULD_MERGEWITHNEXT:
                sendSettledMsgToAllNextNodes(getVertexValue());
                break;
        }
    }
    
    public void sendP2MergeMsgByIncomingMsgDir(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        switch(meToNeighborDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                outgoingMsg.setMessageType(P2MessageType.FROM_SUCCESSOR);
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                outgoingMsg.setMessageType(P2MessageType.FROM_PREDECESSOR);
                break;
        }
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
        sendMsg(mergeDest, outgoingMsg);
    }
    
    public void configureP2MergeMsgForSuccessor(VKmerBytesWritable mergeDest){
        setSuccessorToMeDir();
        outgoingMsg.setFlag(outFlag);
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setFlip(ifFlipWithPredecessor());
        outgoingMsg.setNode(getVertexValue().getPrependMergeNode());
        sendMsg(mergeDest, outgoingMsg);
    }
    
    /**
     * final merge and updateAdjList  having parameter for p2
     */
    public void processP2Merge(P2PathMergeMessageWritable msg){
        byte meToNeighborDir = (byte) (msg.getFlag() & MessageFlag.DIR_MASK); 
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        getVertexValue().getMergeNode(msg.getMessageType()).mergeWithNode(neighborToMeDir, msg.getNode());
        getVertexValue().getNode().mergeWithNodeWithoutKmer(neighborToMeDir, msg.getNode());
        getVertexValue().getApexMap().putAll(msg.getApexMap());
    }
    
    /**
     * send final merge message to neighber for P2 TODO: optimize Node msg
     */
    public void sendFinalMergeMsg(){
        outFlag |= MessageFlag.IS_FINAL;
        outgoingMsg.setUpdateMsg(false);
        outgoingMsg.setApexMap(getVertexValue().getApexMap());
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        switch(meToNeighborDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                outgoingMsg.setMessageType(P2MessageType.FROM_SUCCESSOR);
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                outgoingMsg.setMessageType(P2MessageType.FROM_PREDECESSOR);
                break;
        }
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                outFlag &= MessageFlag.DIR_CLEAR;
                outFlag |= neighborToMeDir;
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setNode(getVertexValue().getPrependMergeNode());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                outFlag &= MessageFlag.DIR_CLEAR;
                outFlag |= neighborToMeDir;       
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                outgoingMsg.setNode(getVertexValue().getAppendMergeNode());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
                break; 
        }
    }
    
    /**
     * send updateMsg to apex
     */
    public void sendUpdateMsgToApex(){
        outgoingMsg.setUpdateApexEdges(true);
        outgoingMsg.setApexMap(getVertexValue().getApexMap());
        outgoingMsg.setNode(getVertexValue().getNode());
        sendSettledMsgToAllNeighborNodes(getVertexValue());
    }
    
    /**
     * head vertex process merge
     */
    public void processMergeInHeadVertex(){
        // process merge when receiving msg 
        byte numOfMsgsFromHead = checkNumOfMsgsFromHead();
         switch(numOfMsgsFromHead){
            case MessageType.BothMsgsFromHead:
            case MessageType.OneMsgFromOldHeadAndOneFromHead: //ex. 6
                for(int i = 0; i < 2; i++)
                    processP2Merge(receivedMsgList.get(i));
                getVertexValue().setState(State.IS_FINAL);
                getVertexValue().processFinalNode();
                //send updateMsg to apex
                sendUpdateMsgToApex();
                // NON-FAKE and Final vertice send msg to FAKE vertex 
                sendMsgToFakeVertex();
                voteToHalt();
                break;
            case MessageType.OneMsgFromHeadAndOneFromNonHead: //ex. 6
                for(int i = 0; i < 2; i++){
                    //set head should merge dir in state
                    if((receivedMsgList.get(i).getFlag() & MessageFlag.VERTEX_MASK) == MessageFlag.IS_HEAD){
                        byte state =  getVertexValue().getState();
                        state &= MessageFlag.HEAD_SHOULD_MERGE_CLEAR;
                        byte dir = (byte)(receivedMsgList.get(i).getFlag() & MessageFlag.HEAD_SHOULD_MERGE_MASK);
                        switch(receivedMsgList.get(i).getFlag() & MessageFlag.DIR_MASK){
                            case MessageFlag.DIR_FF:
                            case MessageFlag.DIR_RR:
                                state |= dir;
                                break;
                            case MessageFlag.DIR_FR:
                            case MessageFlag.DIR_RF:
                                state |= revertHeadMergeDir(dir);
                                break;    
                        }
                        
                        getVertexValue().setState(state);
                    }
                    processP2Merge(receivedMsgList.get(i));
                }
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
    
    /**
     * check if it is final msg
     */
    public boolean isFinalMergeMsg(){
        return (byte)(getMsgFlag() & MessageFlag.VERTEX_MASK) == MessageFlag.IS_FINAL && !incomingMsg.isUpdateMsg();
    }
    
    public boolean isFinalUpdateMsg(){
        return (byte)(getMsgFlag() & MessageFlag.VERTEX_MASK) == MessageFlag.IS_FINAL && incomingMsg.isUpdateMsg();
    }
    
    /**
     * check if it is a valid update node
     */
    public boolean isValidUpateNode(){
        byte meToNeighborDir = (byte)(incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        boolean flag = false;
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                flag = ((getVertexValue().getState() & MessageFlag.HEAD_SHOULD_MERGE_MASK) == MessageFlag.HEAD_SHOULD_MERGEWITHPREV);
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                flag = ((getVertexValue().getState() & MessageFlag.HEAD_SHOULD_MERGE_MASK) == MessageFlag.HEAD_SHOULD_MERGEWITHNEXT);
                break;
        }
        return isHaltNode() || (isHeadNode() && flag);
    }
    
    /**
     * initiate head, rear and path node for P2
     */
    public void initStateForP2(Iterator<P2PathMergeMessageWritable> msgIterator) {
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            if(isHaltNode())
                voteToHalt();
            else if(getHeadFlag() != MessageFlag.IS_HEAD && !isTandemRepeat()){
                if(isValidPath()){
                    setHeadMergeDir();
                    //set deleteKmer and deleteDir
                    KmerAndDirWritable kmerAndDir = new KmerAndDirWritable();
                    kmerAndDir.setDeleteDir((byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK));
                    kmerAndDir.setDeleteKmer(getVertexId());
                    HashMapWritable<VKmerBytesWritable, KmerAndDirWritable> apexMap = new HashMapWritable<VKmerBytesWritable, KmerAndDirWritable>();
                    apexMap.put(new VKmerBytesWritable(incomingMsg.getSourceVertexId()), kmerAndDir);
                    getVertexValue().setApexMap(apexMap);
                    activate();
                } else{
                    getVertexValue().setState(MessageFlag.IS_HALT);
                    voteToHalt();
                }
            } else if(getHeadFlagAndMergeDir() == getMsgFlagAndMergeDir()){
                activate();
            } else{ // already set up
                // if headMergeDir are not the same
                getVertexValue().setState(MessageFlag.IS_HALT);
                voteToHalt();
            }
        }
    }
    
    /**
     * update apex's edges
     */
    public void updateApexEdges(){
        KmerAndDirWritable deleteEdge = incomingMsg.getApexMap().get(getVertexId());
        if(deleteEdge != null && getVertexValue().getEdgeList(deleteEdge.getDeleteDir()).contains(deleteEdge.getDeleteKmer())) //avoid to delete twice
            getVertexValue().getEdgeList(deleteEdge.getDeleteDir()).remove(deleteEdge.getDeleteKmer());
        processFinalUpdate2();
        getVertexValue().setState(MessageFlag.IS_HALT);
        voteToHalt();
    }
    
    @Override
    public void compute(Iterator<P2PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1){
            addFakeVertex();
            // initiate prependMergeNode and appendMergeNode
            initPrependAndAppendMergeNode();
            startSendMsgForP2();
        } else if (getSuperstep() == 2){
            if(isFakeVertex())
                voteToHalt();
            else
                initStateForP2(msgIterator);
        } else if (getSuperstep() % 3 == 0 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex()){
                if(!msgIterator.hasNext()){
                    // processing general case 
                    if(isPathNode())
                        sendSettledMsgToAllNeighborNodes(getVertexValue());
                    if(!isHeadNode())
                        voteToHalt();
                } else{
                    // for processing final merge (1)
                    while(msgIterator.hasNext()){
                        incomingMsg = msgIterator.next();
                        if(incomingMsg.isUpdateApexEdges()){
                            //update edges in apex
                            updateApexEdges();
                        } else{
                            if(isFinalMergeMsg()){ // ex. 4, 5
                                processP2Merge(incomingMsg);
                                getVertexValue().setState(State.IS_FINAL); // setFinalState();
                                getVertexValue().processFinalNode();
                                //send updateMsg to apex
                                sendUpdateMsgToApex();
                                // NON-FAKE and Final vertice send msg to FAKE vertex 
                                sendMsgToFakeVertex();
                                voteToHalt();
                            } else if(isResponseKillMsg()){
                                responseToDeadVertex();
                                voteToHalt();
                            }
                        }
                    }  
                }
            }
            else{
                // Fake vertex agregates message and group them by actual kmer (2)
                aggregateMsgAndGroupInFakeNode(msgIterator);
                voteToHalt();
            }
        } else if (getSuperstep() % 3 == 1 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex()){
                // head doesn't receive msg and send out final msg, ex. 2, 5
                if(!msgIterator.hasNext() && isHeadNode()){
                    outFlag |= MessageFlag.IS_FINAL;
                    headSendMergeMsg();
                    voteToHalt();
                } else{
                    while (msgIterator.hasNext()) {
                        incomingMsg = msgIterator.next();
                        if(incomingMsg.isUpdateApexEdges()){
                            //update edges in apex
                            updateApexEdges();
                        } else{
                            // final Vertex Responses To FakeVertex
                            if(isReceiveKillMsg()){
                                broadcaseKillself();
                            }else if(isResponseKillMsg()){
                                responseToDeadVertex();
                                voteToHalt();
                            } else{
                                sendUpdateMsg();
                                outFlag = 0;
                                sendMergeMsg();
                                voteToHalt();
                            }
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
            if(!isFakeVertex()){
                while (msgIterator.hasNext()) {
                    incomingMsg = msgIterator.next();
                    // final Vertex Responses To FakeVertex 
                    if(isReceiveKillMsg()){
                        broadcaseKillself();
                    } else if(isResponseKillMsg()){
                        responseToDeadVertex();
                        voteToHalt();
                    } else if(incomingMsg.isUpdateMsg() && (selfFlag == State.IS_OLDHEAD || isValidUpateNode())){// only old head update edges
                        if(!isHaltNode())
                            processUpdate();
                        voteToHalt();
                    } else if(isFinalMergeMsg()){// for final processing, receive msg from head, which means final merge (2) ex. 2, 8
                        sendFinalMergeMsg();
                        voteToHalt();
                        break;
                    } else if(!incomingMsg.isUpdateMsg()){
                       receivedMsgList.add(new P2PathMergeMessageWritable(incomingMsg));
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
