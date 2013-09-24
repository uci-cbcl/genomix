package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/**
 * Graph clean pattern: P1(Naive-algorithm) for path merge 
 * @author anbangx
 *
 */
public class P0ForPathMergeVertex extends
    MapReduceVertex<VertexValueWritable, PathMergeMessageWritable> {
    
    private ArrayList<PathMergeMessageWritable> receivedMsg = new ArrayList<PathMergeMessageWritable>();
    private HashMap<VKmerBytesWritable, ArrayList<Byte>> dirMapper = new HashMap<VKmerBytesWritable, ArrayList<Byte>>();
    
    private EdgeWritable tmpEdge = new EdgeWritable();
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        super.initVertex();
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
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        tmpValue.reset();
        synchronized(lock){
            if(fakeVertex == null){
                fakeVertex = new VKmerBytesWritable();
                String fake = generateString(kmerSize + 1);//generaterRandomString(kmerSize + 1);
                fakeVertex.setByRead(kmerSize + 1, fake.getBytes(), 0); 
            }
        }
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
        if(reverseKmer == null)
            reverseKmer = new VKmerBytesWritable();
        if(kmerList == null)
            kmerList = new VKmerListWritable();
        else
            kmerList.reset();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    /**
     * map reduce in FakeNode
     */
    public void aggregateMsgAndGroupInFakeNode(Iterator<PathMergeMessageWritable> msgIterator){
        kmerMapper.clear();
        dirMapper.clear();
        /** Mapper **/
        mapKeyByInternalKmer(msgIterator);
        /** Reducer **/
        reduceKeyByInternalKmer();
    }
    
    /**
     * typical for P1
     */
    @Override
    public void mapKeyByInternalKmer(Iterator<PathMergeMessageWritable> msgIterator){
        byte dir = 0;
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            String kmerString = incomingMsg.getInternalKmer().toString();
            tmpKmer.setByRead(kmerString.length(), kmerString.getBytes(), 0);
            reverseKmer.setByReadReverse(kmerString.length(), kmerString.getBytes(), 0);

            VKmerBytesWritable kmer = new VKmerBytesWritable();
            kmerList = new VKmerListWritable();
            if(reverseKmer.compareTo(tmpKmer) > 0){
                dir = KmerDir.FORWARD;
                kmer.setAsCopy(tmpKmer);
            }
            else{
                dir = KmerDir.REVERSE;
                kmer.setAsCopy(reverseKmer);
            }
            if(!kmerMapper.containsKey(kmer)){
                //kmerList.reset();
                kmerList.append(incomingMsg.getSourceVertexId());
                kmerMapper.put(kmer, kmerList);
            } else{
                kmerList.setCopy(kmerMapper.get(kmer));
                kmerList.append(incomingMsg.getSourceVertexId());
                kmerMapper.put(kmer, kmerList);
            }
            //dirMapper
            ArrayList<Byte> dirList = new ArrayList<Byte>();
            if(!dirMapper.containsKey(kmer)){
                dirList.clear();
                dirList.add(dir);
                dirMapper.put(kmer, dirList);
            } else{
                dirList.clear();
                dirList.addAll(dirMapper.get(kmer));
                dirList.add(dir);
                dirMapper.put(kmer, dirList);
            }
        }
    }
    
    /**
     * typical for P1
     */
    @Override
    public void reduceKeyByInternalKmer(){
        for (VKmerBytesWritable key : kmerMapper.keySet()) {
            kmerList = kmerMapper.get(key);
            //always delete kmerList(1), keep kmerList(0)

            //send kill message to kmerList(1), and carry with kmerList(0) to update edgeLists of kmerList(1)'s neighbor
            outgoingMsg.setFlag(MessageFlag.KILL);
            outgoingMsg.getNode().setInternalKmer(kmerList.getPosition(0));
            boolean isFlip = dirMapper.get(key).get(0) == dirMapper.get(key).get(1) ? false : true;
            outgoingMsg.setFlip(isFlip);
            destVertexId.setAsCopy(kmerList.getPosition(1));
            sendMsg(destVertexId, outgoingMsg);
        }
    }
 
    /**
     * typical for P1
     * do some remove operations on adjMap after receiving the info about dead Vertex
     */
    public void responseToDeadVertexAndUpdateEdges(){
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        
        if(getVertexValue().getEdgeList(neighborToMeDir).getEdge(incomingMsg.getSourceVertexId()) != null){
            tmpEdge.setAsCopy(getVertexValue().getEdgeList(neighborToMeDir).getEdge(incomingMsg.getSourceVertexId()));
            
            getVertexValue().getEdgeList(neighborToMeDir).remove(incomingMsg.getSourceVertexId());
        }
        tmpEdge.setKey(incomingMsg.getNode().getInternalKmer());
        EDGETYPE updateDir = incomingMsg.isFlip() ? neighborToMeDir.flip() : neighborToMeDir; 
        getVertexValue().getEdgeList(updateDir).unionAdd(tmpEdge);
    }
    
    /**
     * head send update message
     */
    public void headSendUpdateMsg(){
        if(isHeadNode()){
            byte headMergeDir = (byte)(getVertexValue().getState() & State.HEAD_CAN_MERGE_MASK);
            switch(headMergeDir){
                case State.HEAD_CAN_MERGEWITHPREV:
                    sendUpdateMsg(isP1, DIR.NEXT);
                    break;
                case State.HEAD_CAN_MERGEWITHNEXT:
                    sendUpdateMsg(isP1, DIR.PREVIOUS);
                    break;
            }
        } else
            voteToHalt();
    }
    
    /**
     * process update when receiving update msg
     */
    public void processUpdateOnceReceiveMsg(Iterator<PathMergeMessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            processUpdate(incomingMsg);
            if(isHaltNode())
                voteToHalt();
            else
                activate();
        }
    }
    /**
     * aggregate received msg
     */
    public void aggregateReceivedMsg(Iterator<PathMergeMessageWritable> msgIterator){
        receivedMsg.clear();
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            receivedMsg.add(incomingMsg);
        }
    }
    
    /**
     * process message directly
     */
    public void processMessageDirectly(){
        for(int i = 0; i < 2; i++)
            processMerge(receivedMsg.get(i));
        //final vertex
        getVertexValue().setState(MessageFlag.IS_HALT);
        voteToHalt();
    }
    
    /**
     * processMerge and sendMsgToFake
     */
    public void processMergeAndSendMsgToFake(){
        boolean isHead = isHeadNode();
        boolean isDead = isDeadNode();
        processMerge(receivedMsg.get(0));
        if(isHead || isDead){
            // NON-FAKE and Final vertice send msg to FAKE vertex 
            sendMsgToFakeVertex();
            //final vertex
            getVertexValue().setState(MessageFlag.IS_HALT);
            voteToHalt();
        } else
            activate();
    }
    
    /**
     * if receive kill msg, broadcaseKillself
     */
    public void broadcaseKillselfOnceReceiveKillMsg(Iterator<PathMergeMessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            if(isReceiveKillMsg()){
                outgoingMsg.setInternalKmer(incomingMsg.getNode().getInternalKmer());
                outgoingMsg.setFlip(incomingMsg.isFlip());
                broadcaseKillself();
            } 
        }
    }
    
    /**
     * if receive dead msg, responseToDeadVertexAndUpdateEdges
     */
    public void responseToDeadVertexAndUpdateEdgesOnceReceiveDeadMsg(Iterator<PathMergeMessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            if(isResponseKillMsg())
                responseToDeadVertexAndUpdateEdges();
        } 
        if(isHeadNode())
            activate();
        else
            voteToHalt();
    }
    
    /**
     * start sending message
     */
    public void startSendMsg() {
        if(isTandemRepeat(getVertexValue())){
            copyWithoutTandemRepeats(getVertexValue(), tmpValue);
            outFlag = 0;
            sendSettledMsgToAllNeighborNodes(tmpValue);
            voteToHalt();
        } else{
            /** check incoming **/
            // update internal state
            if (VertexUtil.isVertexWithOnlyOneIncoming(getVertexValue())){
                byte state = 0;
                state |= State.HEAD_CAN_MERGEWITHPREV;
                getVertexValue().setState(state);
                activate();
            } 
            // send to neighbors
            else if (VertexUtil.isVertexWithManyIncoming(getVertexValue())){
                outFlag = 0;
                sendSettledMsgs(DIR.PREVIOUS, getVertexValue());
                 }
            
            /** check outgoing **/
            // update internal state
            if (VertexUtil.isVertexWithOnlyOneOutgoing(getVertexValue())){
                byte state = 0;
                state |= State.HEAD_CAN_MERGEWITHNEXT;
                getVertexValue().setState(state);
                activate();
            } 
            // send to neighbors
            else if (VertexUtil.isVertexWithManyOutgoing(getVertexValue())){
                outFlag = 0;
                sendSettledMsgs(DIR.NEXT, getVertexValue());
                 }
            
            if(VertexUtil.isUnMergeVertex(getVertexValue()))
                voteToHalt();
         }
     }
    
        public void initState(Iterator<PathMergeMessageWritable> msgIterator) {
                if(isInactiveNode())
                    voteToHalt();
                else{
                    while (msgIterator.hasNext()) {
                        incomingMsg = msgIterator.next();
                        switch(getHeadMergeDir()){
                            case State.PATH_NON_HEAD: // TODO Change name to Path
                                setHeadMergeDir();
                                activate();
                                break;
                            case State.HEAD_CAN_MERGEWITHPREV: // TODO aggregate all the incomingMsgs first, then make a decision about halting
                            case State.HEAD_CAN_MERGEWITHNEXT:
                                if(true){ //if (getHeadFlagAndMergeDir() != getMsgFlagAndMergeDir()){
                                    getVertexValue().setState(State.HEAD_CANNOT_MERGE);
                                    voteToHalt();
                                }
                                break;
                            case State.HEAD_CANNOT_MERGE:
                                voteToHalt();
                                break;
                        }
                    }
                }
        }

    
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            addFakeVertex();
            startSendMsg();
        } else if (getSuperstep() == 2)
            if(!isFakeVertex())
                initState(msgIterator);
            else voteToHalt();
        else if (getSuperstep() % 7 == 3 && getSuperstep() <= maxIteration) {
            //head send update message
            headSendUpdateMsg();
        } else if (getSuperstep() % 7 == 4 && getSuperstep() <= maxIteration) {
            //process update when receiving updateMsg
            processUpdateOnceReceiveMsg(msgIterator);
        } else if (getSuperstep() % 7 == 5 && getSuperstep() <= maxIteration) {
            //head broadcastMergeMsg, while non-head voteToHalt
            if(isHeadNode())
                sendMergeMsg(isP1);
            else
                voteToHalt();
        } else if (getSuperstep() % 7 == 6 && getSuperstep() <= maxIteration) {
            if(!msgIterator.hasNext() && isDeadNode())
                deleteVertex(getVertexId());
            else{
                //aggregate received msg
                aggregateReceivedMsg(msgIterator);
                if(receivedMsg.size() == 2){ //#incomingMsg == even
                    //processMerge directly
                    processMessageDirectly();
                } else if(receivedMsg.size() == 1){
                    //processMerge and sendMsgToFake
                    processMergeAndSendMsgToFake();
                }
            }
        } else if (getSuperstep() % 7 == 0 && getSuperstep() <= maxIteration){
            if(isFakeVertex()){//is FakeVertex
                // Fake vertex agregates message and group them by actual kmer (1) 
                aggregateMsgAndGroupInFakeNode(msgIterator);
                voteToHalt();
            }
        } else if (getSuperstep() % 7 == 1 && getSuperstep() <= maxIteration){
            //if receive kill msg, broadcaseKillself
            broadcaseKillselfOnceReceiveKillMsg(msgIterator);
        } else if (getSuperstep() % 7 == 2 && getSuperstep() <= maxIteration){
            //if receive dead msg, responseToDeadVertexAndUpdateEdges
            responseToDeadVertexAndUpdateEdgesOnceReceiveDeadMsg(msgIterator);
        }
        else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P0ForPathMergeVertex.class));
    }
}
