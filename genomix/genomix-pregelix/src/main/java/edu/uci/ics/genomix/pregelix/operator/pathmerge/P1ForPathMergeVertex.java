package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;

/**
 * Graph clean pattern: P1(Naive-algorithm) for path merge 
 * @author anbangx
 *
 */
public class P1ForPathMergeVertex extends
    MapReduceVertex<VertexValueWritable, PathMergeMessageWritable> {
    
    private ArrayList<PathMergeMessageWritable> receivedMsg = new ArrayList<PathMergeMessageWritable>();
    
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
        headFlag = getHeadFlag();
        headMergeDir = getHeadMergeDir();
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
        headMergeDir = getHeadMergeDir();
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
    
//    /**
//     * typical for P1
//     */
//    @Override
//    public void sendMsgToFakeVertex(){
//        outgoingMsg.reset();
//        if(!getVertexValue().isFakeVertex()){
//            outgoingMsg.setSourceVertexId(getVertexId());
//            outgoingMsg.setNode(getVertexValue().getNode());
//            sendMsg(fakeVertex, outgoingMsg);
//            voteToHalt();
//        }
//    }
//    
    /**
     * map reduce in FakeNode
     */
    public void aggregateMsgAndGroupInFakeNode(Iterator<PathMergeMessageWritable> msgIterator){
        kmerMapper.clear();
        /** Mapper **/
        ArrayList<Byte> kmerDir = mapKeyByInternalKmer(msgIterator);
        boolean isFlip = kmerDir.get(0) == kmerDir.get(1) ? false : true;
        /** Reducer **/
        reduceKeyByInternalKmer(isFlip);//isFlip
    }
    
    /**
     * typical for P1
     */
    public void reduceKeyByInternalKmer(boolean isFlip){
        for (VKmerBytesWritable key : kmerMapper.keySet()) {
            kmerList = kmerMapper.get(key);
            //always delete kmerList(1), keep kmerList(0)

            //send kill message to kmerList(1), and carry with kmerList(0) to update edgeLists of kmerList(1)'s neighbor
            outgoingMsg.setFlag(MessageFlag.KILL);
            outgoingMsg.getNode().setInternalKmer(kmerList.getPosition(0));
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
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        if(getVertexValue().getEdgeList(neighborToMeDir).getEdge(incomingMsg.getSourceVertexId()) != null){
            tmpEdge.setAsCopy(getVertexValue().getEdgeList(neighborToMeDir).getEdge(incomingMsg.getSourceVertexId()));
            
            getVertexValue().getEdgeList(neighborToMeDir).remove(incomingMsg.getSourceVertexId());
        }
        tmpEdge.setKey(incomingMsg.getNode().getInternalKmer());
        byte updateDir = flipDirection(neighborToMeDir, incomingMsg.isFlip());
        getVertexValue().getEdgeList(updateDir).unionAdd(tmpEdge);
    }
//    
//    /**
//     * typical for P1
//     */
//    @Override
//    public ArrayList<Byte> mapKeyByInternalKmer(Iterator<PathMergeMessageWritable> msgIterator){
//        ArrayList<Byte> kmerDir = new ArrayList<Byte>();
//        while(msgIterator.hasNext()){
//            incomingMsg = msgIterator.next();
//            
//            //set kmerMapper
//            String kmerString = incomingMsg.getInternalKmer().toString();
//            tmpKmer.setByRead(kmerString.length(), kmerString.getBytes(), 0);
//            reverseKmer.setByReadReverse(kmerString.length(), kmerString.getBytes(), 0);
//
//            VKmerBytesWritable kmer = new VKmerBytesWritable();
//            kmerList = new VKmerListWritable();
//            if(reverseKmer.compareTo(tmpKmer) > 0){
//                kmerDir.add(KmerDir.FORWARD);
//                kmer.setAsCopy(tmpKmer);
//            } else{
//                kmerDir.add(KmerDir.REVERSE);
//                kmer.setAsCopy(reverseKmer);
//            }
//            if(!kmerMapper.containsKey(kmer)){
//                //kmerList.reset();
//                kmerList.append(incomingMsg.getSourceVertexId());
//                kmerMapper.put(kmer, kmerList);
//            } else{
//                kmerList.setCopy(kmerMapper.get(kmer));
//                kmerList.append(incomingMsg.getSourceVertexId());
//                kmerMapper.put(kmer, kmerList);
//            }
//            
//            //set edgesMapper
//            NodeWritable node = new NodeWritable();
//            if(!edgesMapper.containsKey(incomingMsg.getSourceVertexId())){
//                node.setAsCopy(incomingMsg.getNode());
//                edgesMapper.put(incomingMsg.getSourceVertexId(), node);
//            } else{
//                node.setAsCopy(edgesMapper.get(incomingMsg.getSourceVertexId()));
//                edgesMapper.put(kmer, node);
//            }
//        }
//        return kmerDir;
//    }
//    
//    /**
//     * typical for P1
//     */
//    public void reduceKeyByInternalKmer(boolean isFlip){
//        for(VKmerBytesWritable key : kmerMapper.keySet()){
//            kmerList = kmerMapper.get(key);
//            //always delete kmerList(1), keep kmerList(0)
//            
//            //send kill message to kmerList(1), and carry with kmerList(0) to update edgeLists of kmerList(1)'s neighbor
//            outgoingMsg.setFlag(MessageFlag.KILL);
//            outgoingMsg.getNode().setInternalKmer(kmerList.getPosition(0));
//            destVertexId.setAsCopy(kmerList.getPosition(1));
//            sendMsg(destVertexId, outgoingMsg);
//            
//            //send update message to kmerList(0) to add the edgeList of kmerList(1)
//            outgoingMsg.reset();
//            outgoingMsg.setFlag(MessageFlag.UPDATE);
//            outgoingMsg.setFlip(isFlip);
//            for(byte d: DirectionFlag.values)
//                outgoingMsg.setEdgeList(d, edgesMapper.get(kmerList.getPosition(1)).getEdgeList(d));
//            destVertexId.setAsCopy(kmerList.getPosition(0));
//            sendMsg(destVertexId, outgoingMsg);
//        }
//    }
    
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
        else if (getSuperstep() % 4 == 3 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex()){
                if(isHeadNode()){
                    byte headMergeDir = (byte)(getVertexValue().getState() & State.HEAD_SHOULD_MERGE_MASK);
                    switch(headMergeDir){
                        case State.HEAD_SHOULD_MERGEWITHPREV:
                            sendUpdateMsgToSuccessor(true);
                            break;
                        case State.HEAD_SHOULD_MERGEWITHNEXT:
                            sendUpdateMsgToPredecessor(true);
                            break;
                    }
                } else
                    voteToHalt();
            } else{ //is FakeVertex
                // Fake vertex agregates message and group them by actual kmer (1) 
                aggregateMsgAndGroupInFakeNode(msgIterator);
                voteToHalt();
            }
        } else if (getSuperstep() % 4 == 0 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex()){
                while(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    if(isReceiveKillMsg()){
                        outgoingMsg.setInternalKmer(incomingMsg.getNode().getInternalKmer());
                        outgoingMsg.setFlip(incomingMsg.isFlip());
                        broadcaseKillself();
                    } 
//                    else if(isReceiveUpdateMsg()){
//                        for(byte dir : DirectionFlag.values){
//                            byte updateDir = flipDirection(dir, incomingMsg.isFlip());
//                            getVertexValue().getEdgeList(updateDir).unionAdd(incomingMsg.getEdgeList(dir));
//                        }
//                        voteToHalt();
//                    }
                    else{
                        processUpdate();
                        if(isHaltNode())
                            voteToHalt();
                        else
                            activate();
                    }
                }
            }
        } else if (getSuperstep() % 4 == 1 && getSuperstep() <= maxIteration) {
            if(!isFakeVertex()){
                if(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    if(isResponseKillMsg()){
                        responseToDeadVertexAndUpdateEdges();
                        voteToHalt();
                    }
                } else{
                    if(isHeadNode())
                        broadcastMergeMsg(false);
                }
            }
        } else if (getSuperstep() % 4 == 2 && getSuperstep() <= maxIteration) {
            if(!msgIterator.hasNext() && isDeadNode())
                deleteVertex(getVertexId());
            else{
                receivedMsg.clear();
                while(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    receivedMsg.add(incomingMsg);
                }
                if(receivedMsg.size() == 2){ //#incomingMsg == even
                    for(int i = 0; i < 2; i++)
                        processMerge(receivedMsg.get(i));
                    //final vertex
                    voteToHalt();
                } else{
                    boolean isHead = isHeadNode();
                    processMerge(receivedMsg.get(0));
                    if(isHead){
                        // NON-FAKE and Final vertice send msg to FAKE vertex 
                        sendMsgToFakeVertex();
                        voteToHalt();
                    } else
                        activate();
                }
            }
        } else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P1ForPathMergeVertex.class));
    }
}
