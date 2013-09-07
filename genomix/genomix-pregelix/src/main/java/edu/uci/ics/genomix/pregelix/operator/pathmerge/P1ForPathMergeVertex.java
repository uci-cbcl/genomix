package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;

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
    MapReduceVertex<VertexValueWritable, PathMergeMessageWritable> {
    
    private ArrayList<PathMergeMessageWritable> receivedMsg = new ArrayList<PathMergeMessageWritable>();
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
    }
    
    /**
     * map reduce in FakeNode
     */
    public void aggregateMsgAndGroupInFakeNode(Iterator<PathMergeMessageWritable> msgIterator){
        kmerMapper.clear();
        /** Mapper **/
        ArrayList<Byte> kmerDir = mapKeyByInternalKmer(msgIterator);
        boolean isFlip = kmerDir.get(0) == kmerDir.get(1) ? false : true;
        /** Reducer **/
        reduceKeyByInternalKmer(isFlip);
    }
    
    /**
     * typical for P1
     */
    public void reduceKeyByInternalKmer(boolean isFlip){
        for(VKmerBytesWritable key : kmerMapper.keySet()){
            kmerList = kmerMapper.get(key);
            //always delete kmerList(1), keep kmerList(0)
            
            //send kill message to kmerList(1), and carry with kmerList(0) to update edgeLists of kmerList(1)'s neighbor
            outgoingMsg.setFlag(MessageFlag.KILL);
            outgoingMsg.getNode().setInternalKmer(kmerList.getPosition(0));
            destVertexId.setAsCopy(kmerList.getPosition(1));
            sendMsg(destVertexId, outgoingMsg);
            
            //send update message to kmerList(0) to add the edgeList of kmerList(1)
            outgoingMsg.reset();
            outgoingMsg.setFlag(MessageFlag.UPDATE);
            outgoingMsg.setFlip(isFlip);
            for(byte d: DirectionFlag.values)
                outgoingMsg.setEdgeList(d, getVertexValue().getEdgeList(d));
            destVertexId.setAsCopy(kmerList.getPosition(0));
            sendMsg(destVertexId, outgoingMsg);
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
            while(msgIterator.hasNext()){
                //TODO
                if(isReceiveKillMsg()){
                    broadcaseKillself();
                } else if(isReceiveUpdateMsg()){
                    
                } else{
                    incomingMsg = msgIterator.next();
                    processUpdate();
                    if(isHaltNode())
                        voteToHalt();
                    else
                        activate();
                }
            }
        } else if (getSuperstep() % 4 == 1 && getSuperstep() <= maxIteration) {
            if(isHeadNode())
                broadcastMergeMsg(false);
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
                    }
                }
            }
        } else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, P1ForPathMergeVertex.class));
    }
}
