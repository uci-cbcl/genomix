package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;

public class P0ForPathMergeVertex extends
    BasicPathMergeVertex<VertexValueWritable, PathMergeMessageWritable> {
    
    private HashSet<PathMergeMessageWritable> updateMsgs = new HashSet<PathMergeMessageWritable>();
    private HashSet<PathMergeMessageWritable> otherMsgs = new HashSet<PathMergeMessageWritable>();
    private HashSet<PathMergeMessageWritable> neighborMsgs = new HashSet<PathMergeMessageWritable>();
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
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
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    public void chooseMergeDir() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        EnumSet<DIR> restrictedDirs = DIR.enumSetFromByte(state);
        boolean updated = false;
        //initiate merge dir
        state &= P4State.MERGE_CLEAR;
        state |= P4State.NO_MERGE;   //setMerge(P4State.NO_MERGE);
        
        //choose merge dir -- principle: only merge with nextDir
        if(restrictedDirs.size() == 1){
            EDGETYPE edgeType = restrictedDirs.contains(DIR.PREVIOUS) ? vertex.getEdgetypeFromDir(DIR.NEXT) : vertex.getEdgetypeFromDir(DIR.PREVIOUS);
            state |= P4State.MERGE | edgeType.get();
            updated = true;
        }
        
        getVertexValue().setState(state);
        if (updated)
            activate();
        else 
            voteToHalt();
    }
    
    /**
     * step4: receive and process Merges  for P0
     */
    public void receiveMerges(Iterator<PathMergeMessageWritable> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = vertex.getNode();
        short state = vertex.getState();
        boolean updated = false;
        EDGETYPE senderEdgetype;
        @SuppressWarnings("unused")
        int numMerged = 0;
        // aggregate incomingMsg
        ArrayList<PathMergeMessageWritable> receivedMsgList = new ArrayList<PathMergeMessageWritable>();
        while(msgIterator.hasNext())
            receivedMsgList.add(new PathMergeMessageWritable(msgIterator.next()));
        
        if(receivedMsgList.size() > 2)
            throw new IllegalStateException("In path merge, it is impossible to receive more than 2 messages!");
        
        // odd number of nodes
        if(receivedMsgList.size() == 2){
            for(PathMergeMessageWritable msg : receivedMsgList){
              senderEdgetype = EDGETYPE.fromByte(msg.getFlag());
              node.mergeWithNode(senderEdgetype, msg.getNode());
              state |= (byte) (msg.getFlag() & DIR.MASK);  // update incoming restricted directions
              numMerged++;
              updated = true;
              deleteVertex(msg.getSourceVertexId());
            }
        } else if(receivedMsgList.size() == 1){ // even number of nodes
            PathMergeMessageWritable msg = receivedMsgList.get(0);
            senderEdgetype = EDGETYPE.fromByte(msg.getFlag());
            state |= (byte) (msg.getFlag() & DIR.MASK);  // update incoming restricted directions
            VKmerBytesWritable me = getVertexId();
            VKmerBytesWritable other = msg.getSourceVertexId();
            // determine if merge. if head msg meets head and #receiveMsg = 1
            if (DIR.enumSetFromByte(state).containsAll(EnumSet.allOf(DIR.class))){
                if(me.compareTo(other) < 0){
                    node.mergeWithNode(senderEdgetype, msg.getNode());
                    numMerged++;
                    updated = true;
                    deleteVertex(other);
                } else{
                    // broadcast kill self and update pointer to new kmer
                    node.mergeWithNode(senderEdgetype, msg.getNode());
                    outgoingMsg.setSourceVertexId(me);
                    outgoingMsg.getNode().setInternalKmer(other);
                    outFlag = 0;
                    outFlag |= MessageFlag.TO_NEIGHBOR;
                    for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
                        for(VKmerBytesWritable kmer : vertex.getEdgeList(et).getKeys()){
                            EDGETYPE meToNeighbor = et.mirror();
                            EDGETYPE otherToNeighbor = senderEdgetype.causesFlip() ? meToNeighbor.flip() : meToNeighbor;
                            outFlag &= EDGETYPE.CLEAR;
                            outFlag &= MessageFlag.MERGE_DIR_CLEAR;
                            outFlag |= meToNeighbor.get() | otherToNeighbor.get() << 9;
                            outgoingMsg.setFlag(outFlag);
                            destVertexId = kmer;
                            sendMsg(destVertexId, outgoingMsg);
                        }
                    }
                    
//                    EDGETYPE otherToMe = EDGETYPE.fromByte(msg.getFlag());
//                    NodeWritable msgNode = msg.getNode();
//                    msgNode.getEdgeList(otherToMe).remove(me);
//                    outgoingMsg.reset();
//                    outgoingMsg.setSourceVertexId(me);
//                    outgoingMsg.getNode().setInternalKmer(other);
//                    for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
//                        EDGETYPE neighborToOther = senderEdgetype.causesFlip() ? et.flip() : et;
//                        EDGETYPE otherToNeighbor = neighborToOther.mirror();
//                        outFlag = 0;
//                        outFlag |= MessageFlag.TO_NEIGHBOR | et.mirror().get() | otherToNeighbor.get() << 9;
//                        outgoingMsg.setFlag(outFlag);
//                        
//                        for (VKmerBytesWritable dest : msgNode.getEdgeList(senderEdgetype).getKeys()) 
//                            sendMsg(dest, outgoingMsg);
//                    }
                    
                    // 1. send message to other to add edges
//                    outgoingMsg.reset();
//                    for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
//                        outgoingMsg.getNode().setEdgeList(senderEdgetype.causesFlip() ? et.flip() : et,
//                                node.getEdgeList(et));
//                    }
//                    outFlag = 0;
//                    outFlag |= MessageFlag.TO_OTHER;
//                    outgoingMsg.setFlag(outFlag);
//                    sendMsg(other, outgoingMsg);
                    
                    // 2. send message to neighbor to update edge from toMe to toOther
//                    outgoingMsg.reset();
//                    outgoingMsg.setSourceVertexId(me);
//                    outgoingMsg.getNode().setInternalKmer(other);
//                    for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
//                        EDGETYPE meToNeighbor = et.mirror();
//                        EDGETYPE otherToNeighbor = senderEdgetype.causesFlip() ? meToNeighbor.flip() : meToNeighbor;
//                        outFlag = 0;
//                        outFlag |= MessageFlag.TO_NEIGHBOR | meToNeighbor.get() | otherToNeighbor.get() << 9;
//                        outgoingMsg.setFlag(outFlag);
//                        
//                        for (VKmerBytesWritable dest : vertex.getEdgeList(et).getKeys()) 
//                            sendMsg(dest, outgoingMsg);
//                    }
                    
                    state |= P4State.NO_MERGE;
                    vertex.setState(state);
                    voteToHalt();
                }
            } else{
                node.mergeWithNode(senderEdgetype, msg.getNode());
                numMerged++;
                updated = true;
                deleteVertex(other);
            }
        }
        
        if(isTandemRepeat(getVertexValue())) {
            // tandem repeats can't merge anymore; restrict all future merges
            state |= DIR.NEXT.get();
            state |= DIR.PREVIOUS.get();
            updated = true;
//          updateStatisticsCounter(StatisticsCounter.Num_Cycles); 
        }
//      updateStatisticsCounter(StatisticsCounter.Num_MergedNodes);
//      getVertexValue().setCounters(counters);
        if (updated) {
            vertex.setState(state);
            if (DIR.enumSetFromByte(state).containsAll(EnumSet.allOf(DIR.class)))
                voteToHalt();
            else 
                activate();
        }
    }
    
    public void catagorizeMsg(Iterator<PathMergeMessageWritable> msgIterator){
        updateMsgs.clear();
        otherMsgs.clear();
        neighborMsgs.clear();
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            byte msgType = (byte) (incomingMsg.getFlag() & MessageFlag.MSG_MASK);
            switch(msgType){
                case MessageFlag.TO_UPDATE:
                    updateMsgs.add(new PathMergeMessageWritable(incomingMsg));
                    break;
//                case MessageFlag.TO_OTHER:
//                    otherMsgs.add(new PathMergeMessageWritable(incomingMsg));
//                    break;
                case MessageFlag.TO_NEIGHBOR:
                    neighborMsgs.add(new PathMergeMessageWritable(incomingMsg));
                    break;
                default:
                    throw new IllegalStateException("Message types are allowd for only TO_UPDATE, TO_OTHER and TO_NEIGHBOR!");
            }
        }
    }
    
    public void receiveToOther(Iterator<PathMergeMessageWritable> msgIterator){
        VertexValueWritable value = getVertexValue();
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            NodeWritable node = incomingMsg.getNode();
            for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class))
                value.getEdgeList(et).unionAdd(node.getEdgeList(et));
            voteToHalt();
        }
    }
    
    public void receiveToNeighbor(Iterator<PathMergeMessageWritable> msgIterator){
        VertexValueWritable value = getVertexValue();
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            EDGETYPE deleteToMe = EDGETYPE.fromByte(incomingMsg.getFlag());
            EDGETYPE aliveToMe =  EDGETYPE.fromByte((short) (incomingMsg.getFlag() >> 9));
            
            VKmerBytesWritable deletedKmer = incomingMsg.getSourceVertexId();
            if(value.getEdgeList(deleteToMe).contains(deletedKmer)){
                EdgeWritable deletedEdge = value.getEdgeList(deleteToMe).getEdge(deletedKmer);
                value.getEdgeList(deleteToMe).remove(deletedKmer);
                
                deletedEdge.setKey(incomingMsg.getInternalKmer());
                value.getEdgeList(aliveToMe).unionAdd(deletedEdge);
            }
            voteToHalt();
        }
    }
    
    /**
     * for P0
     */
    public void sendMergeMsg() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        if ((state & P4State.MERGE) != 0) {
            outgoingMsg.reset();
            // tell neighbor where this is coming from (so they can merge kmers and delete)
            EDGETYPE mergeEdgetype = EDGETYPE.fromByte(vertex.getState());
            byte neighborRestrictions = DIR.fromSet(mergeEdgetype.causesFlip() ? DIR.flipSetFromByte(state) : DIR.enumSetFromByte(state));
            
            outgoingMsg.setFlag((short) (mergeEdgetype.mirror().get() | neighborRestrictions));
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(vertex.getNode());
            if (vertex.getDegree(mergeEdgetype.dir()) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeEdgetype + " direction != 1!\n" + vertex);
            VKmerBytesWritable dest = vertex.getEdgeList(mergeEdgetype).get(0).getKey();
//            LOG.info("send merge mesage from " + getVertexId() + " to " + dest + ": " + outgoingMsg + "; my restrictions are: " + DIR.enumSetFromByte(vertex.getState()) + ", their restrictions are: " + DIR.enumSetFromByte(outgoingMsg.getFlag()));
            sendMsg(dest, outgoingMsg);
            
//            LOG.info("killing self: " + getVertexId());
            state |= P4State.NO_MERGE;
            vertex.setState(state);
            voteToHalt();
        }
    }
    
    @Override
    public void compute(Iterator<PathMergeMessageWritable> msgIterator) throws Exception {
        initVertex();
        
        if (getSuperstep() == 1) {
            restrictNeighbors();
        } else if (getSuperstep() % 2 == 0) {
            if (getSuperstep() == 2)
                recieveRestrictions(msgIterator);
            else
                receiveMerges(msgIterator);
            chooseMergeDir();
            updateNeighbors();
        } else if (getSuperstep() % 2 == 1) {
            catagorizeMsg(msgIterator);
            
            receiveUpdates(updateMsgs.iterator());
//            receiveToOther(otherMsgs.iterator());
            receiveToNeighbor(neighborMsgs.iterator());
            
            sendMergeMsg();
        } 
    }

}
