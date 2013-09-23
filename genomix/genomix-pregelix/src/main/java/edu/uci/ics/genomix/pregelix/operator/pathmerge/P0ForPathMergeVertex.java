package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;

public class P0ForPathMergeVertex extends
    BasicPathMergeVertex<VertexValueWritable, PathMergeMessageWritable> {
    
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
     * step4: receive and process Merges 
     */
    public void receiveMerges(Iterator<PathMergeMessageWritable> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = vertex.getNode();
        short state = vertex.getState();
        boolean updated = false;
        EDGETYPE senderEdgetype;
        @SuppressWarnings("unused")
        int numMerged = 0;
        //aggregate incomingMsg
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
            }
        } else if(receivedMsgList.size() == 1){ // even number of nodes
            PathMergeMessageWritable msg = receivedMsgList.get(0);
            senderEdgetype = EDGETYPE.fromByte(msg.getFlag());
            state |= (byte) (msg.getFlag() & DIR.MASK);  // update incoming restricted directions
            //determine if merge. if head msg meets head and #receiveMsg = 1
            if (DIR.enumSetFromByte(state).containsAll(EnumSet.allOf(DIR.class))){
                VKmerBytesWritable me = getVertexId();
                VKmerBytesWritable other = msg.getSourceVertexId();
                if(me.compareTo(other) < 0){
                    node.mergeWithNode(senderEdgetype, msg.getNode());
                    numMerged++;
                    updated = true;
                } else{
                    node.mergeWithNode(senderEdgetype, msg.getNode());
                    // 1. send message to other to add edges
                    outgoingMsg.reset();
                    if(senderEdgetype.causesFlip()){
                        for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class))
                            outgoingMsg.getNode().setEdgeList(et.flip(), node.getEdgeList(et));
                    } else
                        outgoingMsg.setNode(node);
                    sendMsg(other, outgoingMsg);
                    
                    // 2. send message to neighbor to update edge from toMe to toOther
                    outgoingMsg.reset();
                    outgoingMsg.setSourceVertexId(me);
                    outgoingMsg.getNode().setInternalKmer(other);
                    for(EDGETYPE et : EnumSet.allOf(EDGETYPE.class)){
                        outFlag = 0;
                        outFlag |= et.get();
                        for (VKmerBytesWritable dest : vertex.getEdgeList(et).getKeys()) {
                            outgoingMsg.setFlag(outFlag);
                            sendMsg(dest, outgoingMsg);
                        }
                    }
                    
                    deleteVertex(getVertexId());
                }
            } else{
                node.mergeWithNode(senderEdgetype, msg.getNode());
                numMerged++;
                updated = true;
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
            receiveUpdates(msgIterator);
            sendMergeMsg();
        } 
    }

}
