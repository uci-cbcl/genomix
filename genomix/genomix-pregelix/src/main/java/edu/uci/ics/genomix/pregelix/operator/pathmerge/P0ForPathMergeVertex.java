package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.EnumSet;
import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.P4State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
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
