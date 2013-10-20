package edu.uci.ics.genomix.pregelix.TestHelper;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessage;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicMapReduceVertex;

public class MapReduceVertex extends BasicMapReduceVertex<VertexValueWritable, PathMergeMessage> {
    
    @Override
    public void compute(Iterator<PathMergeMessage> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            addFakeVertex("A");
        } else if (getSuperstep() == 2) {
            sendMsgToFakeVertex();
        } else if (getSuperstep() == 3) {
            mapReduceInFakeVertex(msgIterator);
            voteToHalt();
        } else if (getSuperstep() == 4) {
            broadcastKillself();
            deleteVertex(getVertexId());
        } else if (getSuperstep() == 5) {
            pruneDeadEdges(msgIterator);
            voteToHalt();
        }
    }
}
