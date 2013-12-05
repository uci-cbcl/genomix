package edu.uci.ics.genomix.pregelix.operator.test;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.PathMergeMessage;

public class MapReduceVertex extends BasicMapReduceVertex<VertexValueWritable, PathMergeMessage> {

    @Override
    public void compute(Iterator<PathMergeMessage> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            addFakeVertex("A");
        } else if (getSuperstep() == 2) {
            sendMsgToFakeVertex();
        } else if (getSuperstep() == 3) {
            groupByInternalKmer(msgIterator);
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
