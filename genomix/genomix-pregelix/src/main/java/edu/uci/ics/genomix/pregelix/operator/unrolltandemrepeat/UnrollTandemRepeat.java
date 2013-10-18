package edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map.Entry;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

/**
 * Graph clean pattern: Unroll TandemRepeat
 * 
 */
public class UnrollTandemRepeat extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {

    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if (repeatKmer == null)
            repeatKmer = new VKmer();
        if (getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    /**
     * check if this tandem repeat can be solved
     */
    public boolean repeatCanBeMerged() {
        tmpValue.setAsCopy(getVertexValue());
        tmpValue.getEdgeMap(repeatEdgetype).remove(repeatKmer);
        boolean hasFlip = false;
        // pick one edge and flip 
        for (EDGETYPE et : EnumSet.allOf(EDGETYPE.class)) {
            for (Entry<VKmer, ReadIdSet> edge : tmpValue.getEdgeMap(et).entrySet()) {
                EDGETYPE flipDir = et.flipNeighbor();
                tmpValue.getEdgeMap(flipDir).put(edge.getKey(), edge.getValue());
                tmpValue.getEdgeMap(et).remove(edge);
                // setup hasFlip to go out of the loop 
                hasFlip = true;
                break;
            }
            if (hasFlip)
                break;
        }

        if (VertexUtil.isPathVertex(tmpValue) || VertexUtil.isTipVertex(tmpValue)
                || VertexUtil.isSingleVertex(tmpValue))
            return true;
        else
            return false;
    }

    /**
     * merge tandem repeat
     */
    public void mergeTandemRepeat() {
        getVertexValue().getInternalKmer().mergeWithKmerInDir(repeatEdgetype, kmerSize, getVertexId());
        getVertexValue().getEdgeMap(repeatEdgetype).remove(getVertexId());
        boolean hasFlip = false;
        /** pick one edge and flip **/
        for (EDGETYPE et : EDGETYPE.values()) {
            for (Entry<VKmer, ReadIdSet> edge : getVertexValue().getEdgeMap(et).entrySet()) {
                EDGETYPE flipDir = et.flipNeighbor();
                getVertexValue().getEdgeMap(flipDir).put(edge.getKey(), edge.getValue());
                getVertexValue().getEdgeMap(et).remove(edge);
                /** send flip message to node for updating edgeDir **/
                outgoingMsg.setFlag(flipDir.get());
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(edge.getKey(), outgoingMsg);
                /** setup hasFlip to go out of the loop **/
                hasFlip = true;
                break;
            }
            if (hasFlip)
                break;
        }
    }

    /**
     * update edges
     */
    public void updateEdges(MessageWritable incomingMsg) {
        VertexValueWritable vertex = getVertexValue();
        EDGETYPE flipDir = EDGETYPE.fromByte(incomingMsg.getFlag());
        EDGETYPE prevNeighborToMe = flipDir.mirror();
        EDGETYPE curNeighborToMe = flipDir.mirror(); //mirrorDirection((byte)(incomingMsg.getFlag() & MessageFlag.DEAD_MASK));
        vertex.getEdgeMap(curNeighborToMe).put(incomingMsg.getSourceVertexId(),
                vertex.getEdgeMap(prevNeighborToMe).get(incomingMsg.getSourceVertexId()));
        vertex.getEdgeMap(prevNeighborToMe).remove(incomingMsg.getSourceVertexId());
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
        initVertex();
        if (getSuperstep() == 1) {
            if (isTandemRepeat(getVertexValue()) && repeatCanBeMerged()) {
                mergeTandemRepeat();
                //set statistics counter: Num_TandemRepeats
                incrementCounter(StatisticsCounter.Num_TandemRepeats);
                getVertexValue().setCounters(counters);
            }
            voteToHalt();
        } else if (getSuperstep() == 2) {
            while (msgIterator.hasNext()) {
                MessageWritable incomingMsg = msgIterator.next();
                /** update edge **/
                updateEdges(incomingMsg);
            }
            voteToHalt();
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, UnrollTandemRepeat.class));
    }
}
