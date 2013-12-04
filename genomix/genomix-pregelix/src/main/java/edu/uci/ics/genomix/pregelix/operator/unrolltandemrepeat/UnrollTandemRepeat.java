package edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.vertex.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.GraphMutations;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;

/**
 * Graph clean pattern: Unroll TandemRepeat
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
    }

    /**
     * check if this tandem repeat can be solved
     */
    public boolean repeatCanBeMerged() {
        tmpValue.setAsCopy(getVertexValue());
        tmpValue.getEdges(repeatEdgetype).remove(repeatKmer);
        boolean hasFlip = false;
        // pick one edge and flip 
        for (EDGETYPE et : EDGETYPE.values) {
            for (VKmer edge : tmpValue.getEdges(et)) {
                EDGETYPE flipEt = et.flipNeighbor();
                if (!tmpValue.getEdges(flipEt).contains(edge))
                    tmpValue.getEdges(flipEt).append(edge);
                tmpValue.getEdges(et).remove(edge);
                // setup hasFlip to go out of the loop 
                hasFlip = true;
                break;
            }
            if (hasFlip)
                break;
        }

        if ((tmpValue.inDegree() == 0 || tmpValue.inDegree() == 1)
                && (tmpValue.outDegree() == 0 && tmpValue.outDegree() == 1))
            return true;
        else
            return false;
    }

    /**
     * merge tandem repeat
     */
    public void mergeTandemRepeat() {
        getVertexValue().getInternalKmer().mergeWithKmerInDir(repeatEdgetype, kmerSize, getVertexId());
        getVertexValue().getEdges(repeatEdgetype).remove(getVertexId());
        boolean hasFlip = false;
        /** pick one edge and flip **/
        for (EDGETYPE et : EDGETYPE.values) {
            for (VKmer edge : getVertexValue().getEdges(et)) {
                EDGETYPE flipDir = et.flipNeighbor();
                if (!getVertexValue().getEdges(flipDir).contains(edge))
                    getVertexValue().getEdges(flipDir).append(edge);
                getVertexValue().getEdges(et).remove(edge);
                /** send flip message to node for updating edgeDir **/
                outgoingMsg.setFlag(flipDir.get());
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(edge, outgoingMsg);
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
        if (!vertex.getEdges(curNeighborToMe).contains(incomingMsg.getSourceVertexId()))
            vertex.getEdges(curNeighborToMe).append(incomingMsg.getSourceVertexId());
        vertex.getEdges(prevNeighborToMe).remove(incomingMsg.getSourceVertexId());
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
        initVertex();
        if (getSuperstep() == 1) {
            if (isTandemRepeat(getVertexValue())) { // && repeatCanBeMerged()
                mergeTandemRepeat();
                getCounters().findCounter(GraphMutations.Num_TandemRepeats).increment(1);
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

}
