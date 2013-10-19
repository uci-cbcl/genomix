package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.ScaffoldingVertex;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

public class VertexValueWritable extends Node {

    private static final long serialVersionUID = 1L;

    public static class VertexStateFlag {

        public static final byte IS_NON = 0b1 << 6;
        public static final byte IS_ERROR = 0b1 << 6;

        public static final byte VERTEX_MASK = 0b1 << 6;
    }

    public static class State extends VertexStateFlag {
        // 2 bits(0-1) for EDGETYPE, then 2 bits(2-3) for set of DIR's
        // each merge has an edge-type direction (e.g., FF)
        // 1 bit(4) to tell the decision to merge
        public static final byte NO_MERGE = 0b0 << 4;
        public static final byte MERGE = 0b1 << 4;
        public static final byte MERGE_CLEAR = 0b1101100; // clear the MERGE/NO_MERGE and the MERGE_DIRECTION
        public static final byte MERGE_MASK = 0b0010011;
    }

    private short state;
    private boolean isFakeVertex;
    private HashMapWritable<ByteWritable, VLongWritable> counters;

    public VertexValueWritable() {
        super();
        state = 0;
        isFakeVertex = false;
        counters = new HashMapWritable<ByteWritable, VLongWritable>();
    }

    public void setAsCopy(VertexValueWritable other) {
        setNode(other.getNode());
        state = other.getState();
        isFakeVertex = other.isFakeVertex();
        counters.clear();
        counters.putAll(other.getCounters());
    }

    public boolean isValidScaffoldingSearchNode() {
        return (this.getStartReads().size() > 0 || this.getEndReads().size() > 0)
                && (getAverageCoverage() >= ScaffoldingVertex.SCAFFOLDING_VERTEX_MIN_COVERAGE && getInternalKmer()
                        .getLength() >= ScaffoldingVertex.SCAFFOLDING_VERTEX_MIN_LENGTH);
    }

    public void setNode(Node node) {
        // TODO invertigate... does this need to be a copy?
        super.setAsCopy(node.getEdges(), node.getStartReads(), node.getEndReads(), node.getInternalKmer(),
                node.getAverageCoverage());
    }

    public EdgeMap getFFList() {
        return getEdgeMap(EDGETYPE.FF);
    }

    public EdgeMap getFRList() {
        return getEdgeMap(EDGETYPE.FR);
    }

    public EdgeMap getRFList() {
        return getEdgeMap(EDGETYPE.RF);
    }

    public EdgeMap getRRList() {
        return getEdgeMap(EDGETYPE.RR);
    }

    public void setFFList(EdgeMap forwardForwardList) {
        setEdgeMap(EDGETYPE.FF, forwardForwardList);
    }

    public void setFRList(EdgeMap forwardReverseList) {
        setEdgeMap(EDGETYPE.FR, forwardReverseList);
    }

    public void setRFList(EdgeMap reverseForwardList) {
        setEdgeMap(EDGETYPE.RF, reverseForwardList);
    }

    public void setRRList(EdgeMap reverseReverseList) {
        setEdgeMap(EDGETYPE.RR, reverseReverseList);
    }

    public short getState() {
        return state;
    }

    public boolean isFakeVertex() {
        return isFakeVertex;
    }

    public void setFakeVertex(boolean isFakeVertex) {
        this.isFakeVertex = isFakeVertex;
    }

    // reuse isFakeVertex to store isSaved()
    public boolean isSaved() {
        return isFakeVertex;
    }

    public void setSaved(boolean isSaved) {
        this.isFakeVertex = isSaved;
    }

    public void setState(short state) {
        this.state = state;
    }

    public HashMapWritable<ByteWritable, VLongWritable> getCounters() {
        return counters;
    }

    public void setCounters(HashMapWritable<ByteWritable, VLongWritable> counters) {
        this.counters.clear();
        this.counters.putAll(counters);
    }

    public void reset() {
        super.reset();
        this.state = 0;
        this.isFakeVertex = false;
        this.counters.clear();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        this.state = in.readShort();
        this.isFakeVertex = in.readBoolean();
        //        this.counters.readFields(in);
        //        scaffoldingMap.readFields(in);

        if (DEBUG) {
            boolean verbose = false;
            for (VKmer problemKmer : problemKmers) {
                verbose |= this.getInternalKmer().equals(problemKmer);
                verbose |= findEdge(problemKmer) != null;
            }
            if (verbose) {
                LOG.fine("VertexValue.readFields: " + toString());
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeShort(this.state);
        out.writeBoolean(this.isFakeVertex);
        //        this.counters.write(out);
        //        scaffoldingMap.write(out);

        if (DEBUG) {
            boolean verbose = false;
            for (VKmer problemKmer : problemKmers) {
                verbose |= this.getInternalKmer().equals(problemKmer);
                verbose |= findEdge(problemKmer) != null;
            }
            if (verbose) {
                LOG.fine("VertexValue.write: " + toString());
            }
        }
    }

    public int getDegree() {
        return inDegree() + outDegree();
    }

    /**
     * check if prev/next destination exists
     */
    public boolean hasPrevDest() {
        return !getRFList().isEmpty() || !getRRList().isEmpty();
    }

    public boolean hasNextDest() {
        return !getFFList().isEmpty() || !getFRList().isEmpty();
    }

    /**
     * Delete the corresponding edge
     */
    public void processDelete(EDGETYPE neighborToDeleteEdgetype, VKmer keyToDelete) {
        ReadIdSet prevList = this.getEdgeMap(neighborToDeleteEdgetype).remove(keyToDelete);
        if (prevList == null) {
            throw new IllegalArgumentException("processDelete tried to remove an edge that didn't exist: "
                    + keyToDelete + " but I am " + this);
        }
    }

    public void processFinalUpdates(EDGETYPE deleteDir, EDGETYPE updateDir, Node other) {
        EDGETYPE replaceDir = deleteDir.mirror();
        this.getNode().updateEdges(deleteDir, null, updateDir, replaceDir, other, false);
    }

    /**
     * Process any changes to value. This is for merging. nodeToAdd should be only edge
     */
    public void processMerges(EDGETYPE mergeDir, Node node, int kmerSize) {
        super.getNode().mergeWithNode(mergeDir, node);
    }

    @Override
    public String toString() {
        return super.toString() + " state: " + state + " which in P4 means will merge: "
                + ((getState() & State.MERGE) != 0) + ", mergeDir: " + EDGETYPE.fromByte(getState())
                + ", restrictions: " + DIR.enumSetFromByte(getState());
    }
}
