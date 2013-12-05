package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.ScaffoldingVertex;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class VertexValueWritable extends Node {

    private static final long serialVersionUID = 1L;

    public static class VertexStateFlag {

        // general case, marking it as NORMAL_NODE
        public static final byte NORMAL_NODE = 0b1 << 6;
        // ERROR_NODE is used in SymmetryChecker, if the vertex exists error, marking it as ERROR_NODE
        public static final byte ERROR_NODE = 0b1 << 6;
        // KEEP_NODE is used in ExtractSubgraph, if the vertex is extracted, marking it as KEEP_NODE
        public static final byte KEEP_NODE = 0b1 << 6;
        // DEAD_NODE is used in RemoveLowcoverage, if the vertex is deleted, marking it as DEAD_NODE
        public static final byte DEAD_NODE = 0b1 << 6;

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
    
    protected boolean verbose = false;

    public VertexValueWritable() {
        super();
        state = 0;
        isFakeVertex = false;
    }

    public void setAsCopy(VertexValueWritable other) {
        setNode(other);
        state = other.getState();
        isFakeVertex = other.isFakeVertex();
    }

    public boolean isValidScaffoldingSearchNode() {
        return (this.getUnflippedReadIds().size() > 0 || this.getFlippedReadIds().size() > 0)
                && (getAverageCoverage() >= ScaffoldingVertex.SCAFFOLDING_VERTEX_MIN_COVERAGE && getInternalKmer()
                        .getLength() >= ScaffoldingVertex.SCAFFOLDING_VERTEX_MIN_LENGTH);
    }

    public void setNode(Node node) {
        // TODO invertigate... does this need to be a copy?
        super.setAsCopy(node.getAllEdges(), node.getUnflippedReadIds(), node.getFlippedReadIds(), node.getInternalKmer(),
                node.getAverageCoverage());
    }

    public VKmerList getFFList() {
        return getEdges(EDGETYPE.FF);
    }

    public VKmerList getFRList() {
        return getEdges(EDGETYPE.FR);
    }

    public VKmerList getRFList() {
        return getEdges(EDGETYPE.RF);
    }

    public VKmerList getRRList() {
        return getEdges(EDGETYPE.RR);
    }

    public void setFFList(VKmerList forwardForwardList) {
        setEdges(EDGETYPE.FF, forwardForwardList);
    }

    public void setFRList(VKmerList forwardReverseList) {
        setEdges(EDGETYPE.FR, forwardReverseList);
    }

    public void setRFList(VKmerList reverseForwardList) {
        setEdges(EDGETYPE.RF, reverseForwardList);
    }

    public void setRRList(VKmerList reverseReverseList) {
        setEdges(EDGETYPE.RR, reverseReverseList);
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

    public void reset() {
        super.reset();
        this.state = 0;
        this.isFakeVertex = false;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        this.state = in.readShort();
        this.isFakeVertex = in.readBoolean();
        //        this.counters.readFields(in);
        //        scaffoldingMap.readFields(in);

        if (GenomixJobConf.debug) {
            verbose = false;
            for (VKmer problemKmer : GenomixJobConf.debugKmers) {
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

        //        if (DEBUG) {
        //            boolean verbose = false;
        //            for (VKmer problemKmer : problemKmers) {
        //                verbose |= this.getInternalKmer().equals(problemKmer);
        //                verbose |= findEdge(problemKmer) != null;
        //            }
        //            if (verbose) {
        ////                LOG.fine("VertexValue.write: " + toString());
        //            }
        //        }
    }

    public int getDegree() {
        return inDegree() + outDegree();
    }

    /**
     * Process any changes to value. This is for merging. nodeToAdd should be only edge
     */
    public void processMerges(EDGETYPE mergeDir, Node node, int kmerSize) {
        mergeWithNode(mergeDir, node);
    }

    @Override
    public String toString() {
        return super.toString() + " state: " + state + " which in P4 means will merge: "
                + ((getState() & State.MERGE) != 0) + ", mergeDir: " + EDGETYPE.fromByte(getState())
                + ", restrictions: " + DIR.enumSetFromByte(getState());
    }
}
