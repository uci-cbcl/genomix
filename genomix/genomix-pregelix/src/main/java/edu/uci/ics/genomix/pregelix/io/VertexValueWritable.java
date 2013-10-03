package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.BFSTraverseVertex.PathAndEdgeTypeList;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.BFSTraverseVertex.SearchInfo;
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
    private HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap; //use for scaffolding, think optimaztion way
    private HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap;
    
    public VertexValueWritable() {
        super();
        state = 0;
        isFakeVertex = false;
        counters = new HashMapWritable<ByteWritable, VLongWritable>();
        scaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();
        pathMap = new HashMapWritable<LongWritable, PathAndEdgeTypeList>();
    }

    public void setAsCopy(VertexValueWritable other) {
        setNode(other.getNode());
        state = other.getState();
        isFakeVertex = other.isFakeVertex();
        counters.clear();
        counters.putAll(other.getCounters());
        scaffoldingMap.clear();
        scaffoldingMap.putAll(other.getScaffoldingMap());
        pathMap.clear();
        pathMap.putAll(other.pathMap);
    }

    public void setNode(Node node) {
        // TODO invertigate... does this need to be a copy?
        super.setAsCopy(node.getEdges(), node.getStartReads(), node.getEndReads(), node.getInternalKmer(),
                node.getAverageCoverage());
    }

    public EdgeMap getFFList() {
        return getEdgeList(EDGETYPE.FF);
    }

    public EdgeMap getFRList() {
        return getEdgeList(EDGETYPE.FR);
    }

    public EdgeMap getRFList() {
        return getEdgeList(EDGETYPE.RF);
    }

    public EdgeMap getRRList() {
        return getEdgeList(EDGETYPE.RR);
    }

    public void setFFList(EdgeMap forwardForwardList) {
        setEdgeList(EDGETYPE.FF, forwardForwardList);
    }

    public void setFRList(EdgeMap forwardReverseList) {
        setEdgeList(EDGETYPE.FR, forwardReverseList);
    }

    public void setRFList(EdgeMap reverseForwardList) {
        setEdgeList(EDGETYPE.RF, reverseForwardList);
    }

    public void setRRList(EdgeMap reverseReverseList) {
        setEdgeList(EDGETYPE.RR, reverseReverseList);
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

    public HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> getScaffoldingMap() {
        return scaffoldingMap;
    }

    public void setScaffoldingMap(HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap) {
        this.scaffoldingMap.clear();
        this.scaffoldingMap.putAll(scaffoldingMap);
    }
    
    public HashMapWritable<LongWritable, PathAndEdgeTypeList> getPathMap() {
        return pathMap;
    }

    public void setPathMap(HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap) {
        this.pathMap.clear();
        this.pathMap.putAll(pathMap);
    }

    public void reset() {
        super.reset();
        this.state = 0;
        this.isFakeVertex = false;
        this.counters.clear();
        scaffoldingMap.clear();
        pathMap.clear();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        this.state = in.readShort();
        this.isFakeVertex = in.readBoolean();
        //        this.counters.readFields(in);
        //        scaffoldingMap.readFields(in);
        pathMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeShort(this.state);
        out.writeBoolean(this.isFakeVertex);
        //        this.counters.write(out);
        //        scaffoldingMap.write(out);
        pathMap.write(out);
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
        ReadIdSet prevList = this.getEdgeList(neighborToDeleteEdgetype).remove(keyToDelete);
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
        return super.toString();
    }
}
