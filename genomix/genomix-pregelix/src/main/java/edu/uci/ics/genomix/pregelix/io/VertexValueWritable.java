package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable;

public class VertexValueWritable 
    extends NodeWritable{

    private static final long serialVersionUID = 1L;
    
    public static class VertexStateFlag {
        
        public static final byte IS_NON = 0b1 << 6;
        public static final byte IS_ERROR = 0b1 << 6;
        
        public static final byte VERTEX_MASK = 0b1 << 6; 
        public static final byte VERTEX_CLEAR = (byte)0111111;
        
        public static String getContent(short stateFlag){
            switch(stateFlag & VERTEX_MASK){
                case IS_ERROR:
                    return "IS_ERROR";
            }
            return null;
        }
    }
    
    public static class State extends VertexStateFlag{
        // 2 bits(0-1) for EDGETYPE, then 2 bits(2-3) for set of DIR's
        // each merge has an edge-type direction (e.g., FF)
        // 1 bit(4) to tell the decision to merge
        public static final byte NO_MERGE = 0b0 << 4;
        public static final byte MERGE = 0b1 << 4;
        public static final byte MERGE_CLEAR = 0b1101100; // clear the MERGE/NO_MERGE and the MERGE_DIRECTION
        public static final byte MERGE_MASK = 0b0010011;
        
        public static final short IS_NONFAKE = 0 << 7;
        public static final short IS_FAKE = 1 << 7;
        
        public static final short FAKEFLAG_MASK = 1 << 7;
    }
    
    public static class P4State {
        // 2 bits(0-1) for EDGETYPE, then 2 bits(2-3) for set of DIR's
        // each merge has an edge-type direction (e.g., FF)
        public static final byte NO_MERGE = 0b0 << 4;
        public static final byte MERGE = 0b1 << 4;
        public static final byte MERGE_CLEAR = 0b1101100; // clear the MERGE/NO_MERGE and the MERGE_DIRECTION
        public static final byte MERGE_MASK = 0b0010011;
    }
    
    private short state;
    private boolean isFakeVertex;
    private HashMapWritable<ByteWritable, VLongWritable> counters;
    private HashMapWritable<VLongWritable, KmerListAndFlagListWritable> scaffoldingMap; //use for scaffolding, think optimaztion way
    
    public VertexValueWritable() {
        super();
        state = 0;
        isFakeVertex = false;
        counters = new HashMapWritable<ByteWritable, VLongWritable>();
        scaffoldingMap = new HashMapWritable<VLongWritable, KmerListAndFlagListWritable>();
    }

    public void setAsCopy(VertexValueWritable other){
        setNode(other.getNode());
        state = other.getState();
        isFakeVertex = other.isFakeVertex();
        counters.clear();
        counters.putAll(other.getCounters());
        scaffoldingMap.clear();
        scaffoldingMap.putAll(other.getScaffoldingMap());
    }
    
    public void setNode(NodeWritable node){
    	// TODO invertigate... does this need to be a copy?
        super.setAsCopy(node.getEdges(), node.getStartReads(), node.getEndReads(),
                node.getInternalKmer(), node.getAverageCoverage());
    }
    
    public EdgeListWritable getFFList() {
        return getEdgeList(EDGETYPE.FF);
    }

    public EdgeListWritable getFRList() {
        return getEdgeList(EDGETYPE.FR);
    }

    public EdgeListWritable getRFList() {
        return getEdgeList(EDGETYPE.RF);
    }

    public EdgeListWritable getRRList() {
        return getEdgeList(EDGETYPE.RR);
    }
    
    public void setFFList(EdgeListWritable forwardForwardList){
        setEdgeList(EDGETYPE.FF, forwardForwardList);
    }
    
    public void setFRList(EdgeListWritable forwardReverseList){
        setEdgeList(EDGETYPE.FR, forwardReverseList);
    }
    
    public void setRFList(EdgeListWritable reverseForwardList){
        setEdgeList(EDGETYPE.RF, reverseForwardList);
    }

    public void setRRList(EdgeListWritable reverseReverseList){
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
    
    public HashMapWritable<VLongWritable, KmerListAndFlagListWritable> getScaffoldingMap() {
        return scaffoldingMap;
    }

    public void setScaffoldingMap(HashMapWritable<VLongWritable, KmerListAndFlagListWritable> scaffoldingMap) {
        this.scaffoldingMap.clear();
        this.scaffoldingMap.putAll(scaffoldingMap);
    }
    
    public void reset() {
        super.reset();
        this.state = 0;
        this.isFakeVertex = false;
        this.counters.clear();
        scaffoldingMap.clear();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        this.state = in.readShort();
        this.isFakeVertex = in.readBoolean();
        this.counters.readFields(in);
        scaffoldingMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeShort(this.state);
        out.writeBoolean(this.isFakeVertex);
        this.counters.write(out);
        scaffoldingMap.write(out);
    }
    
    public int getDegree(){
        return inDegree() + outDegree();
    }
    
    /**
     * check if prev/next destination exists
     */
    public boolean hasPrevDest(){
        return !getRFList().isEmpty() || !getRRList().isEmpty();
    }
    
    public boolean hasNextDest(){
        return !getFFList().isEmpty() || !getFRList().isEmpty();
    }
    
    /**
     * Delete the corresponding edge
     */
    public void processDelete(EDGETYPE neighborToDeleteEdgetype, EdgeWritable nodeToDelete){
        this.getEdgeList(neighborToDeleteEdgetype).remove(nodeToDelete);
    }
    
    public void processFinalUpdates(EDGETYPE deleteDir, EDGETYPE updateDir, NodeWritable other){
        EDGETYPE replaceDir = deleteDir.mirror();
        this.getNode().updateEdges(deleteDir, null, updateDir, replaceDir, other, false);
    }
    
    /**
     * Process any changes to value.  This is for merging.  nodeToAdd should be only edge
     */
    public void processMerges(EDGETYPE mergeDir, NodeWritable node, int kmerSize){
        super.getNode().mergeWithNode(mergeDir, node);
    }
    
    @Override
    public String toString() {
        return super.toString() + "\t" + State.getContent(state);
    }
}
