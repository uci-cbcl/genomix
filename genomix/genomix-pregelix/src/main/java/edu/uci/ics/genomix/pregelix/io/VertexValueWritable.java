package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import edu.uci.ics.genomix.pregelix.io.common.AdjacencyListWritable;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;

public class VertexValueWritable 
    extends NodeWritable{

    private static final long serialVersionUID = 1L;
    
    public static class HeadMergeDir{
        public static final byte PATH_NON_HEAD = 0b00 << 2;
        public static final byte HEAD_CANNOT_MERGE = 0b01 << 2;
        public static final byte HEAD_CAN_MERGEWITHPREV = 0b10 << 2; //use for initiating head
        public static final byte HEAD_CAN_MERGEWITHNEXT = 0b11 << 2;
        public static final byte HEAD_CAN_MERGE_MASK = 0b11 << 2;
        public static final byte HEAD_CAN_MERGE_CLEAR = (byte)11110011;
    }
    
    public static class VertexStateFlag extends HeadMergeDir{
        public static final byte TO_UPDATE = 0b01 << 5;
        public static final byte TO_OTHER = 0b10 << 5;
        public static final byte TO_NEIGHBOR = 0b11 << 5;
        
        public static final byte MSG_MASK = 0b11 << 5; 
        public static final byte MSG_CLEAR = (byte)0011111;
        
        //TODO clean up code
        public static final byte IS_NON = 0b000 << 4;
        public static final byte IS_HEAD = 0b001 << 4;
        public static final byte IS_FINAL = 0b010 << 4;
        
        public static final byte IS_OLDHEAD = 0b011 << 4;
        
        public static final byte IS_HALT = 0b100 << 4;
        public static final byte IS_DEAD = 0b101 << 4;
        public static final byte IS_ERROR = 0b110 << 4;
        
        public static final byte VERTEX_MASK = 0b111 << 4; 
        public static final byte VERTEX_CLEAR = (byte)0001111;
        
        public static String getContent(short stateFlag){
            switch(stateFlag & VERTEX_MASK){
                case IS_NON:
                    return "IS_NON";
                case IS_HEAD:
                    return "IS_HEAD";
                case IS_FINAL:
                    return "IS_FINAL";
                case IS_OLDHEAD:
                    return "IS_OLDHEAD";
                case IS_HALT:
                    return "IS_HALT";
                case IS_DEAD:
                    return "IS_DEAD";
                case IS_ERROR:
                    return "IS_ERROR";
                    
            }
            return null;
        }
    }
    
    public static class State extends VertexStateFlag{   
        public static final byte NO_MERGE = 0b00 << 0;
        public static final byte CAN_MERGEWITHNEXT = 0b01 << 0;
        public static final byte CAN_MERGEWITHPREV = 0b10 << 0;
        public static final byte CAN_MERGE_MASK = 0b11 << 0;
        public static final byte CAN_MERGE_CLEAR = 0b1111100;
        
        public static final short IS_NONFAKE = 0 << 7;
        public static final short IS_FAKE = 1 << 7;
        
        public static final short FAKEFLAG_MASK = 1 << 7;
    }
    
    public static class P4State {
        // 2 bits for DirectionFlag, then 2 bits for set of DIR's
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
    
    public AdjacencyListWritable getIncomingList() {
        AdjacencyListWritable incomingList = new AdjacencyListWritable();
        incomingList.setForwardList(getRFList());
        incomingList.setReverseList(getRRList());
        return incomingList;
    }

    public void setIncomingList(AdjacencyListWritable incomingList) {
        this.setRFList(incomingList.getForwardList());
        this.setRRList(incomingList.getReverseList());
    }

    public AdjacencyListWritable getOutgoingList() {
        AdjacencyListWritable outgoingList = new AdjacencyListWritable();
        outgoingList.setForwardList(getFFList());
        outgoingList.setReverseList(getFRList());
        return outgoingList;
    }

    public void setOutgoingList(AdjacencyListWritable outgoingList) {
        this.setFFList(outgoingList.getForwardList());
        this.setFRList(outgoingList.getReverseList());
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
        KmerBytesWritable.setGlobalKmerLength(kmerSize); // TODO Do this once at the init of your function, then you don't need it as a parameter here
        super.getNode().mergeWithNode(mergeDir, node);
    }
    
    @Override
    public String toString() {
        return super.toString() + "\t" + State.getContent(state);
    }
}
