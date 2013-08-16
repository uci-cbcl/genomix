package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;

public class VertexValueWritable 
    extends NodeWritable{

    private static final long serialVersionUID = 1L;

    public static class State extends VertexStateFlag{   
        public static final byte HEAD_SHOULD_MERGEWITHPREV = 0b0 << 2; //use for initiating head
        public static final byte HEAD_SHOULD_MERGEWITHNEXT = 0b1 << 2;
        public static final byte HEAD_SHOULD_MERGE_MASK = 0b1 << 2;
        public static final byte HEAD_SHOULD_MERGE_CLEAR = (byte) 11001011;
        
        public static final byte NO_MERGE = 0b00 << 3;
        public static final byte SHOULD_MERGEWITHNEXT = 0b01 << 3;
        public static final byte SHOULD_MERGEWITHPREV = 0b10 << 3;
        public static final byte SHOULD_MERGE_MASK = 0b11 << 3;
        public static final byte SHOULD_MERGE_CLEAR = 0b1100111;
        
        public static final byte UNCHANGE = 0b0 << 3;
        public static final byte KILL = 0b1 << 3;
        public static final byte KILL_MASK = 0b1 << 3;
        
        public static final byte DIR_FROM_DEADVERTEX = 0b10 << 3;
        public static final byte DEAD_MASK = 0b10 << 3;
    }
    
    public static class VertexStateFlag extends FakeFlag {
        public static final byte IS_NON = 0b00 << 5;
        public static final byte IS_RANDOMTAIL = 0b00 << 5;
        public static final byte IS_HEAD = 0b01 << 5;
        public static final byte IS_FINAL = 0b10 << 5;
        public static final byte IS_RANDOMHEAD = 0b11 << 5;
        public static final byte IS_OLDHEAD = 0b11 << 5;

        public static final byte VERTEX_MASK = 0b11 << 5;
        public static final byte VERTEX_CLEAR = (byte) 11001111;
    }
    
    public static class FakeFlag{
        public static final byte IS_NONFAKE = 0 << 0;
        public static final byte IS_FAKE = 1 << 0;
        
        public static final byte FAKEFLAG_MASK = (byte) 00000001;
    }
    
    private byte state;
    private boolean isFakeVertex;
    
    private HashMapWritable<VKmerBytesWritable, VKmerListWritable> traverseMap;

    public VertexValueWritable() {
        super();
        state = 0;
        isFakeVertex = false;
        traverseMap  = new HashMapWritable<VKmerBytesWritable, VKmerListWritable>();
    }
    
//    public NodeWritable getNode(){
//        NodeWritable node = new NodeWritable();
//        node.setAsCopy(this.getEdges(), this.getStartReads(), this.getEndReads(), 
//                this.getInternalKmer(), this.getAverageCoverage());
//        return super();
//    }
    
    public void setNode(NodeWritable node){
        super.setAsCopy(node.getEdges(), node.getStartReads(), node.getEndReads(),
                node.getInternalKmer(), node.getAverageCoverage());
    }
    
    public EdgeListWritable getFFList() {
        return getEdgeList(DirectionFlag.DIR_FF);
    }

    public EdgeListWritable getFRList() {
        return getEdgeList(DirectionFlag.DIR_FR);
    }

    public EdgeListWritable getRFList() {
        return getEdgeList(DirectionFlag.DIR_RF);
    }

    public EdgeListWritable getRRList() {
        return getEdgeList(DirectionFlag.DIR_RR);
    }
    
    public void setFFList(EdgeListWritable forwardForwardList){
        setEdgeList(DirectionFlag.DIR_FF, forwardForwardList);
    }
    
    public void setFRList(EdgeListWritable forwardReverseList){
        setEdgeList(DirectionFlag.DIR_FR, forwardReverseList);
    }
    
    public void setRFList(EdgeListWritable reverseForwardList){
        setEdgeList(DirectionFlag.DIR_RF, reverseForwardList);
    }

    public void setRRList(EdgeListWritable reverseReverseList){
        setEdgeList(DirectionFlag.DIR_RR, reverseReverseList);
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

    public byte getState() {
        return state;
    }
 
    public boolean isFakeVertex() {
        return isFakeVertex;
    }

    public void setFakeVertex(boolean isFakeVertex) {
        this.isFakeVertex = isFakeVertex;
    }

    public void setState(byte state) {
        this.state = state;
    }
    
    public HashMapWritable<VKmerBytesWritable, VKmerListWritable> getTraverseMap() {
        return traverseMap;
    }

    public void setTraverseMap(HashMapWritable<VKmerBytesWritable, VKmerListWritable> traverseMap) {
        this.traverseMap = traverseMap;
    }

    public void reset() {
        super.reset();
        this.state = 0;
        this.isFakeVertex = false;
        this.traverseMap.clear();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        this.state = in.readByte();
        this.isFakeVertex = in.readBoolean();
        this.traverseMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeByte(this.state);
        out.writeBoolean(this.isFakeVertex);
        this.traverseMap.write(out);
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
    public void processDelete(byte neighborToDeleteDir, EdgeWritable nodeToDelete){
        byte dir = (byte)(neighborToDeleteDir & MessageFlag.DIR_MASK);
        this.getEdgeList(dir).remove(nodeToDelete);
    }
    
    /**
     * Process any changes to value.  This is for edge updates.  nodeToAdd should be only edge
     */
    public void processUpdates(byte neighborToDeleteDir, VKmerBytesWritable nodeToDelete,
            byte neighborToMergeDir, EdgeWritable nodeToAdd){
        byte deleteDir = (byte)(neighborToDeleteDir & MessageFlag.DIR_MASK);
        this.getEdgeList(deleteDir).remove(nodeToDelete);
        
        byte mergeDir = (byte)(neighborToMergeDir & MessageFlag.DIR_MASK);
        this.getEdgeList(mergeDir).add(nodeToAdd);
    }
    
    /**
     * Process any changes to value.  This is for merging.  nodeToAdd should be only edge
     */
    public void processMerges(byte neighborToDeleteDir, VKmerBytesWritable nodeToDelete,
            byte neighborToMergeDir, EdgeWritable nodeToAdd, 
            int kmerSize, NodeWritable node){
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        byte deleteDir = (byte)(neighborToDeleteDir & MessageFlag.DIR_MASK);
//        this.getEdgeList(deleteDir).remove(nodeToDelete);
        super.getNode().mergeWithNode(deleteDir, node);
//        this.getInternalKmer().mergeWithKmerInDir(deleteDir, kmerSize, kmer);

//        if(nodeToAdd != null){
//            byte mergeDir = (byte)(neighborToMergeDir & MessageFlag.DIR_MASK);
//            this.getEdgeList(mergeDir).add(nodeToAdd);
//        }
    }
    
    public boolean hasPathTo(VKmerBytesWritable nodeToSeek){
        return traverseMap.containsKey(nodeToSeek);
    }
    
}
