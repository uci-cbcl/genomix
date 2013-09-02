package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import edu.uci.ics.genomix.pregelix.io.common.AdjacencyListWritable;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

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
        public static final byte IS_HEAD = 0b01 << 5;
        public static final byte IS_FINAL = 0b10 << 5;
        
        public static final byte IS_OLDHEAD = 0b11 << 5;
        
        public static final byte IS_HALT = 0b1111111;
        public static final byte IS_DEAD = 0b0111111;
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
    private HashMapWritable<ByteWritable, VLongWritable> counters;
    private HashMapWritable<VLongWritable, KmerListAndFlagListWritable> scaffoldingMap; //use for scaffolding, think optimaztion way
    
    public VertexValueWritable() {
        super();
        state = 0;
        isFakeVertex = false;
        counters = new HashMapWritable<ByteWritable, VLongWritable>();
        scaffoldingMap = new HashMapWritable<VLongWritable, KmerListAndFlagListWritable>();
    }
    
    public VertexValueWritable get(){
        return this;
    }
    
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
        this.state = in.readByte();
        this.isFakeVertex = in.readBoolean();
        this.counters.readFields(in);
        scaffoldingMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeByte(this.state);
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
    public void processDelete(byte neighborToDeleteDir, EdgeWritable nodeToDelete){
        byte dir = (byte)(neighborToDeleteDir & MessageFlag.DIR_MASK);
        this.getEdgeList(dir).remove(nodeToDelete);
    }
    
    /**
     * Process any changes to value.  This is for edge updates.  nodeToAdd should be only edge
     */
    public void processUpdates(byte deleteDir, VKmerBytesWritable toDelete, byte updateDir, NodeWritable other){
        this.getNode().updateEdges(deleteDir, toDelete, updateDir, other);
    }
    
    /**
     * Process any changes to value.  This is for merging.  nodeToAdd should be only edge
     */
    public void processMerges(byte mergeDir, NodeWritable node, int kmerSize){
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        mergeDir = (byte)(mergeDir & MessageFlag.DIR_MASK);
        super.getNode().mergeWithNode(mergeDir, node);
    }
    
}
