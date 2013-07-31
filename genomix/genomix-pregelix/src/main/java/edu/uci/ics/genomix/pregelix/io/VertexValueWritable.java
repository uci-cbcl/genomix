package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;

public class VertexValueWritable implements WritableComparable<VertexValueWritable> {

    public static class State extends VertexStateFlag{
        public static final byte HEAD_SHOULD_MERGEWITHPREV = 0b101 << 0;
        public static final byte HEAD_SHOULD_MERGEWITHNEXT = 0b111 << 0;
            
        public static final byte NO_MERGE = 0b00 << 3;
        public static final byte SHOULD_MERGEWITHNEXT = 0b01 << 3;
        public static final byte SHOULD_MERGEWITHPREV = 0b10 << 3;
        public static final byte SHOULD_MERGE_MASK = 0b11 << 3;
        public static final byte SHOULD_MERGE_CLEAR = 0b1100111;
        
        public static final byte KILL = 0b11 << 3;
        public static final byte KILL_MASK = 0b11 << 3;
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
    
    private PositionListWritable nodeIdList;
    private AdjacencyListWritable incomingList;
    private AdjacencyListWritable outgoingList;
    private byte state;
    private VKmerBytesWritable kmer;
    private int kmerlength = 0;
    private boolean isFakeVertex = false;

    public VertexValueWritable() {
        this(0);
    }
    
    public VertexValueWritable(int kmerSize){
        kmerlength = kmerSize;
        nodeIdList = new PositionListWritable();
        incomingList = new AdjacencyListWritable();
        outgoingList = new AdjacencyListWritable();
        state = State.IS_NON;
        kmer = new VKmerBytesWritable();
    }

    public VertexValueWritable(PositionListWritable nodeIdList, VKmerListWritable forwardForwardList, VKmerListWritable forwardReverseList,
            VKmerListWritable reverseForwardList, VKmerListWritable reverseReverseList,
            byte state, VKmerBytesWritable kmer) {
        set(nodeIdList, forwardForwardList, forwardReverseList, 
                reverseForwardList, reverseReverseList,
                state, kmer);
    }
    
    public void set(PositionListWritable nodeIdList, VKmerListWritable forwardForwardList, VKmerListWritable forwardReverseList,
            VKmerListWritable reverseForwardList, VKmerListWritable reverseReverseList, 
            byte state, VKmerBytesWritable kmer) {
        this.kmerlength = kmer.getKmerLetterLength();
        this.incomingList.setForwardList(reverseForwardList);
        this.incomingList.setReverseList(reverseReverseList);
        this.outgoingList.setForwardList(forwardForwardList);
        this.outgoingList.setReverseList(forwardReverseList);
        this.state = state;
        this.kmer.setAsCopy(kmer);
    }
    
    public void set(VertexValueWritable value) {
        this.kmerlength = value.kmerlength;
        set(value.getNodeIdList(), value.getFFList(),value.getFRList(),value.getRFList(),value.getRRList(),value.getState(),
                value.getKmer());
    }
    
    
    public PositionListWritable getNodeIdList() {
        return nodeIdList;
    }

    public void setNodeIdList(PositionListWritable nodeIdList) {
        this.nodeIdList.set(nodeIdList);
    }

    public VKmerListWritable getFFList() {
        return outgoingList.getForwardList();
    }

    public VKmerListWritable getFRList() {
        return outgoingList.getReverseList();
    }

    public VKmerListWritable getRFList() {
        return incomingList.getForwardList();
    }

    public VKmerListWritable getRRList() {
        return incomingList.getReverseList();
    }
    
    public void setFFList(VKmerListWritable forwardForwardList){
        outgoingList.setForwardList(forwardForwardList);
    }
    
    public void setFRList(VKmerListWritable forwardReverseList){
        outgoingList.setReverseList(forwardReverseList);
    }
    
    public void setRFList(VKmerListWritable reverseForwardList){
        incomingList.setForwardList(reverseForwardList);
    }

    public void setRRList(VKmerListWritable reverseReverseList){
        incomingList.setReverseList(reverseReverseList);
    }
    
    public AdjacencyListWritable getIncomingList() {
        return incomingList;
    }

    public void setIncomingList(AdjacencyListWritable incomingList) {
        this.incomingList.set(incomingList);
    }

    public AdjacencyListWritable getOutgoingList() {
        return outgoingList;
    }

    public void setOutgoingList(AdjacencyListWritable outgoingList) {
        this.outgoingList.set(outgoingList);
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

    public int getLengthOfKmer() {
        return kmer.getKmerLetterLength();
    }

    public VKmerBytesWritable getKmer() {
        return kmer;
    }

    public void setKmer(VKmerBytesWritable kmer) {
        this.kmer.setAsCopy(kmer);
    }

    public int getKmerlength() {
        return kmerlength;
    }

    public void setKmerlength(int kmerlength) {
        this.kmerlength = kmerlength;
    }

    public void reset(int kmerSize) {
        this.kmerlength = kmerSize;
        this.nodeIdList.reset();
        this.incomingList.getForwardList().reset();
        this.incomingList.getReverseList().reset();
        this.outgoingList.getForwardList().reset();
        this.outgoingList.getReverseList().reset();
//        this.kmer.reset(0);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.kmerlength = in.readInt();
        this.reset(kmerlength);
        this.nodeIdList.readFields(in);
        this.incomingList.readFields(in);
        this.outgoingList.readFields(in);
        this.state = in.readByte();
        this.kmer.readFields(in);
        this.isFakeVertex = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerlength);
        this.nodeIdList.write(out);
        this.incomingList.write(out);
        this.outgoingList.write(out);
        out.writeByte(this.state);
        this.kmer.write(out);
        out.writeBoolean(this.isFakeVertex);
    }

    @Override
    public int compareTo(VertexValueWritable o) {
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append(nodeIdList.toString()).append('\t');
        sbuilder.append(outgoingList.getForwardList().toString()).append('\t');
        sbuilder.append(outgoingList.getReverseList().toString()).append('\t');
        sbuilder.append(incomingList.getForwardList().toString()).append('\t');
        sbuilder.append(incomingList.getReverseList().toString()).append('\t');
        sbuilder.append(kmer.toString()).append('}');
        return sbuilder.toString();
    }
    
    public int inDegree(){
        return incomingList.getForwardList().getCountOfPosition() + incomingList.getReverseList().getCountOfPosition();
    }
    
    public int outDegree(){
        return outgoingList.getForwardList().getCountOfPosition() + outgoingList.getReverseList().getCountOfPosition();
    }
    
    public int getDegree(){
        return inDegree() + outDegree();
    }
    
    /*
     * Delete the corresponding edge
     */
    public void processDelete(byte neighborToDeleteDir, KmerBytesWritable nodeToDelete){
        switch (neighborToDeleteDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().remove(nodeToDelete);
                break;
        }
    }
    
    /*
     * Process any changes to value.  This is for edge updates
     */
    public void processUpdates(byte neighborToDeleteDir, KmerBytesWritable nodeToDelete,
            byte neighborToMergeDir, KmerBytesWritable nodeToAdd){
//        TODO
//        this.getListFromDir(neighborToDeleteDir).remove(nodeToDelete);
//        this.getListFromDir(neighborToMergeDir).append(nodeToDelete);
        
        switch (neighborToDeleteDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().remove(nodeToDelete);
                break;
        }
        switch (neighborToMergeDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().append(nodeToAdd);
                break;
        }
    }
    
    /*
     * Process any changes to value.  This is for merging
     */
    public void processMerges(byte neighborToDeleteDir, KmerBytesWritable nodeToDelete,
            byte neighborToMergeDir, KmerBytesWritable nodeToAdd, 
            int kmerSize, VKmerBytesWritable kmer){
        switch (neighborToDeleteDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().remove(nodeToDelete); //set(null);
                this.getKmer().mergeWithFFKmer(kmerSize, kmer);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().remove(nodeToDelete);
                this.getKmer().mergeWithFRKmer(kmerSize, kmer);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().remove(nodeToDelete);
                this.getKmer().mergeWithRFKmer(kmerSize, kmer);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().remove(nodeToDelete);
                this.getKmer().mergeWithRRKmer(kmerSize, kmer);
                break;
        }
        // TODO: remove switch below and replace with general direction merge
//        this.getKmer().mergeWithDirKmer(neighborToMergeDir);
        
        switch (neighborToMergeDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().append(nodeToAdd);
                break;
        }
    }
    

}
