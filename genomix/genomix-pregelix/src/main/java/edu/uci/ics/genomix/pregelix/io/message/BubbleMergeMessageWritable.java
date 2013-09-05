package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class BubbleMergeMessageWritable extends MessageWritable{

    public static class DirToMajor{
        public static final byte FORWARD = 0;
        public static final byte REVERSE = 1;
    }
    
    private VKmerBytesWritable majorVertexId; //use for MergeBubble
    private VKmerBytesWritable minorVertexId;
    private NodeWritable node; //except kmer, other field should be updated when MergeBubble
    private byte meToMajorDir;
    private byte meToMinorDir;
    private VKmerBytesWritable topCoverageVertexId;
    private boolean isFlip;
    
    public BubbleMergeMessageWritable(){
        super();
        majorVertexId = new VKmerBytesWritable();
        minorVertexId = new VKmerBytesWritable();
        node = new NodeWritable();
        meToMajorDir = 0;
        meToMinorDir = 0;
        topCoverageVertexId = new VKmerBytesWritable();
        isFlip = false;
    }
    
    public BubbleMergeMessageWritable(BubbleMergeMessageWritable msg){
        set(msg);
    }
    
    public void set(BubbleMergeMessageWritable msg){
        this.setSourceVertexId(msg.getSourceVertexId());
        this.setFlag(msg.getFlag());
        this.setMajorVertexId(msg.getMajorVertexId());
        this.setMinorVertexId(msg.getMinorVertexId());
        this.setNode(msg.node);
        this.setMeToMajorDir(msg.meToMajorDir);
        this.setMeToMinorDir(msg.meToMinorDir);
        this.setTopCoverageVertexId(msg.topCoverageVertexId);
        this.setFlip(msg.isFlip());
    }
    
    public void reset(){
        super.reset();
        majorVertexId.reset(0);
        minorVertexId.reset(0);
        node.reset();
        meToMajorDir = 0;
        meToMinorDir = 0;
        topCoverageVertexId.reset(0);
        isFlip = false;
    }
    
    public byte getRelativeDirToMajor(){
        switch(meToMajorDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_RR:
                return DirToMajor.FORWARD;
            case MessageFlag.DIR_FR:
            case MessageFlag.DIR_RF:
                return DirToMajor.REVERSE;
        }
        return 0;
    }
    
    public VKmerBytesWritable getMajorVertexId() {
        return majorVertexId;
    }

    public void setMajorVertexId(VKmerBytesWritable majorVertexId) {
        if(this.majorVertexId == null)
            this.majorVertexId = new VKmerBytesWritable();
        this.majorVertexId.setAsCopy(majorVertexId);
    }
    
    
    public VKmerBytesWritable getMinorVertexId() {
        return minorVertexId;
    }

    public void setMinorVertexId(VKmerBytesWritable minorVertexId) {
        if(this.minorVertexId == null)
            this.minorVertexId = new VKmerBytesWritable();
        this.minorVertexId.setAsCopy(minorVertexId);
    }

    public VKmerBytesWritable getTopCoverageVertexId() {
        return topCoverageVertexId;
    }

    public void setTopCoverageVertexId(VKmerBytesWritable topCoverageVertexId) {
        if(this.topCoverageVertexId == null)
            this.topCoverageVertexId = new VKmerBytesWritable();
        this.topCoverageVertexId.setAsCopy(topCoverageVertexId);
    }
    
    public NodeWritable getNode() {
        return node;
    }

    public void setNode(NodeWritable node) {
        if(this.node == null)
            this.node = new NodeWritable();
        this.node.setAsCopy(node);
    }
    
    public byte getMeToMajorDir() {
        return meToMajorDir;
    }

    public void setMeToMajorDir(byte meToMajorDir) {
        this.meToMajorDir = meToMajorDir;
    }

    public byte getMeToMinorDir() {
        return meToMinorDir;
    }

    public void setMeToMinorDir(byte meToMinorDir) {
        this.meToMinorDir = meToMinorDir;
    }
    
    public boolean isFlip() {
        return isFlip;
    }

    public void setFlip(boolean isFlip) {
        this.isFlip = isFlip;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        majorVertexId.readFields(in);
        minorVertexId.readFields(in);
        node.readFields(in);
        meToMajorDir = in.readByte();
        meToMinorDir = in.readByte();
        topCoverageVertexId.readFields(in);
        isFlip = in.readBoolean();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        majorVertexId.write(out);
        minorVertexId.write(out);
        node.write(out);
        out.writeByte(meToMajorDir);
        out.write(meToMinorDir);
        topCoverageVertexId.write(out);
        out.writeBoolean(isFlip);
    }
    
    public static class SortByCoverage implements Comparator<BubbleMergeMessageWritable> {
        @Override
        public int compare(BubbleMergeMessageWritable left, BubbleMergeMessageWritable right) {
            return -Float.compare(left.node.getAverageCoverage(), right.node.getAverageCoverage());
        }
    }
    
    public boolean isFlip(BubbleMergeMessageWritable other){
        return this.getRelativeDirToMajor() != other.getRelativeDirToMajor();
    }
    
    public float computeDissimilar(BubbleMergeMessageWritable other){
        if(isFlip(other)){
            String reverse = other.getNode().getInternalKmer().toString();
            VKmerBytesWritable reverseKmer = new VKmerBytesWritable();
            reverseKmer.setByReadReverse(reverse.length(), reverse.getBytes(), 0);
            return this.getNode().getInternalKmer().fracDissimilar(reverseKmer);
        } else
            return this.getNode().getInternalKmer().fracDissimilar(other.getNode().getInternalKmer());
    }
}
