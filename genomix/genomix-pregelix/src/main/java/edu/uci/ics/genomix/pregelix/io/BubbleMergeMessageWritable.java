package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class BubbleMergeMessageWritable extends MessageWritable{

    public static class DirToMajor{
        public static final byte FORWARD = 0;
        public static final byte REVERSE = 1;
    }
    
    private VKmerBytesWritable majorVertexId; //use for MergeBubble
    private NodeWritable node; //except kmer, other field should be updated when MergeBubble
    private byte meToMajorDir;
    
    public BubbleMergeMessageWritable(){
        super();
        majorVertexId = new VKmerBytesWritable();
        node = new NodeWritable();
        meToMajorDir = 0;
    }
    
    public void set(BubbleMergeMessageWritable msg){
        this.setSourceVertexId(msg.getSourceVertexId());
        this.setFlag(msg.getFlag());
        this.setMajorVertexId(msg.getMajorVertexId());
        this.setNode(msg.node);
        this.setMeToMajorDir(meToMajorDir);
    }
    
    public void reset(){
        super.reset();
        majorVertexId.reset(0);
        node.reset();
        meToMajorDir = 0;
    }
    
    public VKmerBytesWritable getMajorVertexId() {
        return majorVertexId;
    }

    public void setMajorVertexId(VKmerBytesWritable majorVertexId) {
        this.majorVertexId.setAsCopy(majorVertexId);
    }
    
    public NodeWritable getNode() {
        return node;
    }

    public void setNode(NodeWritable node) {
        this.node = node;
    }
    
    public byte getMeToMajorDir() {
        return meToMajorDir;
    }

    public void setMeToMajorDir(byte meToMajorDir) {
        this.meToMajorDir = meToMajorDir;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        majorVertexId.readFields(in);
        node.readFields(in);
        meToMajorDir = in.readByte();
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        majorVertexId.write(out);
        node.write(out);
        out.writeByte(meToMajorDir);
    }
    
    public static class SortByCoverage implements Comparator<BubbleMergeMessageWritable> {
        @Override
        public int compare(BubbleMergeMessageWritable left, BubbleMergeMessageWritable right) {
            return Float.compare(left.node.getAverageCoverage(), right.node.getAverageCoverage());
        }
    }
    
    public float computeDissimilar(BubbleMergeMessageWritable other){
        if(this.getMeToMajorDir() == other.getMeToMajorDir())
            return this.getSourceVertexId().fracDissimilar(other.getSourceVertexId());
        else{
            String reverse = other.getSourceVertexId().toString();
            VKmerBytesWritable reverseKmer = new VKmerBytesWritable();
            reverseKmer.setByReadReverse(reverse.length(), reverse.getBytes(), 0);
            return this.getSourceVertexId().fracDissimilar(reverseKmer);
        }
        
    }
}
