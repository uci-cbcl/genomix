package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class NodeWritable implements WritableComparable<NodeWritable>, Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private PositionWritable nodeID;
    private PositionListWritable forwardForwardList;
    private PositionListWritable forwardReverseList;
    private PositionListWritable reverseForwardList;
    private PositionListWritable reverseReverseList;
    private KmerBytesWritable kmer;

    public NodeWritable() {
        this(21);
    }

    public NodeWritable(int kmerSize) {
        nodeID = new PositionWritable(0,(byte) 0);
        forwardForwardList = new PositionListWritable();
        forwardReverseList = new PositionListWritable();
        reverseForwardList = new PositionListWritable();
        reverseReverseList = new PositionListWritable();
        kmer = new KmerBytesWritable(kmerSize);
    }

    public void setNodeID(PositionWritable ref) {
        this.setNodeID(ref.getReadID(), ref.getPosInRead());
    }

    public void setNodeID(int readID, byte posInRead) {
        nodeID.set(readID, posInRead);
    }

    public void setKmer(KmerBytesWritable right) {
        this.kmer.set(right);
    }
    
    public void reset(int kmerSize) {
        nodeID.set(0, (byte) 0);
        forwardForwardList.reset();
        forwardReverseList.reset();
        reverseForwardList.reset();
        reverseReverseList.reset();
        kmer.reset(kmerSize);
    }

    public PositionListWritable getFFList() {
        return forwardForwardList;
    }
    
    public PositionListWritable getFRList() {
        return forwardReverseList;
    }

    public PositionListWritable getRFList() {
        return reverseForwardList;
    }
    
    public PositionListWritable getRRList() {
        return reverseReverseList;
    }

    public PositionWritable getNodeID() {
        return nodeID;
    }

    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public int getCount() {
        return kmer.getKmerLength();
    }

    public void mergeForwadNext(NodeWritable nextNode, int initialKmerSize) {
        this.forwardForwardList.set(nextNode.forwardForwardList);
        this.forwardReverseList.set(nextNode.forwardReverseList);
        kmer.mergeNextKmer(initialKmerSize, nextNode.getKmer());
    }
    
    public void mergeForwardPre(NodeWritable preNode, int initialKmerSize){
        this.reverseForwardList.set(preNode.reverseForwardList);
        this.reverseReverseList.set(preNode.reverseReverseList);
        kmer.mergePreKmer(initialKmerSize, preNode.getKmer());
    }

    public void set(NodeWritable node) {
        this.nodeID.set(node.getNodeID().getReadID(), node.getNodeID().getPosInRead());
        this.forwardForwardList.set(node.forwardForwardList);
        this.forwardReverseList.set(node.forwardReverseList);
        this.reverseForwardList.set(node.reverseForwardList);
        this.reverseReverseList.set(node.reverseReverseList);
        this.kmer.set(node.kmer);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nodeID.readFields(in);
        this.forwardForwardList.readFields(in);
        this.forwardReverseList.readFields(in);
        this.reverseForwardList.readFields(in);
        this.reverseReverseList.readFields(in);
        this.kmer.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.nodeID.write(out);
        this.forwardForwardList.write(out);
        this.forwardReverseList.write(out);
        this.reverseForwardList.write(out);
        this.reverseReverseList.write(out);
        this.kmer.write(out);
    }

    @Override
    public int compareTo(NodeWritable other) {
        return this.nodeID.compareTo(other.nodeID);
    }

    @Override
    public int hashCode() {
        return nodeID.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append(nodeID.toString()).append('\t');
        sbuilder.append(forwardForwardList.toString()).append('\t');
        sbuilder.append(forwardReverseList.toString()).append('\t');
        sbuilder.append(reverseForwardList.toString()).append('\t');
        sbuilder.append(reverseReverseList.toString()).append('\t');
        sbuilder.append(kmer.toString()).append(')');
        return sbuilder.toString();
    }
    
    public int inDegree(){
        return reverseReverseList.getCountOfPosition() + reverseForwardList.getCountOfPosition();
    }
    
    public int outDegree(){
        return forwardForwardList.getCountOfPosition() + forwardReverseList.getCountOfPosition();
    }
    
    /*
     * Return if this node is a "path" compressible node, that is, it has an in-degree and out-degree of 1 
     */
    public boolean isPathNode() {
        return inDegree() == 1 && outDegree() == 1;
    }

}
