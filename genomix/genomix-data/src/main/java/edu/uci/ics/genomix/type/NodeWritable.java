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
    private PositionListWritable incomingList;
    private PositionListWritable outgoingList;
    private KmerBytesWritable kmer;

    public NodeWritable() {
        nodeID = new PositionWritable();
        incomingList = new PositionListWritable();
        outgoingList = new PositionListWritable();
        kmer = new KmerBytesWritable();
    }

    public NodeWritable(int kmerSize) {
        nodeID = new PositionWritable();
        incomingList = new PositionListWritable();
        outgoingList = new PositionListWritable();
        kmer = new KmerBytesWritable(kmerSize);
    }

    public void setNodeID(PositionWritable ref) {
        this.setNodeID(ref.getReadID(), ref.getPosInRead());
    }

    public void setNodeID(int readID, byte posInRead) {
        nodeID.set(readID, posInRead);
    }

    public void setIncomingList(PositionListWritable incoming) {
        incomingList.set(incoming);
    }

    public void setKmer(KmerBytesWritable kmer) {
        this.kmer = kmer;
    }

    public void setOutgoingList(PositionListWritable outgoing) {
        outgoingList.set(outgoing);
    }

    public void reset(int kmerSize) {
        nodeID.set(0, (byte) 0);
        incomingList.reset();
        outgoingList.reset();
        kmer.reset(kmerSize);
    }

    public PositionListWritable getIncomingList() {
        return incomingList;
    }

    public PositionListWritable getOutgoingList() {
        return outgoingList;
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

    public void mergeNext(NodeWritable nextNode, int initialKmerSize) {
        this.outgoingList.set(nextNode.outgoingList);
        kmer.mergeNextKmer(initialKmerSize, nextNode.getKmer());
    }
    
    public void mergePre(NodeWritable preNode, int initialKmerSize){
        this.incomingList.set(preNode.incomingList);
        kmer.mergePreKmer(initialKmerSize, preNode.getKmer());
    }

    public void set(NodeWritable node) {
        this.nodeID.set(node.getNodeID().getReadID(), node.getNodeID().getPosInRead());
        this.incomingList.set(node.getIncomingList());
        this.outgoingList.set(node.getOutgoingList());
        this.kmer.set(node.kmer);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nodeID.readFields(in);
        this.incomingList.readFields(in);
        this.outgoingList.readFields(in);
        this.kmer.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.nodeID.write(out);
        this.incomingList.write(out);
        this.outgoingList.write(out);
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
        sbuilder.append(incomingList.toString()).append('\t');
        sbuilder.append(outgoingList.toString()).append('\t');
        sbuilder.append(kmer.toString()).append(')');
        return sbuilder.toString();
    }
    
    /*
     * Return if this node is a "path" compressible node, that is, it has an in-degree and out-degree of 1 
     */
    public boolean isPathNode() {
        return incomingList.getCountOfPosition() == 1 && outgoingList.getCountOfPosition() == 1;
    }

}
