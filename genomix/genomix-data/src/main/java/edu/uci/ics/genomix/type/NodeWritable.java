package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class NodeWritable implements WritableComparable<NodeWritable> {
    private PositionWritable nodeID;
    private int countOfKmer;
    private PositionListWritable incomingList;
    private PositionListWritable outgoingList;
    private KmerBytesWritable kmer;
    
    public NodeWritable(){
        nodeID = new PositionWritable();
        countOfKmer = 0;
        incomingList = new PositionListWritable();
        outgoingList = new PositionListWritable();
        kmer = new KmerBytesWritable();
    }

    public NodeWritable(int kmerSize) {
        nodeID = new PositionWritable();
        countOfKmer = 0;
        incomingList = new PositionListWritable();
        outgoingList = new PositionListWritable();
        kmer = new KmerBytesWritable(kmerSize);
    }

    public int getCount() {
        return countOfKmer;
    }

    public void setCount(int count) {
        this.countOfKmer = count;
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

    public void setOutgoingList(PositionListWritable outgoing) {
        outgoingList.set(outgoing);
    }

    public void reset() {
        nodeID.set(0, (byte) 0);
        incomingList.reset();
        outgoingList.reset();
        countOfKmer = 0;
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

    public void mergeNextWithinOneRead(NodeWritable nextNodeEntry) {
        this.countOfKmer += 1;
        this.outgoingList.set(nextNodeEntry.outgoingList);
        kmer.mergeKmerWithNextCode(nextNodeEntry.kmer.getGeneCodeAtPosition(nextNodeEntry.kmer.getKmerLength() - 1));
    }

    public void set(NodeWritable node) {
        this.nodeID.set(node.getNodeID().getReadID(), node.getNodeID().getPosInRead());
        this.countOfKmer = node.countOfKmer;
        this.incomingList.set(node.getIncomingList());
        this.outgoingList.set(node.getOutgoingList());
        this.kmer.set(node.kmer);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.nodeID.readFields(in);
        this.countOfKmer = in.readInt();
        this.incomingList.readFields(in);
        this.outgoingList.readFields(in);
        this.kmer.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.nodeID.write(out);
        out.writeInt(this.countOfKmer);
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
    public String toString(){
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append(nodeID.toString()).append(',');
        sbuilder.append(countOfKmer).append(',');
        sbuilder.append(incomingList.toString()).append(',');
        sbuilder.append(incomingList.toString()).append(',');
        sbuilder.append(kmer.toString()).append(')');
        return sbuilder.toString();
    }

}
