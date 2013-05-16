package edu.uci.ics.genomix.hyracks.data.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.KmerBytesWritable;

public class NodeReference implements WritableComparable<NodeReference> {
    private PositionReference nodeID;
    private int countOfKmer;
    private PositionListReference incomingList;
    private PositionListReference outgoingList;
    private KmerBytesWritable kmer;

    public NodeReference(int kmerSize) {
        nodeID = new PositionReference();
        countOfKmer = 0;
        incomingList = new PositionListReference();
        outgoingList = new PositionListReference();
        kmer = new KmerBytesWritable(kmerSize);
    }

    public int getCount() {
        return countOfKmer;
    }

    public void setCount(int count) {
        this.countOfKmer = count;
    }

    public void setNodeID(PositionReference ref) {
        this.setNodeID(ref.getReadID(), ref.getPosInRead());
    }

    public void setNodeID(int readID, byte posInRead) {
        nodeID.set(readID, posInRead);
    }

    public void setIncomingList(PositionListReference incoming) {
        incomingList.set(incoming);
    }

    public void setOutgoingList(PositionListReference outgoing) {
        outgoingList.set(outgoing);
    }

    public void reset() {
        nodeID.set(0, (byte) 0);
        incomingList.reset();
        outgoingList.reset();
        countOfKmer = 0;
    }

    public PositionListReference getIncomingList() {
        return incomingList;
    }

    public PositionListReference getOutgoingList() {
        return outgoingList;
    }

    public PositionReference getNodeID() {
        return nodeID;
    }

    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public void mergeNextWithinOneRead(NodeReference nextNodeEntry) {
        this.countOfKmer += 1;
        this.outgoingList.set(nextNodeEntry.outgoingList);
        kmer.mergeKmerWithNextCode(nextNodeEntry.kmer.getGeneCodeAtPosition(nextNodeEntry.kmer.getKmerLength() - 1));
    }

    public void set(NodeReference node) {
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
    public int compareTo(NodeReference other) {
        return this.nodeID.compareTo(other.nodeID);
    }

    @Override
    public int hashCode() {
        return nodeID.hashCode();
    }
}
