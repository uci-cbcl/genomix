package edu.uci.ics.genomix.hyracks.data.primitive;

import edu.uci.ics.genomix.type.KmerBytesWritable;


public class NodeReference {
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
    
    public int getCount(){
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
    
    public KmerBytesWritable getKmer(){
        return kmer;
    }

    public void mergeNextWithinOneRead(NodeReference nextNodeEntry) {
        this.countOfKmer += 1;
        this.outgoingList.set(nextNodeEntry.outgoingList);
        kmer.mergeKmerWithNextCode(nextNodeEntry.kmer.getGeneCodeAtPosition(nextNodeEntry.kmer.getKmerLength()-1));
    }

    public void set(NodeReference node) {
        this.nodeID.set(node.getNodeID().getReadID(), node.getNodeID().getPosInRead());
        this.countOfKmer = node.countOfKmer;
        this.incomingList.set(node.getIncomingList());
        this.outgoingList.set(node.getOutgoingList());
        this.kmer.set(node.kmer);
    }

}
