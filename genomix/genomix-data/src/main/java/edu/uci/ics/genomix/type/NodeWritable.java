package edu.uci.ics.genomix.type;


public class NodeWritable {
    private PositionWritable nodeID;
    private int countOfKmer;
    private PositionListWritable incomingList;
    private PositionListWritable outgoingList;

    public NodeWritable() {
        nodeID = new PositionWritable();
        countOfKmer = 0;
        incomingList = new PositionListWritable();
        outgoingList = new PositionListWritable();
    }
    
    public int getCount(){
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

    public void mergeNextWithinOneRead(NodeWritable nextNodeEntry) {
        this.countOfKmer += nextNodeEntry.countOfKmer;
        for(PositionWritable pos : nextNodeEntry.getOutgoingList()){
            this.outgoingList.append(pos);
        }
    }

    public void set(NodeWritable node) {
        this.nodeID.set(node.getNodeID().getReadID(), node.getNodeID().getPosInRead());
        this.countOfKmer = node.countOfKmer;
        this.incomingList.set(node.getIncomingList());
        this.outgoingList.set(node.getOutgoingList());
    }

}
