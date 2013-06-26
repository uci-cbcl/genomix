package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class VertexValueWritable implements WritableComparable<VertexValueWritable> {

    private AdjacencyListWritable incomingList;
    private AdjacencyListWritable outgoingList;
    private byte state;
    private KmerBytesWritable mergeChain;
    private PositionWritable mergeDest;

    public VertexValueWritable() {
        incomingList = new AdjacencyListWritable();
        outgoingList = new AdjacencyListWritable();
        state = State.NON_VERTEX;
        mergeChain = new KmerBytesWritable(0);
        mergeDest = new PositionWritable();
    }

    public VertexValueWritable(PositionListWritable forwardForwardList, PositionListWritable forwardReverseList,
            PositionListWritable reverseForwardList, PositionListWritable reverseReverseList,
            byte state, KmerBytesWritable mergeChain) {
        set(forwardForwardList, forwardReverseList, 
                reverseForwardList, reverseReverseList,
                state, mergeChain);
    }
    
    public void set(PositionListWritable forwardForwardList, PositionListWritable forwardReverseList,
            PositionListWritable reverseForwardList, PositionListWritable reverseReverseList, 
            byte state, KmerBytesWritable mergeChain) {
        this.incomingList.setForwardList(reverseForwardList);
        this.incomingList.setReverseList(reverseReverseList);
        this.outgoingList.setForwardList(forwardForwardList);
        this.outgoingList.setReverseList(forwardReverseList);
        this.state = state;
        this.mergeChain.set(mergeChain);
    }
    
    public void set(VertexValueWritable value) {
        set(value.getFFList(),value.getFRList(),value.getRFList(),value.getRRList(),value.getState(),
                value.getMergeChain());
    }
    
    public PositionListWritable getFFList() {
        return outgoingList.getForwardList();
    }

    public PositionListWritable getFRList() {
        return outgoingList.getReverseList();
    }

    public PositionListWritable getRFList() {
        return incomingList.getForwardList();
    }

    public PositionListWritable getRRList() {
        return incomingList.getReverseList();
    }
    
    public void setFFList(PositionListWritable forwardForwardList){
        outgoingList.setForwardList(forwardForwardList);
    }
    
    public void setFRList(PositionListWritable forwardReverseList){
        outgoingList.setReverseList(forwardReverseList);
    }
    
    public void setRFList(PositionListWritable reverseForwardList){
        incomingList.setForwardList(reverseForwardList);
    }

    public void setRRList(PositionListWritable reverseReverseList){
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

    public void setState(byte state) {
        this.state = state;
    }

    public int getLengthOfMergeChain() {
        return mergeChain.getKmerLength();
    }

    public KmerBytesWritable getMergeChain() {
        return mergeChain;
    }

    public void setMergeChain(KmerBytesWritable mergeChain) {
        this.mergeChain.set(mergeChain);
    }
    
    public PositionWritable getMergeDest() {
        return mergeDest;
    }

    public void setMergeDest(PositionWritable mergeDest) {
        this.mergeDest = mergeDest;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        incomingList.readFields(in);
        outgoingList.readFields(in);
        state = in.readByte();
        mergeChain.readFields(in);
        mergeDest.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        incomingList.write(out);
        outgoingList.write(out);
        out.writeByte(state);
        mergeChain.write(out);
        mergeDest.write(out);
    }

    @Override
    public int compareTo(VertexValueWritable o) {
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append(outgoingList.getForwardList().toString()).append('\t');
        sbuilder.append(outgoingList.getReverseList().toString()).append('\t');
        sbuilder.append(incomingList.getForwardList().toString()).append('\t');
        sbuilder.append(incomingList.getReverseList().toString()).append('\t');
        sbuilder.append(mergeChain.toString()).append(')');
        return sbuilder.toString();
    }
    
    public int inDegree(){
        return incomingList.getForwardList().getCountOfPosition() + incomingList.getReverseList().getCountOfPosition();
    }
    
    public int outDegree(){
        return outgoingList.getForwardList().getCountOfPosition() + outgoingList.getReverseList().getCountOfPosition();
    }
}
