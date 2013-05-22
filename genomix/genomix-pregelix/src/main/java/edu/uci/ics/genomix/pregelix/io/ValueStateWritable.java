package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;

public class ValueStateWritable implements WritableComparable<ValueStateWritable> {

    private PositionListWritable incomingList;
    private PositionListWritable outgoingList;
    private byte state;
    private KmerBytesWritable mergeChain;

    public ValueStateWritable() {
        incomingList = new PositionListWritable();
        outgoingList = new PositionListWritable();
        state = State.NON_VERTEX;
        mergeChain = new KmerBytesWritable(0);
    }

    public ValueStateWritable(PositionListWritable incomingList, PositionListWritable outgoingList, 
            byte state, KmerBytesWritable mergeChain) {
        set(incomingList, outgoingList, state, mergeChain);
    }

    public void set(PositionListWritable incomingList, PositionListWritable outgoingList, 
            byte state, KmerBytesWritable mergeChain) {
        this.incomingList.set(incomingList);
        this.outgoingList.set(outgoingList);
        this.state = state;
        this.mergeChain.set(mergeChain);
    }
    
    public PositionListWritable getIncomingList() {
        return incomingList;
    }

    public void setIncomingList(PositionListWritable incomingList) {
        this.incomingList = incomingList;
    }

    public PositionListWritable getOutgoingList() {
        return outgoingList;
    }

    public void setOutgoingList(PositionListWritable outgoingList) {
        this.outgoingList = outgoingList;
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

    @Override
    public void readFields(DataInput in) throws IOException {
        incomingList.readFields(in);
        outgoingList.readFields(in);
        state = in.readByte();
        mergeChain.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        incomingList.write(out);
        outgoingList.write(out);
        out.writeByte(state);
        mergeChain.write(out);
    }

    @Override
    public int compareTo(ValueStateWritable o) {
        return 0;
    }

    @Override
    public String toString() {
        return state + "\t" + getLengthOfMergeChain() + "\t" + mergeChain.toString();
    }
    
    public int inDegree() {
        return incomingList.getCountOfPosition();
    }

    public int outDegree() {
        return outgoingList.getCountOfPosition();
    }
}
