package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

public class IntermediateNodeWritable implements WritableComparable<IntermediateNodeWritable>, Serializable{

    private static final long serialVersionUID = 1L;
    public static final IntermediateNodeWritable EMPTY_NODE = new IntermediateNodeWritable();
    
    private KmerListWritable forwardForwardList;
    private KmerListWritable forwardReverseList;
    private KmerListWritable reverseForwardList;
    private KmerListWritable reverseReverseList;
    private long uuid;
    
    public IntermediateNodeWritable(){
        forwardForwardList = new KmerListWritable();
        forwardReverseList = new KmerListWritable();
        reverseForwardList = new KmerListWritable();
        reverseReverseList = new KmerListWritable();
        uuid = 0L;
    }
    
    public IntermediateNodeWritable(KmerListWritable FFList, KmerListWritable FRList,
            KmerListWritable RFList, KmerListWritable RRList, long uuid) {
        this();
        set(FFList, FRList, RFList, RRList, uuid);
    }
    
    public void set(IntermediateNodeWritable node){
        set(node.forwardForwardList, node.forwardReverseList, node.reverseForwardList, 
                node.reverseReverseList, node.uuid);
    }
    
    public void set(KmerListWritable FFList, KmerListWritable FRList,
            KmerListWritable RFList, KmerListWritable RRList, long uuid) {
        this.forwardForwardList.set(FFList);
        this.forwardReverseList.set(FRList);
        this.reverseForwardList.set(RFList);
        this.reverseReverseList.set(RRList);
        this.uuid = uuid;
    }

    public void reset(int kmerSize) {
        forwardForwardList.reset();
        forwardReverseList.reset();
        reverseForwardList.reset();
        reverseReverseList.reset();
        uuid = 0;
    }
    
    public KmerListWritable getFFList() {
        return forwardForwardList;
    }

    public void setFFList(KmerListWritable forwardForwardList) {
        this.forwardForwardList = forwardForwardList;
    }

    public KmerListWritable getFReList() {
        return forwardReverseList;
    }

    public void setFRList(KmerListWritable forwardReverseList) {
        this.forwardReverseList = forwardReverseList;
    }

    public KmerListWritable getRFList() {
        return reverseForwardList;
    }

    public void setRFList(KmerListWritable reverseForwardList) {
        this.reverseForwardList = reverseForwardList;
    }

    public KmerListWritable getRRList() {
        return reverseReverseList;
    }

    public void setRRList(KmerListWritable reverseReverseList) {
        this.reverseReverseList = reverseReverseList;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.forwardForwardList.readFields(in);
        this.forwardReverseList.readFields(in);
        this.reverseForwardList.readFields(in);
        this.reverseReverseList.readFields(in);
        this.uuid = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.forwardForwardList.write(out);
        this.forwardReverseList.write(out);
        this.reverseForwardList.write(out);
        this.reverseReverseList.write(out);
        out.writeLong(this.uuid);
    }

    @Override
    public int compareTo(IntermediateNodeWritable other) {
        // TODO Auto-generated method stub
        return this.uuid > other.uuid ? 1 : ((this.uuid == other.uuid) ? 0 : -1);
    }
    
    @Override
    public int hashCode() {
        return Long.valueOf(this.uuid).hashCode(); 
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof IntermediateNodeWritable) {
            IntermediateNodeWritable nw = (IntermediateNodeWritable) o;
            return (this.forwardForwardList.equals(nw.forwardForwardList)
                    && this.forwardReverseList.equals(nw.forwardReverseList)
                    && this.reverseForwardList.equals(nw.reverseForwardList)
                    && this.reverseReverseList.equals(nw.reverseReverseList) && (this.uuid == nw.uuid));
        }
        return false;
    }
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append(forwardForwardList.toString()).append('\t');
        sbuilder.append(forwardReverseList.toString()).append('\t');
        sbuilder.append(reverseForwardList.toString()).append('\t');
        sbuilder.append(reverseReverseList.toString()).append('\t');
        sbuilder.append(uuid).append(')');
        return sbuilder.toString();
    }
}
