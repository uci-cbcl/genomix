package edu.uci.ics.genomix.oldtype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class IntermediateNodeWritable implements WritableComparable<IntermediateNodeWritable>, Serializable{

    private static final long serialVersionUID = 1L;
    public static final IntermediateNodeWritable EMPTY_NODE = new IntermediateNodeWritable();
    
    private KmerListWritable forwardForwardList;
    private KmerListWritable forwardReverseList;
    private KmerListWritable reverseForwardList;
    private KmerListWritable reverseReverseList;
    private PositionWritable nodeId;
    
    public IntermediateNodeWritable(){
        forwardForwardList = new KmerListWritable();
        forwardReverseList = new KmerListWritable();
        reverseForwardList = new KmerListWritable();
        reverseReverseList = new KmerListWritable();
        nodeId = new PositionWritable();
    }
    
    public IntermediateNodeWritable(KmerListWritable FFList, KmerListWritable FRList,
            KmerListWritable RFList, KmerListWritable RRList, PositionWritable uniqueKey) {
        this();
        set(FFList, FRList, RFList, RRList, uniqueKey);
    }
    
    public void set(IntermediateNodeWritable node){
        set(node.forwardForwardList, node.forwardReverseList, node.reverseForwardList, 
                node.reverseReverseList, node.nodeId);
    }
    
    public void set(KmerListWritable FFList, KmerListWritable FRList,
            KmerListWritable RFList, KmerListWritable RRList, PositionWritable uniqueKey) {
        this.forwardForwardList.set(FFList);
        this.forwardReverseList.set(FRList);
        this.reverseForwardList.set(RFList);
        this.reverseReverseList.set(RRList);
        this.nodeId.set(uniqueKey);
    }
    
    public KmerListWritable getFFList() {
        return forwardForwardList;
    }

    public void setFFList(KmerListWritable forwardForwardList) {
        this.forwardForwardList.set(forwardForwardList);
    }

    public KmerListWritable getFRList() {
        return forwardReverseList;
    }

    public void setFRList(KmerListWritable forwardReverseList) {
        this.forwardReverseList.set(forwardReverseList);
    }

    public KmerListWritable getRFList() {
        return reverseForwardList;
    }

    public void setRFList(KmerListWritable reverseForwardList) {
        this.reverseForwardList.set(reverseForwardList);
    }

    public KmerListWritable getRRList() {
        return reverseReverseList;
    }

    public void setRRList(KmerListWritable reverseReverseList) {
        this.reverseReverseList.set(reverseReverseList);
    }

	public PositionWritable getNodeId() {
        return nodeId;
    }

    public void setNodeId(PositionWritable nodeId) {
        this.nodeId.set(nodeId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.forwardForwardList.readFields(in);
        this.forwardReverseList.readFields(in);
        this.reverseForwardList.readFields(in);
        this.reverseReverseList.readFields(in);
        this.nodeId.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.forwardForwardList.write(out);
        this.forwardReverseList.write(out);
        this.reverseForwardList.write(out);
        this.reverseReverseList.write(out);
        this.nodeId.write(out);
    }

    @Override
    public int compareTo(IntermediateNodeWritable other) {
        // TODO Auto-generated method stub
        return this.nodeId.compareTo(other.nodeId);
    }
    
    @Override
    public int hashCode() {
        return this.nodeId.hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof IntermediateNodeWritable) {
            IntermediateNodeWritable nw = (IntermediateNodeWritable) o;
            return (this.forwardForwardList.equals(nw.forwardForwardList)
                    && this.forwardReverseList.equals(nw.forwardReverseList)
                    && this.reverseForwardList.equals(nw.reverseForwardList)
                    && this.reverseReverseList.equals(nw.reverseReverseList) && (this.nodeId.equals(nw.nodeId)));
        }
        return false;
    }
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append(nodeId.toString()).append('\t');
        sbuilder.append(forwardForwardList.toString()).append('\t');
        sbuilder.append(forwardReverseList.toString()).append('\t');
        sbuilder.append(reverseForwardList.toString()).append('\t');
        sbuilder.append(reverseReverseList.toString()).append('\t').append(')');
        return sbuilder.toString();
    }
}
