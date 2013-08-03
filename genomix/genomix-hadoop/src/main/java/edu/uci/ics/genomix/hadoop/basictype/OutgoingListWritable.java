package edu.uci.ics.genomix.hadoop.basictype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.oldtype.PositionListWritable;

public class OutgoingListWritable implements WritableComparable<OutgoingListWritable>{
    private PositionListWritable forwardForwardList;
    private PositionListWritable forwardReverseList;
    
    public OutgoingListWritable(){
        forwardForwardList = new PositionListWritable();
        forwardReverseList = new PositionListWritable();
    }

    public PositionListWritable getForwardForwardList() {
        return forwardForwardList;
    }

    public void setForwardForwardList(PositionListWritable forwardForwardList) {
        this.forwardForwardList = forwardForwardList;
    }

    public PositionListWritable getForwardReverseList() {
        return forwardReverseList;
    }

    public void setForwardReverseList(PositionListWritable forwardReverseList) {
        this.forwardReverseList = forwardReverseList;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        forwardForwardList.readFields(in);
        forwardReverseList.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        forwardForwardList.write(out);
        forwardReverseList.write(out);   
    }

    @Override
    public int compareTo(OutgoingListWritable o) {
        return 0;
    }
    
    public int outDegree(){
        return forwardForwardList.getCountOfPosition() + forwardReverseList.getCountOfPosition();
    }
}
