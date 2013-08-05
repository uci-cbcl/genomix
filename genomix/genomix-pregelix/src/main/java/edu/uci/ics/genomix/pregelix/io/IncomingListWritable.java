package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.PositionListWritable;

public class IncomingListWritable implements WritableComparable<IncomingListWritable>{
    private PositionListWritable reverseForwardList;
    private PositionListWritable reverseReverseList;
    
    public IncomingListWritable(){
        reverseForwardList = new PositionListWritable();
        reverseReverseList = new PositionListWritable();
    }

    public PositionListWritable getReverseForwardList() {
        return reverseForwardList;
    }

    public void setReverseForwardList(PositionListWritable reverseForwardList) {
        this.reverseForwardList = reverseForwardList;
    }

    public PositionListWritable getReverseReverseList() {
        return reverseReverseList;
    }

    public void setReverseReverseList(PositionListWritable reverseReverseList) {
        this.reverseReverseList = reverseReverseList;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reverseForwardList.readFields(in);
        reverseReverseList.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        reverseForwardList.write(out);
        reverseReverseList.write(out);
    }

    @Override
    public int compareTo(IncomingListWritable o) {
        return 0;
    }
    
    public int inDegree(){
        return reverseReverseList.getCountOfPosition() + reverseForwardList.getCountOfPosition();
    }
}
