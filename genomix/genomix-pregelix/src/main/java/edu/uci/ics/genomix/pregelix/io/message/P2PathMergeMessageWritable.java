package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.io.common.LinkedListWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;

public class P2PathMergeMessageWritable extends PathMergeMessageWritable{
    
    private LinkedListWritable<VKmerListWritable> mergeList;
    
    public P2PathMergeMessageWritable(){
        super();
        mergeList = new LinkedListWritable<VKmerListWritable>();
    }

    public void reset(){
        super.reset();
        mergeList.clear();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        mergeList.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        mergeList.write(out);
    }
}
