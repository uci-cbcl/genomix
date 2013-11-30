package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;

public class BubbleMergeWithSearchVertexValueWritable extends VertexValueWritable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    
    public BubbleMergeWithSearchVertexValueWritable(){
        super();
    }
    
    public void setAsCopy(BubbleMergeWithSearchVertexValueWritable other) {
        super.setAsCopy(other);
    }
    
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
    }
}
