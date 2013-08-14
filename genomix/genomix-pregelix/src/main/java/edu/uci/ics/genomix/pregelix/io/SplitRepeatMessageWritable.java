package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EdgeWritable;

public class SplitRepeatMessageWritable extends MessageWritable {
    
    private EdgeWritable createdEdge;
    
    public SplitRepeatMessageWritable(){
        super();
        createdEdge = new EdgeWritable();
    }
    
    public void reset(){
        super.reset();
        createdEdge.reset();
    }

    public EdgeWritable getCreatedEdge() {
        return createdEdge;
    }

    public void setCreatedEdge(EdgeWritable createdEdge) {
        this.createdEdge = createdEdge;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        createdEdge.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        createdEdge.write(out);
    }
}
