package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EdgeWritable;

public class SplitRepeatMessageWritable extends MessageWritable {
    
    private EdgeWritable createdEdge;
    private EdgeWritable deletedEdge;
    
    public SplitRepeatMessageWritable(){
        super();
        createdEdge = new EdgeWritable();
        deletedEdge = new EdgeWritable();
    }
    
    public void reset(){
        super.reset();
        createdEdge.reset();
        deletedEdge.reset();
    }

    public EdgeWritable getCreatedEdge() {
        return createdEdge;
    }

    public void setCreatedEdge(EdgeWritable createdEdge) {
        this.createdEdge.setAsCopy(createdEdge);
    }
    
    public EdgeWritable getDeletedEdge() {
        return deletedEdge;
    }

    public void setDeletedEdge(EdgeWritable deletedEdge) {
        this.deletedEdge.setAsCopy(deletedEdge);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        createdEdge.readFields(in);
        deletedEdge.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        createdEdge.write(out);
        deletedEdge.write(out);
    }
}
