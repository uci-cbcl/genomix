package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class BubbleMergeMessageWritable extends MessageWritable{

    private VKmerBytesWritable startVertexId; //use for MergeBubble
    
    public BubbleMergeMessageWritable(){
        super();
        startVertexId = new VKmerBytesWritable();
    }
    
    public void reset(){
        super.reset();
        startVertexId.reset(0);
    }
    
    public VKmerBytesWritable getStartVertexId() {
        return startVertexId;
    }

    public void setStartVertexId(VKmerBytesWritable startVertexId) {
        this.startVertexId.setAsCopy(startVertexId);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        startVertexId.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        startVertexId.write(out);
    }
}
