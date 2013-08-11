package edu.uci.ics.genomix.pregelix.io.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.io.WritableSizable;

public class MessageWritable implements Writable, WritableSizable {
    
    private VKmerBytesWritable sourceVertexId; // stores srcNode id
    private byte flag; // stores message type
    
    public MessageWritable(){
        sourceVertexId = new VKmerBytesWritable();
        flag = 0;
    }
    
    public void reset(){
        sourceVertexId.reset(0);
        flag = 0;
    }
    
    public VKmerBytesWritable getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(VKmerBytesWritable sourceVertexId) {
        this.sourceVertexId.setAsCopy(sourceVertexId);
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        sourceVertexId.readFields(in);
        flag = in.readByte();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        sourceVertexId.write(out);
        out.writeByte(flag);
    }

    @Override
    public int sizeInBytes() {
        // TODO Auto-generated method stub
        return 0;
    }

}
