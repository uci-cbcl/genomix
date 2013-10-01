package edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.io.WritableSizable;

public class MsgWritable implements WritableComparable<MsgWritable> {
    
    private VKmerBytesWritable sourceVertexId; // stores srcNode id
    private byte flag; // stores message type
    
    public MsgWritable(){
        sourceVertexId = new VKmerBytesWritable();
        flag = 0;
    }
    
    public MsgWritable(MsgWritable other) {
        this();
        sourceVertexId = other.getSourceVertexId();
        flag = other.getFlag();
    }
    
    public void reset(){
        sourceVertexId.reset(0);
        flag = 0;
    }
    
    public void setAsCopy(MsgWritable other) {
        sourceVertexId.setAsCopy(other.getSourceVertexId());
        this.flag = other.getFlag();
    }
    @Override
    public String toString(){
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append(sourceVertexId.toString());
        sbuilder.append('}');
        return sbuilder.toString();
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
    public int compareTo(MsgWritable o) {
        return this.sourceVertexId.compareTo(o.getSourceVertexId());
    }

}
