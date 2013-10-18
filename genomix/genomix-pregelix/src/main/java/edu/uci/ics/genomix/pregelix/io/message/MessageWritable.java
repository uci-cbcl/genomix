package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.io.WritableSizable;

public class MessageWritable implements Writable, WritableSizable {

    private VKmer sourceVertexId; // stores srcNode id
    private short flag; // stores message type

    public MessageWritable() {
        sourceVertexId = new VKmer();
        flag = 0;
    }

    public void setAsCopy(MessageWritable other) {
        setSourceVertexId(other.getSourceVertexId());
        flag = other.getFlag();
    }

    public void reset() {
        sourceVertexId.reset(0);
        flag = 0;
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append(sourceVertexId.toString());
        sbuilder.append('}');
        return sbuilder.toString();
    }

    public VKmer getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(VKmer sourceVertexId) {
        this.sourceVertexId.setAsCopy(sourceVertexId);
    }

    public short getFlag() {
        return flag;
    }

    public void setFlag(short flag) {
        this.flag = flag;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        sourceVertexId.readFields(in);
        flag = in.readShort();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        sourceVertexId.write(out);
        out.writeShort(flag);
    }

    @Override
    public int sizeInBytes() {
        // TODO Auto-generated method stub
        return 0;
    }

}
