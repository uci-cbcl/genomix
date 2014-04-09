package edu.uci.ics.genomix.pregelix.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.pregelix.api.io.WritableSizable;

public class MessageWritable implements Writable, WritableSizable {

    protected static class MESSAGE_FIELDS {
        public static final byte SOURCE_VERTEX_ID = 1 << 0; // used in superclass: MessageWritable
    }

    protected VKmer sourceVertexId; // stores srcNode id
    protected short flag; // stores message type

    protected byte messageFields; // only used during read to keep track of what's in the stream

    public MessageWritable() {
        sourceVertexId = null;
        flag = 0;
    }

    public void setAsCopy(MessageWritable other) {
    	reset();
        setSourceVertexId(other.getSourceVertexId());
        flag = other.getFlag();
    }

    public void reset() {
        sourceVertexId = null;
        flag = 0;
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append(sourceVertexId == null ? "null" : sourceVertexId.toString());
        sbuilder.append('}');
        return sbuilder.toString();
    }

    public VKmer getSourceVertexId() {
        if (sourceVertexId == null) {
            sourceVertexId = new VKmer();
        }
        return sourceVertexId;
    }

    public void setSourceVertexId(VKmer sourceVertexId) {
        getSourceVertexId().setAsCopy(sourceVertexId);
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
        messageFields = in.readByte();
        if ((messageFields & MESSAGE_FIELDS.SOURCE_VERTEX_ID) != 0)
            getSourceVertexId().readFields(in);
        flag = in.readShort();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(getActiveMessageFields());
        if (sourceVertexId != null) {
            sourceVertexId.write(out);
        }
        out.writeShort(flag);
    }
    
    protected byte getActiveMessageFields() {
        byte messageFields = 0;
        if (sourceVertexId != null) {
            messageFields |= MESSAGE_FIELDS.SOURCE_VERTEX_ID;
        }
        return messageFields;
    }

    @Override
    public int sizeInBytes() {
        return Byte.SIZE / 8 + (sourceVertexId == null ? 0 : sourceVertexId.getLength()) + Short.SIZE / 8;
    }

}
