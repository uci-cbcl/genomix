package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.PositionWritable;

public class PositionWritable implements WritableComparable<PositionWritable>, Serializable{
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
    public static final int LENGTH = 8;
    
    public PositionWritable() {
        storage = new byte[LENGTH];
        offset = 0;
    }
    
    public PositionWritable(long uuid){
        this();
        set(uuid);
    }
    
    public PositionWritable(byte[] storage, int offset) {
        setNewReference(storage, offset);
    }
    
    public void set(long uuid){
        Marshal.putLong(uuid, storage, offset);
    }
    
    public void set(PositionWritable pos) {
        set(pos.getUUID());
    }
    
    public void setNewReference(byte[] storage, int offset) {
        this.storage = storage;
        this.offset = offset;
    }
    
    public long getUUID(){
        return Marshal.getLong(storage, offset);
    }
    
    public byte[] getByteArray() {
        return storage;
    }

    public int getStartOffset() {
        return offset;
    }

    public int getLength() {
        return LENGTH;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readFully(storage, offset, LENGTH);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(storage, offset, LENGTH);
    }
    
    @Override
    public int hashCode() {
        return Marshal.hashBytes(getByteArray(), getStartOffset(), getLength());
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PositionWritable))
            return false;
        PositionWritable other = (PositionWritable) o;
        return this.getUUID() == other.getUUID();
    }
    
    @Override
    public int compareTo(PositionWritable other) {
        return (this.getUUID() < other.getUUID()) ? -1 : ((this.getUUID() == other.getUUID()) ? 0 : 1);
    }
    
    @Override
    public String toString() {
        return "(" + Long.toString(this.getUUID()) + ")";
    }
}
