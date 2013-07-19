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
    
    public PositionWritable(byte mateId, long readId, int posId){
        this();
        set(mateId, readId, posId);
    }
    
    public PositionWritable(byte[] storage, int offset) {
        setNewReference(storage, offset);
    }
    
    public void set(byte mateId, long readId, int posId){
        long finalId = (readId << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
        Marshal.putLong(finalId, storage, offset);
    }
    
    public void set(PositionWritable pos) {
        set(pos.getMateId(),pos.getReadId(),pos.getPosId());
    }
    
    public void setNewReference(byte[] storage, int offset) {
        this.storage = storage;
        this.offset = offset;
    }
    
    public byte getMateId(){
        return (byte) (Marshal.getLong(storage, offset) & 0b1);
    }
    
    public long getReadId(){
        return Marshal.getLong(storage, offset) >> 17;
    }
    
    public int getPosId(){
        return (int) ((Marshal.getLong(storage, offset) >> 1) & 0xffff);
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
