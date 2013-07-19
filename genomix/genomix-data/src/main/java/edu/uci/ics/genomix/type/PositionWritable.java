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
    
    public static final int totalBits = 64;
    private static final int bitsForMate = 1;
    private static final int bitsForPosition = 16;
    private static final int readIdShift = bitsForPosition + bitsForMate;
    private static final int positionIdShift = bitsForMate;
    
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
    
    public void set(long uuid){
        Marshal.putLong(uuid, storage, offset);
    }
    
    public void set(byte mateId, long readId, int posId){
        long uuid = (readId << readIdShift) + ((posId & 0xFFFF) << positionIdShift) + (mateId & 0b1);
        Marshal.putLong(uuid, storage, offset);
    }
    
    public void set(PositionWritable pos) {
        set(pos.getMateId(),pos.getReadId(),pos.getPosId());
    }
    
    public void setNewReference(byte[] storage, int offset) {
        this.storage = storage;
        this.offset = offset;
    }
    
    public long getUUID(){
        return Marshal.getLong(storage, offset);
    }
    
    public byte getMateId(){
        return (byte) (Marshal.getLong(storage, offset) & 0b1);
    }
    
    public long getReadId(){
        return Marshal.getLong(storage, offset) >>> readIdShift;
    }
    
    public int getPosId(){
        return (int) ((Marshal.getLong(storage, offset) >>> positionIdShift) & 0xffff);
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
    
    /*
     * String of form "(readId-posID_mate)" where mate is _1 or _2
     */
    @Override
    public String toString() {
        return "(" + this.getReadId() + "-" + this.getPosId() + "_" + (this.getMateId() + 1) + ")";
    }
}
