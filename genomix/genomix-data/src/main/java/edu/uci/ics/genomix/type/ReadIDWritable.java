package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.WritableComparable;
import edu.uci.ics.genomix.data.Marshal;

public class ReadIDWritable implements WritableComparable<ReadIDWritable>, Serializable{
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
    public static final int LENGTH = 6;
    
    public static final int totalBits = 48;
    private static final int bitsForMate = 1;
    private static final int readIdShift = bitsForMate;
    
    public ReadIDWritable() {
        storage = new byte[LENGTH];
        offset = 0;
    }
    
    public ReadIDWritable(byte mateId, long readId){
        this();
        set(mateId, readId);
    }
    
    public ReadIDWritable(byte[] storage, int offset) {
        setNewReference(storage, offset);
    }
    
    public void set(long readIDByte){
        Marshal.putLong(readIDByte, storage, offset);
    }
    
    public static long serialize(byte mateId, long readId) {
        return (readId << 1) + (mateId & 0b1);
    }
    
    public void set(byte mateId, long readId){
        Marshal.putLong(serialize(mateId, readId), storage, offset);
    }
    
    public void set(ReadIDWritable pos) {
        set(pos.getMateId(),pos.getReadId());
    }
    
    public void setNewReference(byte[] storage, int offset) {
        this.storage = storage;
        this.offset = offset;
    }
    
    public void reset(){
        storage = new byte[LENGTH];
        offset = 0;
    }
    
    public long deserialize(){
        return Marshal.getLong(storage, offset);
    }
    
    public byte getMateId(){
        return (byte) (Marshal.getLong(storage, offset) & 0b1);
    }
    
    public long getReadId(){
        return Marshal.getLong(storage, offset) >>> readIdShift;
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
        if (!(o instanceof ReadIDWritable))
            return false;
        ReadIDWritable other = (ReadIDWritable) o;
        return this.deserialize() == other.deserialize();
    }
    
    @Override
    public int compareTo(ReadIDWritable other) {
        return (this.deserialize() < other.deserialize()) ? -1 : ((this.deserialize() == other.deserialize()) ? 0 : 1);
    }
    
    /*
     * String of form "(readId-posID_mate)" where mate is _1 or _2
     */
    @Override
    public String toString() {
        return "(" + this.getReadId() + "_" + (this.getMateId() + 1) + ")";
    }
}
