package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.util.Marshal;

public class ReadHeadInfo implements Writable, Serializable{
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
    public static final int LENGTH = 8;
    
    public static final int totalBits = 64;
    private static final int bitsForMate = 1;
    private static final int bitsForPosition = 16;
    private static final int readIdShift = bitsForPosition + bitsForMate;
    private static final int positionIdShift = bitsForMate;
    
    public ReadHeadInfo() {
        storage = new byte[LENGTH];
        offset = 0;
    }
    
    public ReadHeadInfo(byte mateId, long readId, int posId){
        this();
        set(mateId, readId, posId);
    }
    
    public ReadHeadInfo(ReadHeadInfo other) {
        this();
        set(other);
    }
    public ReadHeadInfo(byte[] storage, int offset) {
        setNewReference(storage, offset);
    }
    
    public void set(long uuid){
        Marshal.putLong(uuid, storage, offset);
    }
    
    public static long makeUUID(byte mateId, long readId, int posId) {
        return (readId << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
    }
    
    public void set(byte mateId, long readId, int posId){
        Marshal.putLong(makeUUID(mateId, readId, posId), storage, offset);
    }
    
    public void set(ReadHeadInfo pos) {
        set(pos.getMateId(),pos.getReadId(),pos.getPosId());
    }
    
    public void setNewReference(byte[] storage, int offset) {
        this.storage = storage;
        this.offset = offset;
    }
    
    public void reset(){
        storage = new byte[LENGTH];
        offset = 0;
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
        if (!(o instanceof ReadHeadInfo))
            return false;
        ReadHeadInfo other = (ReadHeadInfo) o;
        return this.getUUID() == other.getUUID();
    }
    
    /*
     * String of form "(readId-posID_mate)" where mate is _0 or _1
     */
    @Override
    public String toString() {
        return "(" + this.getReadId() + "-" + this.getPosId() + "_" + (this.getMateId()) + ")";
    }
}
