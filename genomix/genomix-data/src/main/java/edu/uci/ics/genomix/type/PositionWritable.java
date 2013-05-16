package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PositionWritable implements Writable {
    private byte[] storage;
    private int offset;
    public static final int LENGTH = 5;
    public static final int INTBYTES = 4;

    public PositionWritable() {
        storage = new byte[LENGTH];
        offset = 0;
    }

    public PositionWritable(int readID, byte posInRead) {
        this();
        set(readID, posInRead);
    }

    public PositionWritable(byte[] storage, int offset) {
        setNewReference(storage, offset);
    }
    
    public void setNewReference(byte[] storage, int offset) {
        this.storage = storage;
        this.offset = offset;
    }
    
    public void set(int readID, byte posInRead) {
        putInt(readID, storage, offset);
        storage[offset + INTBYTES] = posInRead;
    }

    public int getReadID() {
        return getInt(storage, offset);
    }

    public byte getPosInRead() {
        return storage[offset + INTBYTES];
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
    
    public static int getInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }
    
    public static void putInt(int val, byte[] bytes, int offset) {
        bytes[offset] = (byte)((val >>> 24) & 0xFF);        
        bytes[offset + 1] = (byte)((val >>> 16) & 0xFF);
        bytes[offset + 2] = (byte)((val >>>  8) & 0xFF);
        bytes[offset + 3] = (byte)((val >>>  0) & 0xFF);
    }

}
