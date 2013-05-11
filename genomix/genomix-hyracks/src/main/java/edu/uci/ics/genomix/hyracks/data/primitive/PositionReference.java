package edu.uci.ics.genomix.hyracks.data.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class PositionReference implements IValueReference,Writable {
    private byte[] storage;
    private int offset;
    public static final int LENGTH = 5;
    public static final int INTBYTES = 4;

    public PositionReference() {
        storage = new byte[LENGTH];
        offset = 0;
    }

    public PositionReference(byte[] storage, int offset) {
        setNewSpace(storage, offset);
    }

    public PositionReference(int readID, byte posInRead) {
        this();
        IntegerSerializerDeserializer.putInt(readID, storage, offset);
        storage[offset + INTBYTES] = posInRead;
    }

    public void set(int readID, byte posInRead) {
        IntegerSerializerDeserializer.putInt(readID, storage, offset);
        storage[offset + INTBYTES] = posInRead;
    }

    public void setNewSpace(byte[] storage, int offset) {
        this.storage = storage;
        this.offset = offset;
    }

    public int getReadID() {
        return IntegerSerializerDeserializer.getInt(storage, offset);
    }

    public byte getPosInRead() {
        return storage[offset + INTBYTES];
    }

    @Override
    public byte[] getByteArray() {
        return storage;
    }

    @Override
    public int getStartOffset() {
        return offset;
    }

    @Override
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

}
