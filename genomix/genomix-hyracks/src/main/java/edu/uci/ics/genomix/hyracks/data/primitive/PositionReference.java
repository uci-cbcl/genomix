package edu.uci.ics.genomix.hyracks.data.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class PositionReference implements IValueReference, WritableComparable<PositionReference> {
    private byte[] storage;
    private int offset;
    public static final int LENGTH = 5;
    public static final int INTBYTES = 4;

    public PositionReference() {
        storage = new byte[LENGTH];
        offset = 0;
    }

    public PositionReference(byte[] storage, int offset) {
        setNewReference(storage, offset);
    }

    public PositionReference(int readID, byte posInRead) {
        this();
        set(readID, posInRead);
    }

    public void set(int readID, byte posInRead) {
        IntegerSerializerDeserializer.putInt(readID, storage, offset);
        storage[offset + INTBYTES] = posInRead;
    }

    public void setNewReference(byte[] storage, int offset) {
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

    @Override
    public int hashCode() {
        return this.getReadID();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PositionReference))
            return false;
        PositionReference other = (PositionReference) o;
        return this.getReadID() == other.getReadID() && this.getPosInRead() == other.getPosInRead();
    }

    @Override
    public int compareTo(PositionReference other) {
        int diff = this.getReadID() - other.getReadID();
        if (diff == 0) {
            return this.getPosInRead() - other.getPosInRead();
        }
        return diff;
    }

    @Override
    public String toString() {
        return "(" + Integer.toString(getReadID()) + "," + Integer.toString((int) getPosInRead()) + ")";
    }

    /** A Comparator optimized for IntWritable. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(PositionReference.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = IntegerSerializerDeserializer.getInt(b1, s1);
            int thatValue = IntegerSerializerDeserializer.getInt(b2, s2);
            int diff = thisValue - thatValue;
            if (diff == 0){
                return b1[s1+INTBYTES] - b2[s2+INTBYTES];
            }
            return diff;
        }
    }

    static { // register this comparator
        WritableComparator.define(PositionReference.class, new Comparator());
    }
}
