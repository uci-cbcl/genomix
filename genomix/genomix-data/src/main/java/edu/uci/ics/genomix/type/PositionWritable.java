package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.genomix.data.Marshal;

public class PositionWritable implements WritableComparable<PositionWritable>, Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
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

    public void set(PositionWritable pos) {
        set(pos.getReadID(), pos.getPosInRead());
    }

    public void set(int readID, byte posInRead) {
        Marshal.putInt(readID, storage, offset);
        storage[offset + INTBYTES] = posInRead;
    }

    public int getReadID() {
        return Marshal.getInt(storage, offset);
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

    public boolean isSameReadID(PositionWritable other) {
        return getReadID() == other.getReadID();
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
        return this.getReadID() == other.getReadID() && this.getPosInRead() == other.getPosInRead();
    }

    @Override
    public int compareTo(PositionWritable other) {
        int diff1 = this.getReadID() - other.getReadID();
        if (diff1 == 0) {
            int diff2 = Math.abs((int) this.getPosInRead()) - Math.abs((int) other.getPosInRead());
            if (diff2 == 0) {
                return this.getPosInRead() - other.getPosInRead();
            }
            return diff2;
        }
        return diff1;
    }

    @Override
    public String toString() {
        return "(" + Integer.toString(getReadID()) + "," + Integer.toString((int) getPosInRead()) + ")";
    }

    /** A Comparator optimized for IntWritable. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(PositionWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int thisValue = Marshal.getInt(b1, s1);
            int thatValue = Marshal.getInt(b2, s2);
            int diff1 = thisValue - thatValue;
            if (diff1 == 0) {
                int diff2 = Math.abs((int) b1[s1 + INTBYTES]) - Math.abs((int) b2[s2 + INTBYTES]);
                if (diff2 == 0) {
                    return b1[s1 + INTBYTES] - b2[s2 + INTBYTES];
                }
                return diff2;
            }
            return diff1;
        }
    }

    public static class FirstComparator implements RawComparator<PositionWritable> {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, l1 - 1, b2, s2, l2 - 1);
        }

        @Override
        public int compare(PositionWritable o1, PositionWritable o2) {
            int l = o1.getReadID();
            int r = o2.getReadID();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    static { // register this comparator
        WritableComparator.define(PositionWritable.class, new Comparator());
    }
}
