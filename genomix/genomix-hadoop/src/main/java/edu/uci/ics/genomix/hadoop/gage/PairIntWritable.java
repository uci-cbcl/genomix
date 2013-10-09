package edu.uci.ics.genomix.hadoop.gage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.genomix.util.Marshal;

public class PairIntWritable implements WritableComparable<PairIntWritable> {

    IntWritable primaryKey;
    IntWritable secondaryKey;

    int getPrimaryKey() {
        return primaryKey.get();
    }

    int getSecondaryKey() {
        return secondaryKey.get();
    }

    void setPrimaryKey(int value) {
        this.primaryKey.set(value);

    }

    void setSecondaryKey(int value) {
        this.secondaryKey.set(value);
    }

    void set(int a, int b) {
        this.primaryKey.set(a);
        this.secondaryKey.set(b);
    }

    @Override
    public int hashCode() {
        return primaryKey.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PairIntWritable))
            return false;
        PairIntWritable other = (PairIntWritable) o;
        return getPrimaryKey() == other.getPrimaryKey() && getSecondaryKey() == other.getSecondaryKey();
    }

    @Override
    public int compareTo(PairIntWritable other) {
        int diff1 = this.getPrimaryKey() - other.getPrimaryKey();
        if (diff1 == 0) {
            return this.getSecondaryKey() - other.getSecondaryKey();
        }
        return diff1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(getPrimaryKey());
        out.writeInt(getSecondaryKey());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.primaryKey.set(in.readInt());
        this.secondaryKey.set(in.readInt());
    }

    @Override
    public String toString() {
        return "(" + getPrimaryKey() + getSecondaryKey() + ")";
    }

    public static class Comparator extends WritableComparator {
        protected Comparator() {
            super(PairIntWritable.class);
        }

        @Override
        public int compare(WritableComparable et1, WritableComparable et2) {
            PairIntWritable ip1 = (PairIntWritable) et1;
            PairIntWritable ip2 = (PairIntWritable) et2;
            return ip1.compareTo(ip2);
        }
    }

    public static class FirstComparator implements RawComparator<PairIntWritable> {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int primaryKey1 = Marshal.getInt(b1, s1);
            int primaryKey2 = Marshal.getInt(b2, s2);
            return primaryKey1 - primaryKey2;
        }

        @Override
        public int compare(PairIntWritable o1, PairIntWritable o2) {
            return o1.getPrimaryKey() - o2.getPrimaryKey();
        }

    }

    static { // register this comparator
        WritableComparator.define(PairIntWritable.class, new Comparator());
    }

}
