package edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.genomix.data.KmerUtil;
import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class GraphCleanDestId extends VKmerBytesWritable {

    /**
     * 
     */

    private static final long serialVersionUID = 1L;
    public byte secSortStamp;

    public GraphCleanDestId() {
        secSortStamp = 0b00 << 0;
    }

    public GraphCleanDestId(int k) {
        super(k);
    }

    public void setAsCopy(GraphCleanDestId other) {
        this.secSortStamp = other.getSecSStampSecOrder();
        super.setAsCopy(other);
    }
    
    public void setSecSStampFirstOrder() {
        this.secSortStamp = 0b00 << 0;
    }

    public void setSecSStampSecOrder() {
        this.secSortStamp = 0b01 << 0;
    }

    public byte getSecSStampSecOrder() {
        return this.secSortStamp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(secSortStamp);
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.secSortStamp = in.readByte();
        super.readFields(in);
    }

    //TODO question why one comparator extends writablecompartor; the other one implements rawcomparator
    //and why need register
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(GraphCleanDestId.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int diff1 = WritableComparator.compareBytes(b1, s1, 1, b2, s2, 1);
            if (diff1 != 0) {
                return diff1;
            } else {
                int kmerlength1 = Marshal.getInt(b1, s1);
                int kmerlength2 = Marshal.getInt(b2, s2);
                if (kmerlength1 == kmerlength2) {
                    return WritableComparator.compareBytes(b1, s1 + HEADER_SIZE, KmerUtil.getByteNumFromK(kmerlength1),
                            b2, s2 + HEADER_SIZE, KmerUtil.getByteNumFromK(kmerlength2));
                }
                return kmerlength1 - kmerlength2;
            }
        }
    }

    public static class FirstComparator implements RawComparator<GraphCleanDestId> {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, s1, 1, b2, s2, 1);
        }

        @Override
        public int compare(GraphCleanDestId o1, GraphCleanDestId o2) {
            byte l = o1.getSecSStampSecOrder();
            byte r = o2.getSecSStampSecOrder();
            return l == r ? 0 : (l < r ? -1 : 1);
        }
    }

    static { // register this comparator
        WritableComparator.define(GraphCleanDestId.class, new Comparator());
    }
}
