package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparator;

public class VKmerBytesWritable extends KmerBytesWritable {
    public static final int DEFAULT_KMER_LENGTH = 21;
    
    public VKmerBytesWritable(){
        this(DEFAULT_KMER_LENGTH);
    }
    
    public VKmerBytesWritable(int k) {
        super(k);
    }

    public VKmerBytesWritable(KmerBytesWritable other) {
        super(other);
    }

    public void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
        this.size = size;
    }

    public int getCapacity() {
        return bytes.length;
    }

    public void setCapacity(int new_cap) {
        if (new_cap != getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (new_cap < size) {
                size = new_cap;
            }
            if (size != 0) {
                System.arraycopy(bytes, 0, new_data, 0, size);
            }
            bytes = new_data;
        }
    }

    /**
     * Read Kmer from read text into bytes array e.g. AATAG will compress as
     * [0x000G, 0xATAA]
     * 
     * @param k
     * @param array
     * @param start
     */
    public void setByRead(int k, byte[] array, int start) {
        reset(k);
        super.setByRead(array, start);
    }

    /**
     * Compress Reversed Kmer into bytes array AATAG will compress as
     * [0x000A,0xATAG]
     * 
     * @param input
     *            array
     * @param start
     *            position
     */
    public void setByReadReverse(int k, byte[] array, int start) {
        reset(k);
        super.setByReadReverse(array, start);
    }

    public void set(KmerBytesWritable newData) {
        set(newData.kmerlength, newData.bytes, 0, newData.size);
    }

    public void set(int k, byte[] newData, int offset, int length) {
        reset(k);
        System.arraycopy(newData, offset, bytes, 0, size);
    }

    /**
     * Reset array by kmerlength
     * 
     * @param k
     */
    public void reset(int k) {
        this.kmerlength = k;
        setSize(0);
        setSize(KmerUtil.getByteNumFromK(k));
    }

    public static class Comparator extends WritableComparator {
        public final int LEAD_BYTES = 4;

        public Comparator() {
            super(KmerBytesWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int kmerlength1 = readInt(b1, s1);
            int kmerlength2 = readInt(b2, s2);
            if (kmerlength1 == kmerlength2) {
                compareBytes(b1, s1 + LEAD_BYTES, l1 - LEAD_BYTES, b2, s2 + LEAD_BYTES, l2 - LEAD_BYTES);
            }
            return kmerlength1 - kmerlength2;
        }
    }

    static { // register this comparator
        WritableComparator.define(KmerBytesWritable.class, new Comparator());
    }
}
