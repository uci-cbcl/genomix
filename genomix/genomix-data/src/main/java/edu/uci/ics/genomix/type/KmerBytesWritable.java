/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Fix kmer length byteswritable
 * It was used to generate the graph in which phase the kmer length doesn't change.
 * Thus the size of bytes doesn't change either.
 */
public class KmerBytesWritable extends BinaryComparable implements Serializable, WritableComparable<BinaryComparable> {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    private static final byte[] EMPTY_BYTES = {};

    protected int size;
    protected byte[] bytes;
    protected int kmerlength;

    @Deprecated
    public KmerBytesWritable() {
        this(0, EMPTY_BYTES);
    }

    public KmerBytesWritable(int k, byte[] storage) {
        this.kmerlength = k;
        if (k > 0) {
            this.size = KmerUtil.getByteNumFromK(kmerlength);
            this.bytes = storage;
            if (this.bytes.length < size) {
                throw new ArrayIndexOutOfBoundsException("Storage is smaller than required space for kmerlength:k");
            }
        } else {
            this.bytes = storage;
            this.size = 0;
        }
    }

    /**
     * Initial Kmer space by kmerlength
     * 
     * @param k
     *            kmerlength
     */
    public KmerBytesWritable(int k) {
        this.kmerlength = k;
        this.size = KmerUtil.getByteNumFromK(kmerlength);
        if (k > 0) {
            this.bytes = new byte[this.size];
        } else {
            this.bytes = EMPTY_BYTES;
        }
    }

    public KmerBytesWritable(KmerBytesWritable right) {
        if (right != null) {
            this.kmerlength = right.kmerlength;
            this.size = right.size;
            this.bytes = new byte[right.size];
            set(right);
        }else{
            this.kmerlength = 0;
            this.size = 0;
            this.bytes = EMPTY_BYTES;
        }
    }

    public byte getGeneCodeAtPosition(int pos) {
        if (pos >= kmerlength) {
            return -1;
        }
        int posByte = pos / 4;
        int shift = (pos % 4) << 1;
        return (byte) ((bytes[size - 1 - posByte] >> shift) & 0x3);
    }

    public int getKmerLength() {
        return this.kmerlength;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public int getLength() {
        return size;
    }

    /**
     * Read Kmer from read text into bytes array e.g. AATAG will compress as
     * [0x000G, 0xATAA]
     * 
     * @param k
     * @param array
     * @param start
     */
    public void setByRead(byte[] array, int start) {
        byte l = 0;
        int bytecount = 0;
        int bcount = this.size - 1;
        for (int i = start; i < start + kmerlength && i < array.length; i++) {
            byte code = GeneCode.getCodeFromSymbol(array[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[0] = l;
        }
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
    public void setByReadReverse(byte[] array, int start) {
        byte l = 0;
        int bytecount = 0;
        int bcount = size - 1;
        for (int i = start + kmerlength - 1; i >= 0 && i < array.length; i--) {
            byte code = GeneCode.getCodeFromSymbol(array[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[0] = l;
        }
    }

    /**
     * Shift Kmer to accept new char input
     * 
     * @param c
     *            Input new gene character
     * @return the shift out gene, in gene code format
     */
    public byte shiftKmerWithNextChar(byte c) {
        return shiftKmerWithNextCode(GeneCode.getCodeFromSymbol(c));
    }

    /**
     * Shift Kmer to accept new gene code
     * 
     * @param c
     *            Input new gene code
     * @return the shift out gene, in gene code format
     */
    public byte shiftKmerWithNextCode(byte c) {
        byte output = (byte) (bytes[size - 1] & 0x03);
        for (int i = size - 1; i > 0; i--) {
            byte in = (byte) (bytes[i - 1] & 0x03);
            bytes[i] = (byte) (((bytes[i] >>> 2) & 0x3f) | (in << 6));
        }
        int pos = ((kmerlength - 1) % 4) << 1;
        byte code = (byte) (c << pos);
        bytes[0] = (byte) (((bytes[0] >>> 2) & 0x3f) | code);
        clearLeadBit();
        return output;
    }

    /**
     * Shift Kmer to accept new input char
     * 
     * @param c
     *            Input new gene character
     * @return the shiftout gene, in gene code format
     */
    public byte shiftKmerWithPreChar(byte c) {
        return shiftKmerWithPreCode(GeneCode.getCodeFromSymbol(c));
    }

    /**
     * Shift Kmer to accept new gene code
     * 
     * @param c
     *            Input new gene code
     * @return the shiftout gene, in gene code format
     */
    public byte shiftKmerWithPreCode(byte c) {
        int pos = ((kmerlength - 1) % 4) << 1;
        byte output = (byte) ((bytes[0] >> pos) & 0x03);
        for (int i = 0; i < size - 1; i++) {
            byte in = (byte) ((bytes[i + 1] >> 6) & 0x03);
            bytes[i] = (byte) ((bytes[i] << 2) | in);
        }
        bytes[size - 1] = (byte) ((bytes[size - 1] << 2) | c);
        clearLeadBit();
        return output;
    }

    protected void clearLeadBit() {
        if (kmerlength % 4 != 0) {
            bytes[0] &= (1 << ((kmerlength % 4) << 1)) - 1;
        }
    }

    public void set(KmerBytesWritable newData) {
        if (kmerlength != newData.kmerlength){
            throw new IllegalArgumentException("kmerSize is different, try to use VKmerBytesWritable instead");
        }
        if (kmerlength > 0 ){
            set(newData.bytes, 0, newData.size);
        }
    }

    public void set(byte[] newData, int offset, int length) {
        if (kmerlength > 0){
            System.arraycopy(newData, offset, bytes, 0, size);
        }
    }

    /**
     * Don't read the kmerlength from datastream,
     * Read it from configuration
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.kmerlength = in.readInt();
        this.size = KmerUtil.getByteNumFromK(kmerlength);
        if (this.kmerlength > 0) {
            if (this.bytes.length < this.size) {
                this.bytes = new byte[this.size];
            }
            in.readFully(bytes, 0, size);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(kmerlength);
        if (kmerlength > 0) {
            out.write(bytes, 0, size);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + this.kmerlength;
    }

    @Override
    public boolean equals(Object right_obj) {
        if (right_obj instanceof KmerBytesWritable)
            return this.kmerlength == ((KmerBytesWritable) right_obj).kmerlength && super.equals(right_obj);
        return false;
    }

    @Override
    public String toString() {
        return KmerUtil.recoverKmerFrom(this.kmerlength, this.getBytes(), 0, this.getLength());
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
                return compareBytes(b1, s1 + LEAD_BYTES, l1 - LEAD_BYTES, b2, s2 + LEAD_BYTES, l2 - LEAD_BYTES);
            }
            return kmerlength1 - kmerlength2;
        }
    }

    static { // register this comparator
        WritableComparator.define(KmerBytesWritable.class, new Comparator());
    }

}
