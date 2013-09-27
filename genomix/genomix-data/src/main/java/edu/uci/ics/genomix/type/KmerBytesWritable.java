/*
* Copyright 2009-2013 by The Regents of the University of California
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

import edu.uci.ics.genomix.data.KmerUtil;
import edu.uci.ics.genomix.data.Marshal;

/**
 * Fixed, static-length Kmer used as the key and edge values of each
 * NodeWritable. Kmer length should be set once during configuration and should
 * never change.
 */
public class KmerBytesWritable extends BinaryComparable implements Serializable, WritableComparable<BinaryComparable> { // TODO make consistent with NodeWritable

    private static final long serialVersionUID = 1L;
    protected static final byte[] EMPTY_BYTES = {};

    protected static int lettersInKmer;
    private static int bytesUsed;
    protected byte[] bytes;
    protected int offset;

    /**
     * set the *GLOBAL* kmer length to the given k value.
     * NOTE: this will invalidate ALL previously created kmers. This function
     * should be called before any kmers are created
     */
    public static void setGlobalKmerLength(int k) {
        setBytesUsed(KmerUtil.getByteNumFromK(k));
        lettersInKmer = k;
    }

    /**
     * Initialize as empty kmer
     */
    public KmerBytesWritable() {
        bytes = new byte[getBytesUsed()];
        offset = 0;
    }

    /**
     * Copy contents of kmer string
     */
    public KmerBytesWritable(String kmer) {
        this();
        setFromStringBytes(kmer.getBytes(), 0);
    }

    /**
     * Set as reference to existing data
     */
    public KmerBytesWritable(byte[] newStorage, int newOffset) {
        setAsReference(newStorage, newOffset);
    }

    /**
     * copy kmer in other
     * 
     * @param other
     */
    public KmerBytesWritable(KmerBytesWritable other) {
        this();
        setAsCopy(other);
    }
    
    /**
     * copy kmer in other
     * 
     * @param other
     */
    public KmerBytesWritable(VKmerBytesWritable other) {
        this();
        setAsCopy(other);
    }

    /**
     * Deep copy of the given kmer
     * 
     * @param other
     */
    public void setAsCopy(KmerBytesWritable other) {
        if (lettersInKmer > 0) {
            System.arraycopy(other.bytes, other.offset, bytes, offset, getBytesUsed());
        }
    }
    
    /**
     * Deep copy of the given kmer
     * 
     * @param other
     */
    public void setAsCopy(VKmerBytesWritable other) {
        if (other.lettersInKmer != lettersInKmer) {
            throw new IllegalArgumentException("Provided VKmer (" + other + ") is of an incompatible length (was " + other.getKmerLetterLength() + ", should be " + lettersInKmer + ")!");
        }
        if (lettersInKmer > 0) {
            System.arraycopy(other.bytes, other.kmerStartOffset, bytes, offset, getBytesUsed());
        }
    }


    /**
     * Deep copy of the given bytes data
     * 
     * @param newData
     * @param newOffset
     */
    public void setAsCopy(byte[] newData, int newOffset) {
        if (newData.length - newOffset < getBytesUsed()) {
            throw new IllegalArgumentException("Requested " + getBytesUsed() + " bytes (k=" + lettersInKmer
                    + ") but buffer has only " + (newData.length - newOffset) + " bytes");
        }
        System.arraycopy(newData, newOffset, bytes, offset, getBytesUsed());
    }

    /**
     * Point this datablock to the given bytes array It works like the pointer
     * to new datablock.
     * 
     * @param newData
     * @param newOffset
     */
    public void setAsReference(byte[] newData, int newOffset) {
        if (newData.length - newOffset < getBytesUsed()) {
            throw new IllegalArgumentException("Requested " + getBytesUsed() + " bytes (k=" + lettersInKmer
                    + ") but buffer has only " + (newData.length - newOffset) + " bytes");
        }
        bytes = newData;
        offset = newOffset;
    }
    
    /**
     * Point this datablock to the given kmer's byte array It works like the pointer
     * to new datablock.
     * 
     * @param newData
     * @param offset
     */
    public void setAsReference(VKmerBytesWritable other) {
        if (other.lettersInKmer != lettersInKmer) {
            throw new IllegalArgumentException("Provided VKmer (" + other + ") is of an incompatible length (was " + other.getKmerLetterLength() + ", should be " + lettersInKmer + ")!");
        }
        bytes = other.bytes;
        offset = other.kmerStartOffset;
    }

    /**
     * Get one genecode (A|G|C|T) from the given kmer index e.g. Get the 4th
     * gene of the kmer ACGTA will return T
     * 
     * @param pos
     * @return
     */
    public byte getGeneCodeAtPosition(int pos) {
        if (pos >= lettersInKmer || pos < 0) {
            throw new ArrayIndexOutOfBoundsException("Gene position (" + pos + ") out of bounds for k=" + lettersInKmer);
        }
        return geneCodeAtPosition(pos);
    }

    /**
     * unchecked version of getGeneCodeAtPosition. Used when kmerlength is
     * inaccurate (mid-merge)
     */
    private byte geneCodeAtPosition(int pos) {
        int posByte = pos / 4;
        int shift = (pos % 4) << 1;
        return (byte) ((bytes[offset + getBytesUsed() - 1 - posByte] >> shift) & 0x3);
    }

    public static int getKmerLength() {
        return lettersInKmer;
    }
    
    public static int getBytesPerKmer() {
        return getBytesUsed();
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    public int getOffset() {
        return offset;
    }

    @Override
    public int getLength() {
        return getBytesUsed();
    }

    /**
     * Read Kmer from read text into bytes array e.g. AATAG will compress as
     * [0x000G, 0xATAA]
     * 
     * @param stringBytes
     * @param start
     */
    @SuppressWarnings("static-access")
    public void setFromStringBytes(byte[] stringBytes, int start) {
        byte l = 0;
        int bytecount = 0;
        int bcount = this.getBytesUsed() - 1;
        for (int i = start; i < start + lettersInKmer && i < stringBytes.length; i++) {
            byte code = GeneCode.getCodeFromSymbol(stringBytes[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[offset + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[offset] = l;
        }
    }

    /**
     * Compress Reversed read into bytes array e.g. AATAG will paired to CTATT,
     * and then compress as [0x000T,0xTATC]
     * 
     * @param input
     *            array
     * @param start
     *            position
     */
    public void setReversedFromStringBytes(byte[] array, int start) {
        byte l = 0;
        int bytecount = 0;
        int bcount = getBytesUsed() - 1;
        // for (int i = start + kmerlength - 1; i >= 0 && i < array.length; i--)
        // {
        for (int i = start + lettersInKmer - 1; i >= start && i < array.length; i--) {
            byte code = GeneCode.getPairedCodeFromSymbol(array[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[offset + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[offset] = l;
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
        byte output = (byte) (bytes[offset + getBytesUsed() - 1] & 0x03);
        for (int i = getBytesUsed() - 1; i > 0; i--) {
            byte in = (byte) (bytes[offset + i - 1] & 0x03);
            bytes[offset + i] = (byte) (((bytes[offset + i] >>> 2) & 0x3f) | (in << 6));
        }
        int pos = ((lettersInKmer - 1) % 4) << 1;
        byte code = (byte) (c << pos);
        bytes[offset] = (byte) (((bytes[offset] >>> 2) & 0x3f) | code);
        clearLeadBit();
        return output;
    }

    public static void appendOneByteAtPosition(int k, byte onebyte, byte[] buffer, int start, int length) {
        int position = start + length - 1 - k / 4;
        if (position < start) {
            throw new IllegalArgumentException("Buffer for kmer storage is invalid");
        }
        int shift = ((k) % 4) << 1;
        int mask = shift == 0 ? 0 : ((1 << shift) - 1);

        buffer[position] = (byte) ((buffer[position] & mask) | ((0xff & onebyte) << shift));
        if (position > start && shift != 0) {
            buffer[position - 1] = (byte) ((buffer[position - 1] & (0xff - mask)) | ((byte) ((0xff & onebyte) >>> (8 - shift))));
        }
    }

    public static byte getOneByteFromKmerAtPosition(int k, byte[] buffer, int start, int length) {
        int position = start + length - 1 - k / 4;
        if (position < start) {
            throw new IllegalArgumentException("Buffer of kmer storage is invalid");
        }
        int shift = (k % 4) << 1;
        byte data = (byte) (((0xff) & buffer[position]) >>> shift);
        if (shift != 0 && position > start) {
            data |= 0xff & (buffer[position - 1] << (8 - shift));
        }
        return data;
    }

    protected void clearLeadBit() {
        if (lettersInKmer % 4 != 0) {
            bytes[offset] &= (1 << ((lettersInKmer % 4) << 1)) - 1;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        in.readFully(bytes, offset, getBytesUsed());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(bytes, offset, getBytesUsed());
    }

    @Override
    public int hashCode() {
        return Marshal.hashBytes(bytes, offset, getBytesUsed());
    }

    @Override
    public boolean equals(Object right_obj) {
        if (right_obj instanceof KmerBytesWritable) {
            // since these may be backed by storage of different sizes, we have to manually check each byte
            KmerBytesWritable right = (KmerBytesWritable) right_obj;
            for (int i=0; i < getBytesUsed(); i++) {
                if (bytes[offset + i] != right.bytes[right.offset + i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return KmerUtil.recoverKmerFrom(lettersInKmer, bytes, offset, getBytesUsed());
    }

    public static int getBytesUsed() {
        return bytesUsed;
    }

    public static void setBytesUsed(int bytesUsed) {
        KmerBytesWritable.bytesUsed = bytesUsed;
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(KmerBytesWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        WritableComparator.define(KmerBytesWritable.class, new Comparator());
    }

}
