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
import edu.uci.ics.genomix.oldtype.NodeWritable.DirectionFlag;

/**
 * Variable kmer length byteswritable
 * It was used to generate the graph in which phase the kmer length doesn't change.
 * Thus the kmerByteSize of bytes doesn't change either.
 */
public class KmerBytesWritable extends BinaryComparable implements Serializable, WritableComparable<BinaryComparable> {

    private static final long serialVersionUID = 1L;
    private static final byte[] EMPTY_BYTES = { 0, 0, 0, 0 }; // int indicating 0 length
    private static final int HEADER_SIZE = 4; // number of bytes for header info

    private int lettersInKmer;
    private int bytesUsed;
    private byte[] bytes;
    private int offset;

    /**
     * Initialize as empty kmer
     */
    public KmerBytesWritable() {
        this(EMPTY_BYTES, 0);
    }

    /**
     * Copy contents of kmer string
     */
    public KmerBytesWritable(String kmer) {
        bytes = new byte[HEADER_SIZE + KmerUtil.getByteNumFromK(kmer.length())];
        setAsCopy(kmer);
    }

    /**
     * Set as reference to given data
     */
    public KmerBytesWritable(byte[] storage, int offset) {
        setAsReference(storage, offset);
    }

    /**
     * Reserve space for k letters
     */
    public KmerBytesWritable(int k) {
        if (k > 0) {
            this.bytes = new byte[HEADER_SIZE + KmerUtil.getByteNumFromK(k)];
        } else {
            this.bytes = EMPTY_BYTES;
        }
        this.offset = 0;
        setKmerLength(k);
    }

    /**
     * copy kmer in other
     * 
     * @param other
     */
    public KmerBytesWritable(KmerBytesWritable other) {
        this(other.lettersInKmer);
        setAsCopy(other);
    }

    /**
     * Deep copy of the given kmer
     * 
     * @param other
     */
    public void setAsCopy(KmerBytesWritable other) {
        reset(other.lettersInKmer);
        if (lettersInKmer > 0) {
            System.arraycopy(other.bytes, other.offset + HEADER_SIZE, bytes, this.offset + HEADER_SIZE, lettersInKmer);
        }
    }

    /**
     * set from String kmer
     */
    public void setAsCopy(String kmer) {
        int k = kmer.length();
        reset(k);
        System.arraycopy(kmer.getBytes(), 0, bytes, offset + HEADER_SIZE, k);
    }

    /**
     * Deep copy of the given bytes data
     * 
     * @param newData
     * @param offset
     */
    public void setAsCopy(byte[] newData, int offset) {
        int k = Marshal.getInt(newData, offset);
        reset(k);
        System.arraycopy(newData, offset + HEADER_SIZE, bytes, this.offset + HEADER_SIZE, k);
    }

    /**
     * Reset array by kmerlength
     * 
     * @param k
     */
    public void reset(int k) {
        setKmerLength(k);
        setSize(bytesUsed);
        clearLeadBit();
    }

    /**
     * Point this datablock to the given bytes array
     * It works like the pointer to new datablock.
     * NOTE: kmerlength is NOT updated
     * 
     * @param newData
     * @param offset
     */
    public void setAsReference(byte[] newData, int offset) {
        this.bytes = newData;
        this.offset = offset;
        int kRequested = Marshal.getInt(newData, offset);
        int bytesRequested = KmerUtil.getByteNumFromK(kRequested);
        if (newData.length - offset < bytesRequested) {
            throw new IllegalArgumentException("Requested " + bytesRequested + " bytes (k=" + kRequested
                    + ") but buffer has only " + (newData.length - offset) + " bytes");
        }
        setKmerLength(kRequested);
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
        this.bytesUsed = size;
    }

    protected int getCapacity() {
        return bytes.length;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap != getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (new_cap < bytesUsed) {
                bytesUsed = new_cap;
            }
            if (bytesUsed != 0) {
                System.arraycopy(bytes, offset, new_data, 0, bytesUsed);
            }
            bytes = new_data;
            offset = 0;
        }
    }

    /**
     * Get one genecode (A|G|C|T) from the given kmer index
     * e.g. Get the 4th gene of the kmer ACGTA will return T
     * 
     * @param pos
     * @return
     */
    public byte getGeneCodeAtPosition(int pos) {
        if (pos >= lettersInKmer) {
            throw new IllegalArgumentException("gene position out of bound");
        }
        return geneCodeAtPosition(pos);
    }

    // unchecked version of above. Used when kmerlength is inaccurate (mid-merge)
    private byte geneCodeAtPosition(int pos) {
        int posByte = pos / 4;
        int shift = (pos % 4) << 1;
        return (byte) ((bytes[offset + bytesUsed - 1 - posByte] >> shift) & 0x3);
    }

    public void setKmerLength(int k) {
        this.bytesUsed = HEADER_SIZE + KmerUtil.getByteNumFromK(k);
        this.lettersInKmer = k;
        Marshal.putInt(k, bytes, offset);
    }

    public int getKmerLength() {
        return lettersInKmer;
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
        return bytesUsed;
    }

    /**
     * Read Kmer from read text into bytes array e.g. AATAG will compress as
     * [0x000G, 0xATAA]
     * 
     * @param k
     * @param stringBytes
     *            : byte array from a _string_. Meaning there's no header
     * @param start
     */
    public void setByRead(byte[] stringBytes, int start) {
        byte l = 0;
        int bytecount = 0;
        int bcount = this.bytesUsed - 1;
        for (int i = start; i < start + lettersInKmer && i < stringBytes.length; i++) {
            byte code = GeneCode.getCodeFromSymbol(stringBytes[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[offset + HEADER_SIZE + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[offset + HEADER_SIZE] = l;
        }
    }

    public void setByRead(int k, byte[] array, int start) {
        reset(k);
        setByRead(array, start);
    }

    /**
     * Compress Reversed read into bytes array
     * e.g. AATAG will paired to CTATT, and then compress as
     * [0x000T,0xTATC]
     * 
     * @param input
     *            array
     * @param start
     *            position
     */
    public void setByReadReverse(byte[] array, int start) {
        byte l = 0;
        int bytecount = 0;
        int bcount = bytesUsed - 1;
        //        for (int i = start + kmerlength - 1; i >= 0 && i < array.length; i--) {
        for (int i = start + lettersInKmer - 1; i >= start && i < array.length; i--) {
            byte code = GeneCode.getPairedCodeFromSymbol(array[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[offset + HEADER_SIZE + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[offset + HEADER_SIZE] = l;
        }
    }

    public void setByReadReverse(int k, byte[] array, int start) {
        reset(k);
        setByReadReverse(array, start);
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
        byte output = (byte) (bytes[offset + HEADER_SIZE + bytesUsed - 1] & 0x03);
        for (int i = bytesUsed - 1; i > 0; i--) {
            byte in = (byte) (bytes[offset + HEADER_SIZE + i - 1] & 0x03);
            bytes[offset + HEADER_SIZE + i] = (byte) (((bytes[offset + HEADER_SIZE + i] >>> 2) & 0x3f) | (in << 6));
        }
        int pos = ((lettersInKmer - 1) % 4) << 1;
        byte code = (byte) (c << pos);
        bytes[offset + HEADER_SIZE] = (byte) (((bytes[offset + HEADER_SIZE] >>> 2) & 0x3f) | code);
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
        int pos = ((lettersInKmer - 1) % 4) << 1;
        byte output = (byte) ((bytes[offset + HEADER_SIZE] >> pos) & 0x03);
        for (int i = 0; i < bytesUsed - 1; i++) {
            byte in = (byte) ((bytes[offset + HEADER_SIZE + i + 1] >> 6) & 0x03);
            bytes[offset + HEADER_SIZE + i] = (byte) ((bytes[offset + HEADER_SIZE + i] << 2) | in);
        }
        bytes[offset + HEADER_SIZE + bytesUsed - 1] = (byte) ((bytes[offset + HEADER_SIZE + bytesUsed - 1] << 2) | c);
        clearLeadBit();
        return output;
    }

    /**
     * Merge Kmer with the next connected Kmer
     * e.g. AAGCTAA merge with AACAACC, if the initial kmerSize = 3
     * then it will return AAGCTAACAACC
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param kmer
     *            : the next kmer
     */
    public void mergeWithFFKmer(int initialKmerSize, KmerBytesWritable kmer) {
        int preKmerLength = lettersInKmer;
        int preSize = bytesUsed;
        lettersInKmer += kmer.lettersInKmer - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(lettersInKmer));
        for (int i = 1; i <= preSize; i++) {
            bytes[offset + HEADER_SIZE + bytesUsed - i] = bytes[offset + preSize - i];
        }
        for (int k = initialKmerSize - 1; k < kmer.getKmerLength(); k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, kmer.getBytes(), kmer.getOffset(), kmer.getLength());
            appendOneByteAtPosition(preKmerLength + k - initialKmerSize + 1, onebyte, bytes, offset + HEADER_SIZE, bytesUsed);
        }
        clearLeadBit();
    }

    /**
     * Merge Kmer with the next connected Kmer, when that Kmer needs to be reverse-complemented
     * e.g. AAGCTAA merge with GGTTGTT, if the initial kmerSize = 3
     * then it will return AAGCTAACAACC
     * A merge B => A B~
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param kmer
     *            : the next kmer
     */
    public void mergeWithFRKmer(int initialKmerSize, KmerBytesWritable kmer) {
        int preSize = bytesUsed;
        int preKmerLength = lettersInKmer;
        lettersInKmer += kmer.lettersInKmer - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(lettersInKmer));
        // copy prefix into right-side of buffer
        for (int i = 1; i <= preSize; i++) {
            bytes[offset + HEADER_SIZE + bytesUsed - i] = bytes[offset + HEADER_SIZE + preSize - i];
        }

        int bytecount = (preKmerLength % 4) * 2;
        int bcount = bytesUsed - preSize - bytecount / 8; // may overlap previous kmer
        byte l = bcount == bytesUsed - preSize ? bytes[offset + bcount] : 0x00;
        bytecount %= 8;
        for (int i = kmer.lettersInKmer - initialKmerSize; i >= 0; i--) {
            byte code = GeneCode.getPairedGeneCode(kmer.getGeneCodeAtPosition(i));
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[offset + HEADER_SIZE + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[offset + HEADER_SIZE] = l;
        }
    }

    /**
     * Merge Kmer with the previous connected Kmer, when that kmer needs to be reverse-complemented
     * e.g. AACAACC merge with TTCTGCC, if the initial kmerSize = 3
     * then it will return GGCAGAACAACC
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param preKmer
     *            : the previous kmer
     */
    public void mergeWithRFKmer(int initialKmerSize, KmerBytesWritable preKmer) {
        int preKmerLength = lettersInKmer;
        int preSize = bytesUsed;
        lettersInKmer += preKmer.lettersInKmer - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(lettersInKmer));
        //        byte cacheByte = getOneByteFromKmerAtPosition(0, bytes, offset, preSize);

        int byteIndex = bytesUsed - 1;
        byte cacheByte = 0x00;
        int posnInByte = 0;

        // copy rc of preKmer into high bytes
        for (int i = preKmer.lettersInKmer - 1; i >= initialKmerSize - 1; i--) {
            byte code = GeneCode.getPairedGeneCode(preKmer.getGeneCodeAtPosition(i));
            cacheByte |= (byte) (code << posnInByte);
            posnInByte += 2;
            if (posnInByte == 8) {
                bytes[byteIndex--] = cacheByte;
                cacheByte = 0;
                posnInByte = 0;
            }
        }

        // copy my kmer into low positions of bytes
        for (int i = 0; i < preKmerLength; i++) {
            // expanding the capacity makes this offset incorrect.  It's off by the # of additional bytes added.
            int newposn = i + (bytesUsed - preSize) * 4;
            byte code = geneCodeAtPosition(newposn);
            cacheByte |= (byte) (code << posnInByte);
            posnInByte += 2;
            if (posnInByte == 8) {
                bytes[byteIndex--] = cacheByte;
                cacheByte = 0;
                posnInByte = 0;
            }
        }
        bytes[offset + HEADER_SIZE] = cacheByte;
        clearLeadBit();
    }

    /**
     * Merge Kmer with the previous connected Kmer
     * e.g. AACAACC merge with AAGCTAA, if the initial kmerSize = 3
     * then it will return AAGCTAACAACC
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param preKmer
     *            : the previous kmer
     */
    public void mergeWithRRKmer(int initialKmerSize, KmerBytesWritable preKmer) {
        int preKmerLength = lettersInKmer;
        int preSize = bytesUsed;
        lettersInKmer += preKmer.lettersInKmer - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(lettersInKmer));
        byte cacheByte = getOneByteFromKmerAtPosition(0, bytes, offset, preSize);

        // copy prekmer
        for (int k = 0; k < preKmer.lettersInKmer - initialKmerSize + 1; k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, preKmer.bytes, preKmer.offset, preKmer.bytesUsed);
            appendOneByteAtPosition(k, onebyte, bytes, offset + HEADER_SIZE, bytesUsed);
        }

        // copy current kmer
        int k = 4;
        for (; k < preKmerLength; k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, bytes, offset, preSize);
            appendOneByteAtPosition(preKmer.lettersInKmer - initialKmerSize + k - 4 + 1, cacheByte, bytes, offset + HEADER_SIZE,
                    bytesUsed);
            cacheByte = onebyte;
        }
        appendOneByteAtPosition(preKmer.lettersInKmer - initialKmerSize + k - 4 + 1, cacheByte, bytes, offset + HEADER_SIZE, bytesUsed);
        clearLeadBit();
    }

    public void mergeWithKmerInDir(byte dir, int initialKmerSize, KmerBytesWritable kmer) {
        switch (dir & DirectionFlag.DIR_MASK) {
            case DirectionFlag.DIR_FF:
                mergeWithFFKmer(initialKmerSize, kmer);
                break;
            case DirectionFlag.DIR_FR:
                mergeWithFRKmer(initialKmerSize, kmer);
                break;
            case DirectionFlag.DIR_RF:
                mergeWithRFKmer(initialKmerSize, kmer);
                break;
            case DirectionFlag.DIR_RR:
                mergeWithRRKmer(initialKmerSize, kmer);
                break;
            default:
                throw new RuntimeException("Direction not recognized: " + dir);
        }
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
            bytes[offset + HEADER_SIZE] &= (1 << ((lettersInKmer % 4) << 1)) - 1;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lettersInKmer = in.readInt();
        bytesUsed = KmerUtil.getByteNumFromK(lettersInKmer);
        if (lettersInKmer > 0) {
            if (this.bytes.length < this.bytesUsed) {
                this.bytes = new byte[this.bytesUsed];
                this.offset = 0;
                
            }
            in.readFully(bytes, offset + HEADER_SIZE, bytesUsed - HEADER_SIZE);
        }
        Marshal.putInt(lettersInKmer, bytes, offset);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(lettersInKmer);
        if (lettersInKmer > 0) {
            out.write(bytes, offset + HEADER_SIZE, bytesUsed - HEADER_SIZE);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + this.lettersInKmer;
    }

    @Override
    public boolean equals(Object right_obj) {
        if (right_obj instanceof KmerBytesWritable)
            return this.lettersInKmer == ((KmerBytesWritable) right_obj).lettersInKmer && super.equals(right_obj);
        return false;
    }

    @Override
    public String toString() {
        return KmerUtil.recoverKmerFrom(this.lettersInKmer, this.getBytes(), offset + HEADER_SIZE, this.getLength());
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(KmerBytesWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int kmerlength1 = Marshal.getInt(b1, s1);
            int kmerlength2 = Marshal.getInt(b2, s2);
            if (kmerlength1 == kmerlength2) {
                return compareBytes(b1, s1 + HEADER_SIZE, l1 - HEADER_SIZE, b2, s2 + HEADER_SIZE, l2 - HEADER_SIZE);
            }
            return kmerlength1 - kmerlength2;
        }
    }

    static { // register this comparator
        WritableComparator.define(KmerBytesWritable.class, new Comparator());
    }

}
