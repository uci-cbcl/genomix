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

import edu.uci.ics.genomix.data.KmerUtil;

/**
 * Variable kmer length byteswritable
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
    protected int offset;
    protected int kmerlength;

    public KmerBytesWritable() {
        this(0, EMPTY_BYTES, 0);
    }

    public KmerBytesWritable(int k, byte[] storage, int offset) {
        setNewReference(k, storage, offset);
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
        this.offset = 0;
    }

    public KmerBytesWritable(KmerBytesWritable right) {
        this(right.kmerlength);
        set(right);
    }

    public void set(KmerBytesWritable newData) {
        if (newData == null) {
            this.set(0, EMPTY_BYTES, 0);
        } else {
            this.set(newData.kmerlength, newData.bytes, newData.getOffset());
        }
    }

    public void set(byte[] newData, int offset) {
        if (kmerlength > 0) {
            System.arraycopy(newData, offset, bytes, this.offset, size);
        }
    }

    public void set(int k, byte[] newData, int offset) {
        reset(k);
        if (k > 0) {
            System.arraycopy(newData, offset, bytes, this.offset, size);
        }
    }

    /**
     * Reset array by kmerlength
     * 
     * @param k
     */
    public void reset(int k) {
        this.kmerlength = k;
        setSize(KmerUtil.getByteNumFromK(k));
        clearLeadBit();
    }

    public void setNewReference(byte[] newData, int offset) {
        this.bytes = newData;
        this.offset = offset;
        if (newData.length - offset < size) {
            throw new IllegalArgumentException("Not given enough space");
        }
    }

    public void setNewReference(int k, byte[] newData, int offset) {
        this.kmerlength = k;
        this.size = KmerUtil.getByteNumFromK(k);
        setNewReference(newData, offset);
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
        this.size = size;
    }

    protected int getCapacity() {
        return bytes.length;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap != getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (new_cap < size) {
                size = new_cap;
            }
            if (size != 0) {
                System.arraycopy(bytes, offset, new_data, 0, size);
            }
            bytes = new_data;
            offset = 0;
        }
    }

    public byte getGeneCodeAtPosition(int pos) {
        if (pos >= kmerlength) {
            throw new IllegalArgumentException("gene position out of bound");
        }
        int posByte = pos / 4;
        int shift = (pos % 4) << 1;
        return (byte) ((bytes[offset + size - 1 - posByte] >> shift) & 0x3);
    }

    public int getKmerLength() {
        return this.kmerlength;
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
                bytes[offset + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[offset] = l;
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
        int bcount = size - 1;
        for (int i = start + kmerlength - 1; i >= 0 && i < array.length; i--) {
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
        byte output = (byte) (bytes[offset + size - 1] & 0x03);
        for (int i = size - 1; i > 0; i--) {
            byte in = (byte) (bytes[offset + i - 1] & 0x03);
            bytes[offset + i] = (byte) (((bytes[offset + i] >>> 2) & 0x3f) | (in << 6));
        }
        int pos = ((kmerlength - 1) % 4) << 1;
        byte code = (byte) (c << pos);
        bytes[offset] = (byte) (((bytes[offset] >>> 2) & 0x3f) | code);
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
        byte output = (byte) ((bytes[offset] >> pos) & 0x03);
        for (int i = 0; i < size - 1; i++) {
            byte in = (byte) ((bytes[offset + i + 1] >> 6) & 0x03);
            bytes[offset + i] = (byte) ((bytes[offset + i] << 2) | in);
        }
        bytes[offset + size - 1] = (byte) ((bytes[offset + size - 1] << 2) | c);
        clearLeadBit();
        return output;
    }

    /**
     * Merge kmer with next neighbor in gene-code format.
     * The k of new kmer will increase by 1
     * e.g. AAGCT merge with A => AAGCTA
     * 
     * @param nextCode
     *            : next neighbor in gene-code format
     */
    public void mergeNextCode(byte nextCode) {
        this.kmerlength += 1;
        setSize(KmerUtil.getByteNumFromK(kmerlength));
        if (kmerlength % 4 == 1) {
            for (int i = getLength() - 1; i > 0; i--) {
                bytes[offset + i] = bytes[offset + i - 1];
            }
            bytes[offset] = (byte) (nextCode & 0x3);
        } else {
            bytes[offset] = (byte) (bytes[offset] | ((nextCode & 0x3) << (((kmerlength - 1) % 4) << 1)));
        }
        clearLeadBit();
    }

    /**
     * Merge Kmer with the next connected Kmer
     * e.g. AAGCTAA merge with AACAACC, if the initial kmerSize = 3
     * then it will return AAGCTAACAACC
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param kmer
     */
    public void mergeNextKmer(int initialKmerSize, KmerBytesWritable kmer) {
        int preKmerLength = kmerlength;
        int preSize = size;
        this.kmerlength += kmer.kmerlength - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(kmerlength));
        for (int i = 1; i <= preSize; i++) {
            bytes[offset + size - i] = bytes[offset + preSize - i];
        }
        for (int k = initialKmerSize - 1; k < kmer.getKmerLength(); k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, kmer.getBytes(), kmer.getOffset(), kmer.getLength());
            appendOneByteAtPosition(preKmerLength + k - initialKmerSize + 1, onebyte, bytes, offset, size);
        }
        clearLeadBit();
    }

    public void mergePreKmer(int initialKmerSize, KmerBytesWritable preKmer) {
        int preKmerLength = kmerlength;
        int preSize = size;
        this.kmerlength += preKmer.kmerlength - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(kmerlength));
        byte cacheByte = getOneByteFromKmerAtPosition(0, bytes, offset, preSize);

        // copy prekmer
        for (int k = 0; k < preKmer.kmerlength - initialKmerSize + 1; k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, preKmer.bytes, preKmer.offset, preKmer.size);
            appendOneByteAtPosition(k, onebyte, bytes, offset, size);
        }

        // copy current kmer
        int k = 4;
        for (; k < preKmerLength; k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, bytes, offset, preSize);
            appendOneByteAtPosition(preKmer.kmerlength - initialKmerSize + k - 4 + 1, cacheByte, bytes, offset, size);
            cacheByte = onebyte;
        }
        appendOneByteAtPosition(preKmer.kmerlength - initialKmerSize + k - 4 + 1, cacheByte, bytes, offset, size);
        clearLeadBit();
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
            buffer[position - 1] = (byte) ((buffer[position - 1] & (0xff - mask)) | ((byte) ((0xff & onebyte) >> (8 - shift))));
        }
    }

    public static byte getOneByteFromKmerAtPosition(int k, byte[] buffer, int start, int length) {
        int position = start + length - 1 - k / 4;
        if (position < start) {
            throw new IllegalArgumentException("Buffer for kmer storage is invalid");
        }
        int shift = (k % 4) << 1;
        byte data = (byte) (((0xff) & buffer[position]) >> shift);
        if (shift != 0 && position > start) {
            data |= 0xff & (buffer[position - 1] << (8 - shift));
        }
        return data;
    }

    protected void clearLeadBit() {
        if (kmerlength % 4 != 0) {
            bytes[offset] &= (1 << ((kmerlength % 4) << 1)) - 1;
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
                this.offset = 0;
            }
            in.readFully(bytes, offset, size);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(kmerlength);
        if (kmerlength > 0) {
            out.write(bytes, offset, size);
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
        return KmerUtil.recoverKmerFrom(this.kmerlength, this.getBytes(), offset, this.getLength());
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
