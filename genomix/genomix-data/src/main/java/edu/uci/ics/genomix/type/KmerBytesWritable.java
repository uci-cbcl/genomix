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
import edu.uci.ics.genomix.oldtype.NodeWritable.DirectionFlag;

/**
 * Variable kmer length byteswritable
 * It was used to generate the graph in which phase the kmer length doesn't change.
 * Thus the kmerByteSize of bytes doesn't change either.
 */
public class KmerBytesWritable extends BinaryComparable implements Serializable, WritableComparable<BinaryComparable> {
    /**
	 * 
	 */
    private static final long serialVersionUID = 1L;
    private static final byte[] EMPTY_BYTES = {};

    public int kmerByteSize;
    protected byte[] bytes;
    protected int offset;
    protected int kmerlength;

    public KmerBytesWritable() {
        this(0, EMPTY_BYTES, 0);
    }

    public KmerBytesWritable(int k, byte[] storage, int offset) {
        setNewReference(k, storage, offset);
    }

    public KmerBytesWritable(int k, String kmer) {
        setNewReference(kmer.length(), kmer.getBytes(), 0);
    }

    /**
     * Initial Kmer space by kmerlength
     * 
     * @param k
     *            kmerlength
     */
    public KmerBytesWritable(int k) {
        this.kmerlength = k;
        this.kmerByteSize = KmerUtil.getByteNumFromK(kmerlength);
        if (k > 0) {
            this.bytes = new byte[this.kmerByteSize];
        } else {
            this.bytes = EMPTY_BYTES;
        }
        this.offset = 0;
    }

    public KmerBytesWritable(KmerBytesWritable right) {
        this(right.kmerlength);
        set(right);
    }

    /**
     * Deep copy of the given kmer
     * 
     * @param newData
     */
    public void set(KmerBytesWritable newData) {
        if (newData == null) {
            this.set(0, EMPTY_BYTES, 0);
        } else {
            this.set(newData.kmerlength, newData.bytes, newData.getOffset());
        }
    }

    /**
     * Deep copy of the given bytes data
     * It will not change the kmerlength
     * 
     * @param newData
     * @param offset
     */
    public void set(byte[] newData, int offset) {
        if (kmerlength > 0) {
            System.arraycopy(newData, offset, bytes, this.offset, kmerByteSize);
        }
    }

    /**
     * Deep copy of the given data, and also set to new kmerlength
     * 
     * @param k
     *            : new kmer length
     * @param newData
     *            : data storage
     * @param offset
     *            : start offset
     */
    public void set(int k, byte[] newData, int offset) {
        reset(k);
        if (k > 0) {
            System.arraycopy(newData, offset, bytes, this.offset, kmerByteSize);
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

    /**
     * Point this datablock to the given bytes array
     * It works like the pointer to new datablock.
     * kmerlength will not change
     * 
     * @param newData
     * @param offset
     */
    public void setNewReference(byte[] newData, int offset) {
        this.bytes = newData;
        this.offset = offset;
        if (newData.length - offset < kmerByteSize) {
            throw new IllegalArgumentException("Not given enough space");
        }
    }

    /**
     * Point this datablock to the given bytes array
     * It works like the pointer to new datablock.
     * It also set the new kmerlength
     * 
     * @param k
     * @param newData
     * @param offset
     */
    public void setNewReference(int k, byte[] newData, int offset) {
        this.kmerlength = k;
        this.kmerByteSize = KmerUtil.getByteNumFromK(k);
        setNewReference(newData, offset);
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
        this.kmerByteSize = size;
    }

    protected int getCapacity() {
        return bytes.length;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap != getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (new_cap < kmerByteSize) {
                kmerByteSize = new_cap;
            }
            if (kmerByteSize != 0) {
                System.arraycopy(bytes, offset, new_data, 0, kmerByteSize);
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
        if (pos >= kmerlength) {
            throw new IllegalArgumentException("gene position out of bound");
        }
        return geneCodeAtPosition(pos);
    }
    
    // unchecked version of above. Used when kmerlength is inaccurate (mid-merge)
    private byte geneCodeAtPosition(int pos) {
        int posByte = pos / 4;
        int shift = (pos % 4) << 1;
        return (byte) ((bytes[offset + kmerByteSize - 1 - posByte] >> shift) & 0x3);
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
        return kmerByteSize;
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
        int bcount = this.kmerByteSize - 1;
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
        int bcount = kmerByteSize - 1;
//        for (int i = start + kmerlength - 1; i >= 0 && i < array.length; i--) {
        for (int i = start + kmerlength - 1; i >= start && i < array.length; i--) {
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
        byte output = (byte) (bytes[offset + kmerByteSize - 1] & 0x03);
        for (int i = kmerByteSize - 1; i > 0; i--) {
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
        for (int i = 0; i < kmerByteSize - 1; i++) {
            byte in = (byte) ((bytes[offset + i + 1] >> 6) & 0x03);
            bytes[offset + i] = (byte) ((bytes[offset + i] << 2) | in);
        }
        bytes[offset + kmerByteSize - 1] = (byte) ((bytes[offset + kmerByteSize - 1] << 2) | c);
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
        int preKmerLength = kmerlength;
        int preSize = kmerByteSize;
        this.kmerlength += kmer.kmerlength - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(kmerlength));
        for (int i = 1; i <= preSize; i++) {
            bytes[offset + kmerByteSize - i] = bytes[offset + preSize - i];
        }
        for (int k = initialKmerSize - 1; k < kmer.getKmerLength(); k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, kmer.getBytes(), kmer.getOffset(), kmer.getLength());
            appendOneByteAtPosition(preKmerLength + k - initialKmerSize + 1, onebyte, bytes, offset, kmerByteSize);
        }
        clearLeadBit();
    }

    /**
     * Merge Kmer with the next connected Kmer, when that Kmer needs to be reverse-complemented
     * e.g. AAGCTAA merge with GGTTGTT, if the initial kmerSize = 3
     * then it will return AAGCTAACAACC
     * 
     * A merge B =>  A B~
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param kmer
     *            : the next kmer
     */
    public void mergeWithFRKmer(int initialKmerSize, KmerBytesWritable kmer) {
        int preSize = kmerByteSize;
        int preKmerLength = kmerlength;
        this.kmerlength += kmer.kmerlength - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(kmerlength));
        // copy prefix into right-side of buffer
        for (int i = 1; i <= preSize; i++) {
            bytes[offset + kmerByteSize - i] = bytes[offset + preSize - i];
        }

        int bytecount = (preKmerLength % 4) * 2;
        int bcount = kmerByteSize - preSize - bytecount / 8; // may overlap previous kmer
        byte l = bcount == kmerByteSize - preSize ? bytes[offset + bcount] : 0x00;
        bytecount %= 8;
        for (int i = kmer.kmerlength - initialKmerSize; i >= 0; i--) {
            byte code = GeneCode.getPairedGeneCode(kmer.getGeneCodeAtPosition(i));
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
        int preKmerLength = kmerlength;
        int preSize = kmerByteSize;
        this.kmerlength += preKmer.kmerlength - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(kmerlength));
        //        byte cacheByte = getOneByteFromKmerAtPosition(0, bytes, offset, preSize);

        int byteIndex = kmerByteSize - 1;
        byte cacheByte = 0x00;
        int posnInByte = 0;

        // copy rc of preKmer into high bytes
        for (int i = preKmer.kmerlength - 1; i >= initialKmerSize - 1; i--) {
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
            int newposn = i + (kmerByteSize - preSize) * 4;
            byte code = geneCodeAtPosition(newposn);
            cacheByte |= (byte) (code << posnInByte);
            posnInByte += 2;
            if (posnInByte == 8) {
                bytes[byteIndex--] = cacheByte;
                cacheByte = 0;
                posnInByte = 0;
            }
        }
        bytes[offset] = cacheByte;
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
        int preKmerLength = kmerlength;
        int preSize = kmerByteSize;
        this.kmerlength += preKmer.kmerlength - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(kmerlength));
        byte cacheByte = getOneByteFromKmerAtPosition(0, bytes, offset, preSize);

        // copy prekmer
        for (int k = 0; k < preKmer.kmerlength - initialKmerSize + 1; k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, preKmer.bytes, preKmer.offset, preKmer.kmerByteSize);
            appendOneByteAtPosition(k, onebyte, bytes, offset, kmerByteSize);
        }

        // copy current kmer
        int k = 4;
        for (; k < preKmerLength; k += 4) {
            byte onebyte = getOneByteFromKmerAtPosition(k, bytes, offset, preSize);
            appendOneByteAtPosition(preKmer.kmerlength - initialKmerSize + k - 4 + 1, cacheByte, bytes, offset, kmerByteSize);
            cacheByte = onebyte;
        }
        appendOneByteAtPosition(preKmer.kmerlength - initialKmerSize + k - 4 + 1, cacheByte, bytes, offset, kmerByteSize);
        clearLeadBit();
    }
    
    public void mergeWithKmerInDir(byte dir, int initialKmerSize, KmerBytesWritable kmer) {
        switch(dir & DirectionFlag.DIR_MASK) {
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
                throw new RuntimeException("Direciotn not recognized: " + dir);
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
        if (kmerlength % 4 != 0) {
            bytes[offset] &= (1 << ((kmerlength % 4) << 1)) - 1;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.kmerlength = in.readInt();
        this.kmerByteSize = KmerUtil.getByteNumFromK(kmerlength);
        if (this.kmerlength > 0) {
            if (this.bytes.length < this.kmerByteSize) {
                this.bytes = new byte[this.kmerByteSize];
                this.offset = 0;
            }
            in.readFully(bytes, offset, kmerByteSize);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(kmerlength);
        if (kmerlength > 0) {
            out.write(bytes, offset, kmerByteSize);
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