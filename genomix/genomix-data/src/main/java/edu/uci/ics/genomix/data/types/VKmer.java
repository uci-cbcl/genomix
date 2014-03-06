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

package edu.uci.ics.genomix.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.genomix.data.utils.GeneCode;
import edu.uci.ics.genomix.data.utils.KmerUtil;
import edu.uci.ics.genomix.data.utils.Marshal;

/**
 * Variable-length kmer which stores its length internally.
 * Note: `offset` as used in this class is the offset at which the *kmer*
 * begins. There is a {@value HEADER_SIZE}-byte header preceding the kmer
 */
public class VKmer extends BinaryComparable implements Serializable, WritableComparable<BinaryComparable> {
    private static final long serialVersionUID = 1L;
    protected static final byte[] EMPTY_BYTES = { 0, 0, 0, 0 }; // int indicating 0 length
    protected static final int HEADER_SIZE = 4; // number of bytes for header info

    protected int lettersInKmer;
    protected int bytesUsed;
    protected byte[] bytes;
    protected int kmerStartOffset;
    protected int storageMaxSize; // since we may be a reference inside a larger datablock, we must track our maximum size

    /**
     * Initialize as empty kmer
     */
    public VKmer() {
        this(EMPTY_BYTES, 0);
    }

    /**
     * Copy contents of kmer string
     */
    public VKmer(String kmer) {
        bytes = new byte[HEADER_SIZE + KmerUtil.getByteNumFromK(kmer.length())];
        kmerStartOffset = HEADER_SIZE;
        storageMaxSize = bytes.length;
        setAsCopy(kmer);
    }

    /**
     * Set as reference to given data
     * 
     * @param storage
     *            : byte array with header
     * @param offset
     */
    public VKmer(byte[] storage, int offset) {
        setAsReference(storage, offset);
    }

    /**
     * Reserve space for k letters
     */
    public VKmer(int k) {
        if (k > 0) {
            bytes = new byte[HEADER_SIZE + KmerUtil.getByteNumFromK(k)];
        } else if (k == 0) {
            bytes = EMPTY_BYTES;
        } else {
            throw new IllegalArgumentException("Invalid K (" + k + ").");
        }
        kmerStartOffset = HEADER_SIZE;
        storageMaxSize = bytes.length;
        setKmerLength(k);
    }

    /**
     * deep copy of kmer in other
     * 
     * @param other
     */
    public VKmer(VKmer other) {
        this(other.lettersInKmer);
        setAsCopy(other);
    }

    /**
     * deep copy of kmer in other
     * 
     * @param other
     */
    public VKmer(Kmer other) {
        this(Kmer.lettersInKmer);
        setAsCopy(other);
    }

    /**
     * Deep copy of the given kmer
     * 
     * @param other
     */
    public void setAsCopy(VKmer other) {
        reset(other.lettersInKmer);
        if (lettersInKmer > 0) {
            System.arraycopy(other.bytes, other.kmerStartOffset, bytes, this.kmerStartOffset, bytesUsed);
        }
    }

    /**
     * Deep copy of the given kmer
     * 
     * @param other
     */
    @SuppressWarnings("static-access")
    public void setAsCopy(Kmer other) {
        reset(other.lettersInKmer);
        if (lettersInKmer > 0) {
            System.arraycopy(other.bytes, other.offset, bytes, this.kmerStartOffset, bytesUsed);
        }
    }

    /**
     * set from String kmer
     */
    public void setAsCopy(String kmer) {
        setFromStringBytes(kmer.length(), kmer.getBytes(), 0);
    }

    /**
     * Deep copy of the given bytes data
     * 
     * @param newData
     *            : byte array to copy (should have a header)
     * @param offset
     */
    public int setAsCopy(byte[] newData, int offset) {
        int k = Marshal.getInt(newData, offset);
        reset(k);
        System.arraycopy(newData, offset + HEADER_SIZE, bytes, this.kmerStartOffset, bytesUsed);
        return offset + HEADER_SIZE + bytesUsed;
    }

    /**
     * Point this datablock to the given bytes array It works like the pointer
     * to new datablock.
     * 
     * @param newData
     *            : byte array to copy (should have a header)
     * @param blockOffset
     */
    public int setAsReference(byte[] newData, int blockOffset) {
        bytes = newData;
        kmerStartOffset = blockOffset + HEADER_SIZE;
        int kRequested = Marshal.getInt(newData, blockOffset);
        int bytesRequested = KmerUtil.getByteNumFromK(kRequested) + HEADER_SIZE;
        if (newData.length - blockOffset < bytesRequested) {
            throw new IllegalArgumentException("Requested " + bytesRequested + " bytes (k=" + kRequested
                    + ") but buffer has only " + (newData.length - blockOffset) + " bytes");
        }
        storageMaxSize = bytesRequested; // since we are a reference, store our max capacity
        setKmerLength(kRequested);
        return blockOffset + bytesRequested;
    }

    /**
     * Shallow copy of the given kmer (s.t. we are backed by the same bytes)
     * WARNING: Changes in the kmerLength after using setAsReference may not always
     * be reflected in either `other` or `this`!
     */
    public void setAsReference(VKmer other) {
        this.bytes = other.bytes;
        this.bytesUsed = other.bytesUsed;
        this.kmerStartOffset = other.kmerStartOffset;
        this.lettersInKmer = other.lettersInKmer;
        this.storageMaxSize = other.storageMaxSize;
    }

    /**
     * Reset array by kmerlength
     * 
     * @param k
     */
    public void reset(int k) {
        int newByteLength = KmerUtil.getByteNumFromK(k);
        if (bytesUsed < newByteLength) {
            bytes = new byte[newByteLength + HEADER_SIZE];
            kmerStartOffset = HEADER_SIZE;
            storageMaxSize = bytes.length;
        }
        setKmerLength(k);
    }

    protected void clearLeadBit() {
        if (lettersInKmer % 4 != 0) {
            bytes[kmerStartOffset] &= (1 << ((lettersInKmer % 4) << 1)) - 1;
        }
    }

    /**
     * Get one genecode (A|G|C|T) from the given kmer index e.g. Get the 3th (start from 0)
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
        return (byte) ((bytes[kmerStartOffset + bytesUsed - 1 - posByte] >> shift) & 0x3);
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
        byte output = (byte) (bytes[kmerStartOffset + bytesUsed - 1] & 0x03);
        for (int i = bytesUsed - 1; i > 0; i--) {
            byte in = (byte) (bytes[kmerStartOffset + i - 1] & 0x03);
            bytes[kmerStartOffset + i] = (byte) (((bytes[kmerStartOffset + i] >>> 2) & 0x3f) | (in << 6));
        }
        int pos = ((lettersInKmer - 1) % 4) << 1;
        byte code = (byte) (c << pos);
        bytes[kmerStartOffset] = (byte) (((bytes[kmerStartOffset] >>> 2) & 0x3f) | code);
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
        byte output = (byte) ((bytes[kmerStartOffset] >> pos) & 0x03);
        for (int i = 0; i < bytesUsed - 1; i++) {
            byte in = (byte) ((bytes[kmerStartOffset + i + 1] >> 6) & 0x03);
            bytes[kmerStartOffset + i] = (byte) ((bytes[kmerStartOffset + i] << 2) | in);
        }
        bytes[kmerStartOffset + bytesUsed - 1] = (byte) ((bytes[kmerStartOffset + bytesUsed - 1] << 2) | c);
        clearLeadBit();
        return output;
    }

    public int getKmerLetterLength() {
        return lettersInKmer;
    }

    @Override
    public byte[] getBytes() {
        return ByteBuffer.wrap(bytes, getBlockOffset(), getLength()).array();
    }

    public byte[] getBlockBytes() {
        return bytes;
    }

    /**
     * Return the (hyracks-specific) data block offset. This includes the header.
     */
    public int getBlockOffset() {
        return kmerStartOffset - HEADER_SIZE;
    }

    /**
     * Return the data block offset where the kmer data begins. This excludes the header.
     */
    public int getKmerOffset() {
        return kmerStartOffset;
    }

    /**
     * Return the number of bytes used by both header and kmer chain
     */
    @Override
    public int getLength() {
        return bytesUsed + HEADER_SIZE;
    }

    /**
     * Return the number of bytes used by the kmer chain
     */
    public int getKmerByteLength() {
        return bytesUsed;
    }

    public void setKmerLength(int k) {
        this.bytesUsed = KmerUtil.getByteNumFromK(k);
        this.lettersInKmer = k;
        saveHeader(k);
    }

    protected int getKmerByteCapacity() {
        return storageMaxSize - HEADER_SIZE;
    }

    protected void setKmerByteCapacity(int new_cap) {
        if (new_cap != getKmerByteCapacity()) {
            byte[] new_data = new byte[new_cap + HEADER_SIZE];
            if (new_cap < bytesUsed) {
                bytesUsed = new_cap;
            }
            if (bytesUsed != 0) {
                System.arraycopy(bytes, kmerStartOffset, new_data, HEADER_SIZE, bytesUsed);
            }
            bytes = new_data;
            kmerStartOffset = HEADER_SIZE;
            storageMaxSize = bytes.length;
        }
    }

    private void saveHeader(int length) {
        Marshal.putInt(length, bytes, kmerStartOffset - HEADER_SIZE);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lettersInKmer = in.readInt();
        bytesUsed = KmerUtil.getByteNumFromK(lettersInKmer);
        if (lettersInKmer > 0) {
            if (getKmerByteCapacity() < this.bytesUsed) {
                this.bytes = new byte[this.bytesUsed + HEADER_SIZE];
                this.kmerStartOffset = HEADER_SIZE;
                storageMaxSize = bytes.length;
            }
            in.readFully(bytes, kmerStartOffset, bytesUsed);
        }
        saveHeader(lettersInKmer);
    }

    /**
     * write the entire byte array including the header
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.write(bytes, kmerStartOffset - HEADER_SIZE, bytesUsed + HEADER_SIZE);
    }

    @Override
    public int hashCode() {
        return Marshal.hashBytes(bytes, kmerStartOffset - HEADER_SIZE, bytesUsed + HEADER_SIZE);
    }

    @Override
    public boolean equals(Object right_obj) {
        if (right_obj instanceof VKmer) {
            // since these may be backed by storage of different sizes, we have to manually check each byte, including the header
            VKmer right = (VKmer) right_obj;
            for (int i = -HEADER_SIZE; i < bytesUsed; i++) {
                if (bytes[kmerStartOffset + i] != right.bytes[right.kmerStartOffset + i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return KmerUtil.recoverKmerFrom(this.lettersInKmer, bytes, kmerStartOffset, bytesUsed);
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(VKmer.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int kmerlength1 = Marshal.getInt(b1, s1);
            int kmerlength2 = Marshal.getInt(b2, s2);
            if (kmerlength1 == kmerlength2) {
                return compareBytes(b1, s1 + HEADER_SIZE, KmerUtil.getByteNumFromK(kmerlength1), b2, s2 + HEADER_SIZE,
                        KmerUtil.getByteNumFromK(kmerlength2));
            }
            return kmerlength1 - kmerlength2;
        }
    }

    static { // register this comparator
        WritableComparator.define(VKmer.class, new Comparator());
    }

    /**
     * Ensures that there is space for at least `size` bytes of kmer (not
     * including any header)
     */
    protected void setSize(int size) {
        if (size > getKmerByteCapacity()) {
            setKmerByteCapacity((size * 3 / 2));
        }
        this.bytesUsed = size;
    }

    public void setFromStringBytes(int k, byte[] stringBytes, int start) {
        reset(k);
        setFromStringBytes(stringBytes, start);
    }

    /**
     * Read Kmer from read text into bytes array e.g. AATAG will compress as
     * [0x000G, 0xATAA]
     * 
     * @param stringBytes
     * @param start
     */
    public void setFromStringBytes(byte[] stringBytes, int start) {
        byte l = 0;
        int bytecount = 0;
        int bcount = this.bytesUsed - 1;
        for (int i = start; i < start + lettersInKmer && i < stringBytes.length; i++) {
            byte code = GeneCode.getCodeFromSymbol(stringBytes[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[kmerStartOffset + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[kmerStartOffset] = l;
        }
    }

    public void setReversedFromStringBytes(int k, byte[] stringBytes, int start) {
        reset(k);
        setReversedFromStringBytes(stringBytes, start);
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
        int bcount = bytesUsed - 1;
        // for (int i = start + kmerlength - 1; i >= 0 && i < array.length; i--)
        // {
        for (int i = start + lettersInKmer - 1; i >= start && i < array.length; i--) {
            byte code = GeneCode.getPairedCodeFromSymbol(array[i]);
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[kmerStartOffset + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[kmerStartOffset] = l;
        }
    }

    /**
     * Merge Kmer with the next connected Kmer e.g. AAGCTAA merge with AACAACC,
     * if the initial kmerSize = 3 then it will return AAGCTAACAACC
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param kmer
     *            : the next kmer
     */
    public void mergeWithFFKmer(int initialKmerSize, VKmer kmer) {
        if (lettersInKmer < initialKmerSize - 1 || kmer.lettersInKmer < initialKmerSize - 1) {
            throw new IllegalArgumentException("Not enough letters in the kmers to perform a merge! Tried K="
                    + initialKmerSize + ", merge '" + this + "' with '" + kmer + "'.");
        }
        int preKmerLength = lettersInKmer;
        int preSize = bytesUsed;
        lettersInKmer += kmer.lettersInKmer - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(lettersInKmer));
        for (int i = 1; i <= preSize; i++) {
            bytes[kmerStartOffset + bytesUsed - i] = bytes[kmerStartOffset + preSize - i];
        }
        for (int k = initialKmerSize - 1; k < kmer.getKmerLetterLength(); k += 4) {
            byte onebyte = Kmer.getOneByteFromKmerAtPosition(k, kmer.bytes, kmer.kmerStartOffset, kmer.bytesUsed);
            Kmer.appendOneByteAtPosition(preKmerLength + k - initialKmerSize + 1, onebyte, bytes, kmerStartOffset,
                    bytesUsed);
        }
        clearLeadBit();
        saveHeader(lettersInKmer);
    }

    public void mergeWithFFKmer(int kmerSize, Kmer kmer) {
        // TODO make this more efficient
        mergeWithFFKmer(kmerSize, new VKmer(kmer.toString()));
    }

    public void mergeWithFRKmer(int initialKmerSize, VKmer kmer) {
        VKmer revcomp = new VKmer();
        revcomp.setReversedFromStringBytes(kmer.getKmerLetterLength(), kmer.toString().getBytes(), 0);
        mergeWithFFKmer(initialKmerSize, revcomp);
    }

    /**
     * Merge Kmer with the next connected Kmer, when that Kmer needs to be
     * reverse-complemented e.g. AAGCTAA merge with GGTTGTT, if the initial
     * kmerSize = 3 then it will return AAGCTAACAACC A merge B => A B~
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param kmer
     *            : the next kmer
     */
    public void mergeWithFRKmerOLD(int initialKmerSize, VKmer kmer) {
        if (lettersInKmer < initialKmerSize - 1 || kmer.lettersInKmer < initialKmerSize - 1) {
            throw new IllegalArgumentException("Not enough letters in the kmers to perform a merge! Tried K="
                    + initialKmerSize + ", merge '" + this + "' with '" + kmer + "'.");
        }
        int preSize = bytesUsed;
        int preKmerLength = lettersInKmer;
        lettersInKmer += kmer.lettersInKmer - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(lettersInKmer));
        // copy prefix into right-side of buffer
        for (int i = 1; i <= preSize; i++) {
            bytes[kmerStartOffset + bytesUsed - i] = bytes[kmerStartOffset + preSize - i];
        }

        int bytecount = (preKmerLength % 4) * 2;
        int bcount = bytesUsed - preSize - bytecount / 8; // may overlap
                                                          // previous kmer
        byte l = bcount == bytesUsed - preSize ? bytes[kmerStartOffset + bcount] : 0x00;
        bytecount %= 8;
        for (int i = kmer.lettersInKmer - initialKmerSize; i >= 0; i--) {
            byte code = GeneCode.getPairedGeneCode(kmer.getGeneCodeAtPosition(i));
            l |= (byte) (code << bytecount);
            bytecount += 2;
            if (bytecount == 8) {
                bytes[kmerStartOffset + bcount--] = l;
                l = 0;
                bytecount = 0;
            }
        }
        if (bcount >= 0) {
            bytes[kmerStartOffset] = l;
        }
        saveHeader(lettersInKmer);
    }

    public void mergeWithFRKmer(int kmerSize, Kmer kmer) {
        // TODO make this more efficient
        mergeWithFRKmer(kmerSize, new VKmer(kmer.toString()));
    }

    /**
     * Merge Kmer with the previous connected Kmer, when that kmer needs to be
     * reverse-complemented e.g. AACAACC merge with TTCTGCC, if the initial
     * kmerSize = 3 then it will return GGCAGAACAACC
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param preKmer
     *            : the previous kmer
     */
    public void mergeWithRFKmer(int initialKmerSize, VKmer preKmer) {
        // TODO make this more efficient
        VKmer reversed = new VKmer(preKmer.lettersInKmer);
        reversed.setReversedFromStringBytes(preKmer.toString().getBytes(), 0);
        mergeWithRRKmer(initialKmerSize, reversed);
    }

    public void mergeWithRFKmer(int kmerSize, Kmer kmer) {
        // TODO make this more efficient
        mergeWithRFKmer(kmerSize, new VKmer(kmer.toString()));
    }

    /**
     * Merge Kmer with the previous connected Kmer e.g. AACAACC merge with
     * AAGCTAA, if the initial kmerSize = 3 then it will return AAGCTAACAACC
     * 
     * @param initialKmerSize
     *            : the initial kmerSize
     * @param preKmer
     *            : the previous kmer
     */
    public void mergeWithRRKmer(int initialKmerSize, VKmer preKmer) {
        if (lettersInKmer < initialKmerSize - 1 || preKmer.lettersInKmer < initialKmerSize - 1) {
            throw new IllegalArgumentException("Not enough letters in the kmers to perform a merge! Tried K="
                    + initialKmerSize + ", merge '" + this + "' with '" + preKmer + "'.");
        }
        int preKmerLength = lettersInKmer;
        int preSize = bytesUsed;
        lettersInKmer += preKmer.lettersInKmer - initialKmerSize + 1;
        setSize(KmerUtil.getByteNumFromK(lettersInKmer));
        byte cacheByte = Kmer.getOneByteFromKmerAtPosition(0, bytes, kmerStartOffset, preSize);

        // copy prekmer
        for (int k = 0; k < preKmer.lettersInKmer - initialKmerSize + 1; k += 4) {
            byte onebyte = Kmer.getOneByteFromKmerAtPosition(k, preKmer.bytes, preKmer.kmerStartOffset,
                    preKmer.bytesUsed);
            Kmer.appendOneByteAtPosition(k, onebyte, bytes, kmerStartOffset, bytesUsed);
        }

        // copy current kmer
        int k = 4;
        for (; k < preKmerLength; k += 4) {
            byte onebyte = Kmer.getOneByteFromKmerAtPosition(k, bytes, kmerStartOffset, preSize);
            Kmer.appendOneByteAtPosition(preKmer.lettersInKmer - initialKmerSize + k - 4 + 1, cacheByte, bytes,
                    kmerStartOffset, bytesUsed);
            cacheByte = onebyte;
        }
        Kmer.appendOneByteAtPosition(preKmer.lettersInKmer - initialKmerSize + k - 4 + 1, cacheByte, bytes,
                kmerStartOffset, bytesUsed);
        clearLeadBit();
        saveHeader(lettersInKmer);
    }

    public void mergeWithRRKmer(int kmerSize, Kmer kmer) {
        // TODO make this more efficient
        mergeWithRRKmer(kmerSize, new VKmer(kmer.toString()));
    }

    public void mergeWithKmerInDir(EDGETYPE edgeType, int initialKmerSize, VKmer kmer) {
        switch (edgeType) {
            case FF:
                mergeWithFFKmer(initialKmerSize, kmer);
                break;
            case FR:
                mergeWithFRKmer(initialKmerSize, kmer);
                break;
            case RF:
                mergeWithRFKmer(initialKmerSize, kmer);
                break;
            case RR:
                mergeWithRRKmer(initialKmerSize, kmer);
                break;
            default:
                throw new RuntimeException("Direction not recognized: " + edgeType);
        }
    }

    public void mergeWithKmerInDir(EDGETYPE edgeType, int initialKmerSize, Kmer kmer) {
        // TODO make this more efficient
        mergeWithKmerInDir(edgeType, initialKmerSize, new VKmer(kmer.toString()));
    }

    public Kmer asFixedLengthKmer() {
        if (lettersInKmer != Kmer.getKmerLength()) {
            throw new IllegalArgumentException("VKmer " + this.toString()
                    + " is not of the same length as the fixed length Kmer (" + Kmer.getKmerLength() + " )!");
        }
        return new Kmer(bytes, kmerStartOffset);
    }

    /**
     * return the edit distance required to transform kemr1 into kmer2 using substitutions, insertions, and deletions.
     * This uses the classic dynamic programming algorithm and takes O(length_1 * length_2) time and space.
     */
    public static int editDistance(VKmer kmer1, VKmer kmer2) {
        int rows = kmer1.getKmerLetterLength() + 1, columns = kmer2.getKmerLetterLength() + 1, r = 0, c = 0, match = 0;
        int[][] distMat = new int[rows][columns];

        // initialize top row and left column
        for (r = 0; r < rows; r++) {
            distMat[r][0] = r;
        }
        for (c = 0; c < columns; c++) {
            distMat[0][c] = c;
        }

        // fill out the matrix as the min of left+1, up+1, and diag+nomatch
        for (r = 1; r < rows; r++) {
            for (c = 1; c < columns; c++) {
                match = kmer1.getGeneCodeAtPosition(r - 1) == kmer2.getGeneCodeAtPosition(c - 1) ? 0 : 1;
                distMat[r][c] = min(distMat[r - 1][c] + 1, distMat[r][c - 1] + 1, distMat[r - 1][c - 1] + match);
            }
        }
        return distMat[rows - 1][columns - 1];
    }

    private static int min(int a, int b, int c) {
        return a <= b ? (a <= c ? a : c) : (b <= c ? b : c);
    }

    private static int min(int a, int b) {
        return a <= b ? a : b;
    }

    public int editDistance(VKmer other) {
        return editDistance(this, other);
    }

    /**
     * @return true iff kmer1 (starting at offset1) matches the letters of kmer2 (starting at offset2) for length letters
     */
    public static boolean matchesExactly(VKmer kmer1, int offset1, VKmer kmer2, int offset2, int length) {
        if (length < 0)
            throw new IllegalArgumentException("invalid length " + length + " provided to matchesExactly " + kmer1
                    + " " + kmer2);
        // length mismatches will never match exactly
        if (kmer1.getKmerLetterLength() < offset1 + length)
            return false;
        if (kmer2.getKmerLetterLength() < offset2 + length)
            return false;
        //ELMIRA
        /**
        if (offset1 < 0){
        	return false;
        }
        **/
        // check each letter
        for (int i = 0; i < length; i++) {
            if (kmer1.getGeneCodeAtPosition(offset1 + i) != kmer2.getGeneCodeAtPosition(offset2 + i)) {
                return false;
            }
        }
        return true;
    }

    public boolean matchesExactly(int thisOffset, VKmer otherKmer, int otherOffset, int length) {
        return matchesExactly(this, thisOffset, otherKmer, otherOffset, length);
    }

    /**
     * Search for queryKmer (starting at queryOffset and running kmerLength letters) within targetKmer, from targetStart to targetEnd.
     * 
     * @return true if queryKmer was found within the given range of targetKmer
     */
    public boolean matchesInRange(VKmer targetKmer, int targetStart, int targetEnd, VKmer queryKmer, int queryOffset,
            int kmerLength) {
        targetStart = Math.max(0, targetStart);
        targetEnd = Math.min(targetKmer.getKmerLetterLength(), targetEnd);

        // TODO finish this using Nan's method?
        return false;
    }

    public boolean matchesInRange(int targetStart, int targetEnd, VKmer queryKmer, int queryOffset, int kmerLength) {
        return matchesInRange(this, targetStart, targetEnd, queryKmer, queryOffset, kmerLength);
    }

    public int indexOf(VKmer pattern) {
        return indexOf(this, pattern);
    }

    public int indexOfRangeQuery(VKmer pattern, int pStart, int pEnd, int mStart, int mEnd) throws IOException {
        return indexOfRangeQuery(pattern, pStart, pEnd, this, mStart, mEnd);
    }

    /**
     * use KMP to fast detect whether master Vkmer contain pattern (only detect the first position which pattern match);
     * if true return index, otherwise return -1;
     * 
     * @param master
     * @param pattern
     * @return
     */
    public static int indexOf(VKmer master, VKmer pattern) {
        int patternSize = pattern.getKmerLetterLength();
        int strSize = master.getKmerLetterLength();
        int[] failureSet = computeFailureSet(pattern, patternSize);
        int p = 0;
        int m = 0;
        while (p < patternSize && m < strSize) {
            if (pattern.getGeneCodeAtPosition(p) == master.getGeneCodeAtPosition(m)) {
                p++;
                m++;
            } else if (p == 0) {
                m++;
            } else {
                p = failureSet[p - 1] + 1;
            }
        }
        if (p < patternSize) {
            return -1;
        } else {
            return m - patternSize;
        }
    }

    public static int indexOfRangeQuery(VKmer pattern, int pStart, int pEnd, VKmer master, int mStart, int mEnd)
            throws IOException {

        if ((pStart > pEnd) || (mStart > mEnd)) {
            throw new IOException("index Start can't exceed index End!");
        }
        if ((pStart < 0) || (mStart < 0) || (pEnd >= pattern.getKmerLetterLength())
                || (mEnd >= master.getKmerLetterLength())) {
            throw new IOException("index Start can't be less than 0, or index End can't exceed the kmer length!");
        }
        if ((pEnd - pStart) > (mEnd - mStart)) {
            throw new IOException("sdfsdf");
        }

        int subPsize = pEnd - pStart + 1;
        int subMSize = mEnd - mStart + 1;
        int[] failureSet = computeFailureSetWithRange(pattern, subPsize, pStart, pEnd);
        int p = 0;
        int m = 0;
        while (p < subPsize && m < subMSize) {
            if (pattern.getGeneCodeAtPosition(p + pStart) == master.getGeneCodeAtPosition(m + mStart)) {
                p++;
                m++;
            } else if (p == 0) {
                m++;
            } else {
                p = failureSet[p - 1] + 1;
            }
        }
        if (p < subPsize) {
            return -1;
        } else {
            return m + mStart - subPsize;
        }
    }

    /**
     * compute the failure function of KMP algorithm
     * 
     * @param failureSet
     * @param pattern
     * @return
     */
    protected static int[] computeFailureSet(VKmer pattern, int patternSize) {
        int[] failureSet = new int[patternSize];
        int i = 0;
        failureSet[0] = -1;
        for (int j = 1; j < pattern.getKmerLetterLength(); j++) {
            i = failureSet[j - 1];
            while (i > 0 && pattern.getGeneCodeAtPosition(j) != pattern.getGeneCodeAtPosition(i + 1)) {
                i = failureSet[i];
            }
            if (pattern.getGeneCodeAtPosition(j) == pattern.getGeneCodeAtPosition(i + 1)) {
                failureSet[j] = i + 1;
            } else
                failureSet[j] = -1;
        }
        return failureSet;
    }

    protected static int[] computeFailureSetWithRange(VKmer pattern, int subPsize, int start, int end) {
        int[] failureSet = new int[subPsize];
        int i = 0;
        failureSet[0] = -1;
        for (int j = 1; j < subPsize; j++) {
            i = failureSet[j - 1];
            while (i > 0 && pattern.getGeneCodeAtPosition(j + start) != pattern.getGeneCodeAtPosition(i + start + 1)) {
                i = failureSet[i];
            }
            if (pattern.getGeneCodeAtPosition(j + start) == pattern.getGeneCodeAtPosition(i + start + 1)) {
                failureSet[j] = i + 1;
            } else
                failureSet[j] = -1;
        }
        return failureSet;
    }

    /**
     * return the fractional difference between the given kmers. This is the edit distance divided by the smaller length.
     * Note: the fraction may be larger than 1 (when the edit distance is larger than the kmer)
     * For example, two kmers AAAAA and AAAT have an edit distance of 2; the fracDissimilar will be 2/4 = .5
     */
    public static float fracDissimilar(VKmer kmer1, VKmer kmer2) {
        return editDistance(kmer1, kmer2) / (float) min(kmer1.getKmerLetterLength(), kmer2.getKmerLetterLength());
    }

    public float fracDissimilar(boolean sameOrientation, VKmer other) {
        if (sameOrientation)
            return fracDissimilar(this, other);
        else {
            String reverse = other.toString(); // TODO don't use toString here (something more efficient?)
            VKmer reverseKmer = new VKmer();
            reverseKmer.setReversedFromStringBytes(reverse.length(), reverse.getBytes(), 0);
            return fracDissimilar(this, reverseKmer);
        }
    }

    public float editDistance(boolean sameOrientation, VKmer other) {
        if (sameOrientation)
            return editDistance(this, other);
        else {
            String reverse = other.toString(); // TODO don't use toString here (something more efficient?)
            VKmer reverseKmer = new VKmer();
            reverseKmer.setReversedFromStringBytes(reverse.length(), reverse.getBytes(), 0);
            return editDistance(this, reverseKmer);
        }
    }

    @Override
    public int compareTo(BinaryComparable other) {
        Comparator c = new Comparator();
        if (other instanceof VKmer) {
            VKmer otherVK = (VKmer) other;
            return c.compare(getBlockBytes(), getBlockOffset(), getLength(), otherVK.getBlockBytes(),
                    otherVK.getBlockOffset(), otherVK.getLength());
        }
        return c.compare(getBlockBytes(), getBlockOffset(), getLength(), other.getBytes(), 0, other.getLength());
    }

    public static VKmer reverse(VKmer other) {
        String reverse = other.toString(); // TODO don't use toString here (something more efficient?)
        VKmer reverseKmer = new VKmer();
        reverseKmer.setReversedFromStringBytes(reverse.length(), reverse.getBytes(), 0);
        return reverseKmer;
    }

    public VKmer reverse() {
        return reverse(this);
    }

}
