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
 * Variable kmer length byteswritable It was used to generate the graph in which
 * phase the kmer length doesn't change. Thus the kmerByteSize of bytes doesn't
 * change either.
 */
public class VKmerBytesWritable extends KmerBytesWritable {

	private static final long serialVersionUID = 1L;
	protected static final byte[] EMPTY_BYTES = { 0, 0, 0, 0 }; // int
																// indicating 0
																// length
	protected static final int HEADER_SIZE = 4; // number of bytes for header
												// info

	/**
	 * Initialize as empty kmer
	 */
	public VKmerBytesWritable() {
		this(EMPTY_BYTES, 0);
	}

	/**
	 * Copy contents of kmer string
	 */
	public VKmerBytesWritable(String kmer) {
		bytes = new byte[HEADER_SIZE + KmerUtil.getByteNumFromK(kmer.length())];
		offset = 0;
		setAsCopy(kmer);
	}

	/**
	 * Set as reference to given data
	 */
	public VKmerBytesWritable(byte[] storage, int offset) {
		setAsReference(storage, offset);
	}

	/**
	 * Reserve space for k letters
	 */
	public VKmerBytesWritable(int k) {
		if (k > 0) {
			bytes = new byte[HEADER_SIZE + KmerUtil.getByteNumFromK(k)];
		} else {
			bytes = EMPTY_BYTES;
		}
		offset = 0;
		setKmerLength(k);
	}

	/**
	 * deep copy of kmer in other
	 * 
	 * @param other
	 */
	public VKmerBytesWritable(VKmerBytesWritable other) {
		this(other.lettersInKmer);
		setAsCopy(other);
	}

	/**
	 * Deep copy of the given kmer
	 * 
	 * @param other
	 */
	@Override
	public void setAsCopy(KmerBytesWritable other) {
		reset(other.lettersInKmer);
		if (lettersInKmer > 0) {
			System.arraycopy(other.bytes, other.offset, bytes, this.offset
					+ HEADER_SIZE, lettersInKmer);
		}
	}

	/**
	 * Deep copy of the given kmer
	 * 
	 * @param other
	 */
	public void setAsCopy(VKmerBytesWritable other) {
		reset(other.lettersInKmer);
		if (lettersInKmer > 0) {
			System.arraycopy(other.bytes, other.offset + HEADER_SIZE, bytes,
					this.offset + HEADER_SIZE, lettersInKmer);
		}
	}

	/**
	 * set from String kmer
	 */
	@Override
	public void setAsCopy(String kmer) {
		int k = kmer.length();
		reset(k);
		System.arraycopy(kmer.getBytes(), 0, bytes, offset + HEADER_SIZE, k);
	}

	/**
	 * Deep copy of the given bytes data
	 * 
	 * @param k
	 * @param newData
	 *            : presumed NOT to have a header
	 * @param offset
	 */
	@Override
	public void setAsCopy(int k, byte[] newData, int offset) {
		reset(k);
		System.arraycopy(newData, offset, bytes, this.offset + HEADER_SIZE, k);
	}

	/**
	 * Deep copy of the given bytes data
	 * 
	 * @param newData
	 *            : byte array to copy (should have a header)
	 * @param offset
	 */
	public void setAsCopy(byte[] newData, int offset) {
		int k = Marshal.getInt(newData, offset);
		reset(k);
		System.arraycopy(newData, offset + HEADER_SIZE, bytes, this.offset
				+ HEADER_SIZE, k);
	}

	/**
	 * Point this datablock to the given bytes array It works like the pointer
	 * to new datablock.
	 * 
	 * @param newData
	 *            : byte array to copy (should have a header)
	 * @param offset
	 */
	public void setAsReference(byte[] newData, int offset) {
		this.bytes = newData;
		this.offset = offset;
		int kRequested = Marshal.getInt(newData, offset);
		int bytesRequested = KmerUtil.getByteNumFromK(kRequested);
		if (newData.length - offset < bytesRequested) {
			throw new IllegalArgumentException("Requested " + bytesRequested
					+ " bytes (k=" + kRequested + ") but buffer has only "
					+ (newData.length - offset) + " bytes");
		}
		setKmerLength(kRequested);
	}

	@Override
	public void setKmerLength(int k) {
		this.bytesUsed = HEADER_SIZE + KmerUtil.getByteNumFromK(k);
		this.lettersInKmer = k;
		Marshal.putInt(k, bytes, offset);
	}

	@Override
	public void setByRead(byte[] stringBytes, int start) {
		offset += HEADER_SIZE;
		super.setByRead(stringBytes, start);
		offset -= HEADER_SIZE;
	}

	@Override
	public void setByReadReverse(byte[] array, int start) {
		offset += HEADER_SIZE;
		super.setByReadReverse(array, start);
		offset -= HEADER_SIZE;
	}

	@Override
	public byte shiftKmerWithNextCode(byte c) {
		offset += HEADER_SIZE;
		byte retval = super.shiftKmerWithNextCode(c);
		offset -= HEADER_SIZE;
		return retval;

	}

	@Override
	public byte shiftKmerWithPreCode(byte c) {
		offset += HEADER_SIZE;
		byte retval = super.shiftKmerWithPreCode(c);
		offset -= HEADER_SIZE;
		return retval;
	}

	@Override
	public void mergeWithFFKmer(int initialKmerSize, KmerBytesWritable kmer) {
		offset += HEADER_SIZE;
		super.mergeWithFFKmer(initialKmerSize, kmer);
		offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	/**
	 * Merge with kmer that's FF to me. See KmerBytesWritable.mergeWithFFKmer.
	 */
	public void mergeWithFFKmer(int initialKmerSize, VKmerBytesWritable kmer) {
		offset += HEADER_SIZE;
		kmer.offset += HEADER_SIZE;
		super.mergeWithFFKmer(initialKmerSize, kmer);
		offset -= HEADER_SIZE;
		kmer.offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	@Override
	public void mergeWithFRKmer(int initialKmerSize, KmerBytesWritable kmer) {
		offset += HEADER_SIZE;
		super.mergeWithFRKmer(initialKmerSize, kmer);
		offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	/**
	 * Merge with kmer that's FR to me. See KmerBytesWritable.mergeWithFRKmer.
	 */
	public void mergeWithFRKmer(int initialKmerSize, VKmerBytesWritable kmer) {
		offset += HEADER_SIZE;
		kmer.offset += HEADER_SIZE;
		super.mergeWithFRKmer(initialKmerSize, kmer);
		offset -= HEADER_SIZE;
		kmer.offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	@Override
	public void mergeWithRFKmer(int initialKmerSize, KmerBytesWritable preKmer) {
		offset += HEADER_SIZE;
		super.mergeWithRFKmer(initialKmerSize, preKmer);
		offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	/**
	 * Merge with kmer that's RF to me. See KmerBytesWritable.mergeWithRFKmer.
	 */
	public void mergeWithRFKmer(int initialKmerSize, VKmerBytesWritable preKmer) {
		offset += HEADER_SIZE;
		preKmer.offset += HEADER_SIZE;
		super.mergeWithRFKmer(initialKmerSize, preKmer);
		offset -= HEADER_SIZE;
		preKmer.offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	@Override
	public void mergeWithRRKmer(int initialKmerSize, KmerBytesWritable preKmer) {
		offset += HEADER_SIZE;
		super.mergeWithRRKmer(initialKmerSize, preKmer);
		offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	/**
	 * Merge with kmer that's RR to me. See KmerBytesWritable.mergeWithRRKmer.
	 */
	public void mergeWithRRKmer(int initialKmerSize, VKmerBytesWritable preKmer) {
		offset += HEADER_SIZE;
		preKmer.offset += HEADER_SIZE;
		super.mergeWithRRKmer(initialKmerSize, preKmer);
		offset -= HEADER_SIZE;
		preKmer.offset -= HEADER_SIZE;
		Marshal.putInt(lettersInKmer, bytes, offset);
	}

	@Override
	protected void clearLeadBit() {
		offset += HEADER_SIZE;
		super.clearLeadBit();
		offset -= HEADER_SIZE;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		lettersInKmer = in.readInt();
		bytesUsed = HEADER_SIZE + KmerUtil.getByteNumFromK(lettersInKmer);
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
	public boolean equals(Object right) {
		if (right instanceof VKmerBytesWritable) {
			return this.lettersInKmer == ((VKmerBytesWritable) right).lettersInKmer
					&& super.equals(right);
		} else if (right instanceof KmerBytesWritable) {
			// for Kmers, we need to skip our header
			KmerBytesWritable rightKmer = (KmerBytesWritable) right;
			if (lettersInKmer != rightKmer.lettersInKmer) {
				// break early
				return false;
			}
			for (int i = 0; i < lettersInKmer; i++) {
				if (bytes[i + HEADER_SIZE] != rightKmer.bytes[i]) {
					return false;
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public String toString() {
		return KmerUtil.recoverKmerFrom(this.lettersInKmer, this.getBytes(),
				offset + HEADER_SIZE, this.getLength());
	}

	public static class Comparator extends WritableComparator {

		public Comparator() {
			super(VKmerBytesWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int kmerlength1 = Marshal.getInt(b1, s1);
			int kmerlength2 = Marshal.getInt(b2, s2);
			if (kmerlength1 == kmerlength2) {
				return compareBytes(b1, s1 + HEADER_SIZE, l1 - HEADER_SIZE, b2,
						s2 + HEADER_SIZE, l2 - HEADER_SIZE);
			}
			return kmerlength1 - kmerlength2;
		}
	}

	static { // register this comparator
		WritableComparator.define(VKmerBytesWritable.class, new Comparator());
	}

}
