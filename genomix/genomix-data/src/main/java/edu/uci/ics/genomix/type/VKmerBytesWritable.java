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

import org.apache.hadoop.io.WritableComparator;

import edu.uci.ics.genomix.data.KmerUtil;
import edu.uci.ics.genomix.data.Marshal;

/**
 * Variable-length kmer which stores its length internally.
 * 
 * Note: `offset` as used in this class is the offset at which the *kmer*
 * begins. There is a {@value HEADER_SIZE}-byte header preceding the kmer
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
		this(EMPTY_BYTES, HEADER_SIZE);
	}

	/**
	 * Copy contents of kmer string
	 */
	public VKmerBytesWritable(String kmer) {
		bytes = new byte[HEADER_SIZE + KmerUtil.getByteNumFromK(kmer.length())];
		offset = HEADER_SIZE;
		setAsCopy(kmer);
	}

	/**
	 * Set as reference to given data
	 * 
	 * @param storage
	 *            : byte array with header
	 * @param offset
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
		offset = HEADER_SIZE;
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
			System.arraycopy(other.bytes, other.offset, bytes, this.offset,
					bytesUsed);
		}
	}

	/**
	 * set from String kmer
	 */
	@Override
	public void setAsCopy(String kmer) {
		int k = kmer.length();
		reset(k);
		System.arraycopy(kmer.getBytes(), 0, bytes, offset, bytesUsed);
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
		System.arraycopy(newData, offset + HEADER_SIZE, bytes, this.offset,
				bytesUsed);
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
		this.offset = offset + HEADER_SIZE;
		int kRequested = Marshal.getInt(newData, offset);
		int bytesRequested = KmerUtil.getByteNumFromK(kRequested) + HEADER_SIZE;
		if (newData.length - offset < bytesRequested) {
			throw new IllegalArgumentException("Requested " + bytesRequested
					+ " bytes (k=" + kRequested + ") but buffer has only "
					+ (newData.length - offset) + " bytes");
		}
		setKmerLength(kRequested);
	}

	@Override
	public void setKmerLength(int k) {
		this.bytesUsed = KmerUtil.getByteNumFromK(k);
		this.lettersInKmer = k;
		Marshal.putInt(k, bytes, offset - HEADER_SIZE);
	}

	@Override
	protected int getCapacity() {
		return bytes.length - HEADER_SIZE;
	}

	@Override
	protected void setCapacity(int new_cap) {
		if (new_cap != getCapacity()) {
			byte[] new_data = new byte[new_cap + HEADER_SIZE];
			if (new_cap < bytesUsed) {
				bytesUsed = new_cap;
			}
			if (bytesUsed != 0) {
				System.arraycopy(bytes, offset, new_data, HEADER_SIZE,
						bytesUsed);
			}
			bytes = new_data;
			offset = HEADER_SIZE;
		}
	}

	@Override
	public void mergeWithFFKmer(int initialKmerSize, KmerBytesWritable kmer) {
		super.mergeWithFFKmer(initialKmerSize, kmer);
		Marshal.putInt(lettersInKmer, bytes, offset - HEADER_SIZE);
	}

	@Override
	public void mergeWithFRKmer(int initialKmerSize, KmerBytesWritable kmer) {
		super.mergeWithFRKmer(initialKmerSize, kmer);
		Marshal.putInt(lettersInKmer, bytes, offset - HEADER_SIZE);
	}

	@Override
	public void mergeWithRFKmer(int initialKmerSize, KmerBytesWritable preKmer) {
		super.mergeWithRFKmer(initialKmerSize, preKmer);
		Marshal.putInt(lettersInKmer, bytes, offset - HEADER_SIZE);
	}

	@Override
	public void mergeWithRRKmer(int initialKmerSize, KmerBytesWritable preKmer) {
		super.mergeWithRRKmer(initialKmerSize, preKmer);
		Marshal.putInt(lettersInKmer, bytes, offset - HEADER_SIZE);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		lettersInKmer = in.readInt();
		bytesUsed = KmerUtil.getByteNumFromK(lettersInKmer);
		if (lettersInKmer > 0) {
			if (getCapacity() < this.bytesUsed) {
				this.bytes = new byte[this.bytesUsed + HEADER_SIZE];
				this.offset = HEADER_SIZE;
			}
			in.readFully(bytes, offset, bytesUsed);
		}
		Marshal.putInt(lettersInKmer, bytes, offset - HEADER_SIZE);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(bytes, offset - HEADER_SIZE, bytesUsed + HEADER_SIZE);
	}

	@Override
	public boolean equals(Object right) {
		if (right instanceof VKmerBytesWritable) {
			return super.equals(right); // compare bytes directly
		} else if (right instanceof KmerBytesWritable) {
			// for Kmers, we need to skip our header
			KmerBytesWritable rightKmer = (KmerBytesWritable) right;
			if (lettersInKmer != rightKmer.lettersInKmer) { // check length
				return false;
			}
			for (int i = 0; i < lettersInKmer; i++) { // check letters
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
				offset, this.getLength());
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
