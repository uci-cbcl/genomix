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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KmerBytesWritable extends BinaryComparable implements
		WritableComparable<BinaryComparable> {
	private byte size;
	private byte[] bytes;
	private byte kmerlength;

	/**
	 * Initial Kmer space by kmerlength
	 * 
	 * @param k
	 *            kmerlength
	 */
	public KmerBytesWritable(int k) {
		int length = KmerUtil.getByteNumFromK(k);
		this.bytes = new byte[length];
		this.size = (byte) length;
		this.kmerlength = (byte) k;
	}

	public KmerBytesWritable(KmerBytesWritable right) {
		this.bytes = new byte[right.size];
		this.size =  right.size;
		set(right);
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
		return (int) size;
	}

	public void setSize(byte size) {
		if ((int) size > getCapacity()) {
			setCapacity((byte) (size * 3 / 2));
		}
		this.size = size;
	}

	public int getCapacity() {
		return bytes.length;
	}

	public void setCapacity(byte new_cap) {
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
		this.kmerlength = (byte) k;
		setSize((byte) 0);
		setSize((byte) KmerUtil.getByteNumFromK(k));

		byte l = 0;
		int bytecount = 0;
		int bcount = this.size - 1;
		for (int i = start; i < start + k; i++) {
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
	public void setByReadReverse(int k, byte[] array, int start) {
		this.kmerlength = (byte) k;
		setSize((byte) 0);
		setSize((byte) KmerUtil.getByteNumFromK(k));

		byte l = 0;
		int bytecount = 0;
		int bcount = size - 1;
		for (int i = start + k - 1; i >= 0; i--) {
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
		return (byte) (1 << output);
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
		// (k%4) * 2
		if (kmerlength % 4 != 0) {
			bytes[0] &= (1 << ((kmerlength % 4) << 1)) - 1;
		}
		bytes[size - 1] = (byte) ((bytes[size - 1] << 2) | c);
		return (byte) (1 << output);
	}

	public void set(KmerBytesWritable newData) {
		set(newData.kmerlength, newData.bytes, (byte) 0, newData.size);
	}

	public void set(int k, byte[] newData, int offset, int length) {
		this.kmerlength = (byte) k;
		setSize((byte) 0);
		setSize((byte) (length - offset));
		System.arraycopy(newData, offset, bytes, 0, size);
	}

	/**
	 * Reset array by kmerlength
	 * @param k
	 */
	public void reset(int k) {
		this.kmerlength = (byte) k;
		setSize((byte) 0);
		setSize((byte) KmerUtil.getByteNumFromK(k));
	}

	public void readFields(DataInput in) throws IOException {
		this.kmerlength = in.readByte();
		setSize((byte) 0); // clear the old data
		setSize(in.readByte());
		in.readFully(bytes, 0, size);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(this.kmerlength);
		out.writeByte(size);
		out.write(bytes, 0, size);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	@Override
	public boolean equals(Object right_obj) {
		if (right_obj instanceof KmerBytesWritable)
			return this.kmerlength == ((KmerBytesWritable) right_obj).kmerlength
					&& super.equals(right_obj);
		return false;
	}

	@Override
	public String toString() {
		return KmerUtil.recoverKmerFrom(this.kmerlength, this.getBytes(), 0,
				this.getLength());
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
