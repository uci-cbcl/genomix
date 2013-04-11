package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

public class ValueWritable implements WritableComparable<ValueWritable> {

	private byte value;
	private int lengthOfMergeChain;
	private byte[] mergeChain;

	public ValueWritable() {
		lengthOfMergeChain = 0;
	}

	public ValueWritable(byte value, int lengthOfMergeChain, byte[] mergeChain) {
		this.value = value;
		this.lengthOfMergeChain = lengthOfMergeChain;
		this.mergeChain = mergeChain;
	}

	public byte getValue() {
		return value;
	}

	public void setValue(byte value) {
		this.value = value;
	}

	public int getLengthOfMergeChain() {
		return lengthOfMergeChain;
	}

	public void setLengthOfMergeChain(int lengthOfMergeChain) {
		this.lengthOfMergeChain = lengthOfMergeChain;
	}

	public byte[] getMergeChain() {
		return mergeChain;
	}

	public void setMergeChain(byte[] mergeChain) {
		this.mergeChain = mergeChain;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readByte();
		lengthOfMergeChain = in.readInt();
		if(lengthOfMergeChain != 0){
			mergeChain = new byte[(lengthOfMergeChain-1)/4 + 1];
			in.readFully(mergeChain);
		}
		else
			mergeChain = new byte[0];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(value);
		out.writeInt(lengthOfMergeChain);
		if(lengthOfMergeChain != 0)
			out.write(mergeChain);
	}

	@Override
	public int compareTo(ValueWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
