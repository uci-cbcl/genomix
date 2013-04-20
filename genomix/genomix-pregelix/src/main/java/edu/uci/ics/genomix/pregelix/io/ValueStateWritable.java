package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.old.Kmer;


public class ValueStateWritable implements WritableComparable<ValueStateWritable> {

	private byte value;
	private int state;
	private int lengthOfMergeChain;
	private byte[] mergeChain;

	public ValueStateWritable() {
		state = State.NON_VERTEX;
		lengthOfMergeChain = 0;
	}

	public ValueStateWritable(byte value, int state, int lengthOfMergeChain, byte[] mergeChain) {
		this.value = value;
		this.state = state;
		this.lengthOfMergeChain = lengthOfMergeChain;
		this.mergeChain = mergeChain;
	}

	public byte getValue() {
		return value;
	}

	public void setValue(byte value) {
		this.value = value;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
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
		state = in.readInt();
		lengthOfMergeChain = in.readInt();
		if(lengthOfMergeChain < 0)
			System.out.println();
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
		out.writeInt(state);
		out.writeInt(lengthOfMergeChain);
		if(lengthOfMergeChain != 0)
			out.write(mergeChain);
	}

	@Override
	public int compareTo(ValueStateWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public String toString() {
		if(lengthOfMergeChain == 0)
			return Kmer.GENE_CODE.getSymbolFromBitMap(value);
		return 	Kmer.GENE_CODE.getSymbolFromBitMap(value) + "\t" +
				lengthOfMergeChain + "\t" +
				Kmer.recoverKmerFrom(lengthOfMergeChain, mergeChain, 0, mergeChain.length) + "\t" +
				state;
	}
	
}
