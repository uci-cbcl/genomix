package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class KmerCountValue implements  Writable{
	private byte adjBitMap;
	private byte count;

	public KmerCountValue(byte bitmap, byte count) {
		reset(bitmap, count);
	}
	
	public KmerCountValue() {
		adjBitMap = 0;
		count = 0;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		adjBitMap = arg0.readByte();
		count = arg0.readByte();
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeByte(adjBitMap);
		arg0.writeByte(count);
	}

	@Override
	public String toString() {
		return Kmer.GENE_CODE.getSymbolFromBitMap(adjBitMap) + '\t' + String.valueOf(count);
	}

	public void reset(byte bitmap, byte count) {
		this.adjBitMap = bitmap;
		this.count = count;
	}

}