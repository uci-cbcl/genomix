package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.operator.NaiveAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;


public class ValueStateWritable implements WritableComparable<ValueStateWritable> {

	private byte adjMap;
	private byte state;
	private VKmerBytesWritable mergeChain;

	public ValueStateWritable() {
		state = State.NON_VERTEX;
		mergeChain = new VKmerBytesWritable(-1);
	}

	public ValueStateWritable(byte adjMap, byte state, VKmerBytesWritable mergeChain) {
		this.adjMap = adjMap;
		this.state = state;
		this.mergeChain.set(mergeChain);
	}
	
	public void set(byte adjMap, byte state, VKmerBytesWritable mergeChain){
		this.adjMap = adjMap;
		this.state = state;
		this.mergeChain.set(mergeChain);
	}

	public byte getAdjMap() {
		return adjMap;
	}

	public void setAdjMap(byte adjMap) {
		this.adjMap = adjMap;
	}

	public byte getState() {
		return state;
	}

	public void setState(byte state) {
		this.state = state;
	}

	public int getLengthOfMergeChain() {
		return mergeChain.getKmerLength();
	}

	public VKmerBytesWritable getMergeChain() {
		return mergeChain;
	}

	public void setMergeChain(KmerBytesWritable mergeChain) {
		this.mergeChain.set(mergeChain);
	}
	
	public void setMergeChain(VKmerBytesWritable mergeChain) {
		this.mergeChain.set(mergeChain);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		adjMap = in.readByte();
		state = in.readByte();
		mergeChain.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(adjMap);
		out.writeByte(state);
		mergeChain.write(out);
	}

	@Override
	public int compareTo(ValueStateWritable o) {
		return 0;
	}
	
	@Override
	public String toString() {
		if(mergeChain.getKmerLength() == -1 || mergeChain.getKmerLength() == 0)
			return GeneCode.getSymbolFromBitMap(adjMap);
		return 	GeneCode.getSymbolFromBitMap(adjMap) + "\t" +
				getLengthOfMergeChain() + "\t" +
				mergeChain.toString();
				//+ "\t" + state;
	}
	
}
