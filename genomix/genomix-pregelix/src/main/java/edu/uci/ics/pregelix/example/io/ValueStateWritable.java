package edu.uci.ics.pregelix.example.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.pregelix.type.State;


public class ValueStateWritable implements WritableComparable<ValueStateWritable> {

	private byte value;
	private int state;

	public ValueStateWritable() {
		state = State.NON_VERTEX;
	}

	public ValueStateWritable(byte value, int state) {
		this.value = value;
		this.state = state;
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

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readByte();
		state = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(value);
		out.writeInt(state);
	}

	@Override
	public int compareTo(ValueStateWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}
