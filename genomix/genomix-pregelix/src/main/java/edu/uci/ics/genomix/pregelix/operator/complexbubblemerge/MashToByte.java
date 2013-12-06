package edu.uci.ics.genomix.pregelix.operator.complexbubblemerge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**
 * Enum can't be new and readFields() wouldn't make sense, but it still can be
 * written. ByteArrayListWritable is used for making an ArrayList of Enum type
 * 
 * @author anbangx
 */
public abstract class MashToByte<E> extends ArrayList<E> implements Writable {
	/**
     * 
     */
	private static final long serialVersionUID = 1L;

	abstract E get(byte val);

	abstract byte getByte(E val);

	@Override
	public void readFields(DataInput in) throws IOException {
		this.clear();

		int numFields = in.readInt();
		if (numFields == 0)
			return;

		E obj;
		for (int i = 0; i < numFields; i++) {
			obj = get(in.readByte());
			this.add(obj);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.size());
		if (size() == 0)
			return;
		E obj;

		for (int i = 0; i < size(); i++) {
			obj = get(i);
			if (obj == null) {
				throw new IOException("Cannot serialize null fields!");
			}
			out.writeByte(getByte(obj));
		}
	}

}
