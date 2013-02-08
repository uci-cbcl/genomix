package edu.uci.ics.genomix.data.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ByteSerializerDeserializer implements
		ISerializerDeserializer<Byte> {

	private static final long serialVersionUID = 1L;

	public static final ByteSerializerDeserializer INSTANCE = new ByteSerializerDeserializer();

	private ByteSerializerDeserializer() {
	}

	@Override
	public Byte deserialize(DataInput in) throws HyracksDataException {
		try {
			return in.readByte();
		} catch (IOException e) {
			throw new HyracksDataException(e);
		}
	}

	@Override
	public void serialize(Byte instance, DataOutput out)
			throws HyracksDataException {
		try {
			out.writeByte(instance.intValue());
		} catch (IOException e) {
			throw new HyracksDataException(e);
		}
	}

	public static byte getByte(byte[] bytes, int offset) {
		return bytes[offset];
	}

	public static void putByte(byte val, byte[] bytes, int offset) {
		bytes[offset] = val;
	}

}