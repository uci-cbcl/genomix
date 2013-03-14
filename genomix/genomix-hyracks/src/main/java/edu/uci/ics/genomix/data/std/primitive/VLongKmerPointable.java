package edu.uci.ics.genomix.data.std.primitive;

import edu.uci.ics.genomix.data.partition.KmerHashPartitioncomputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public final class VLongKmerPointable extends AbstractPointable implements
		IHashable, IComparable, INumeric {
	public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean isFixedLength() {
			return false;
		}

		@Override
		public int getFixedLength() {
			return -1;
		}
	};

	public static final IPointableFactory FACTORY = new IPointableFactory() {
		private static final long serialVersionUID = 1L;

		@Override
		public IPointable createPointable() {
			return new VLongKmerPointable();
		}

		@Override
		public ITypeTraits getTypeTraits() {
			return TYPE_TRAITS;
		}
	};


	public long getLong() {
		return KmerHashPartitioncomputerFactory.getLong(bytes, start);
	}

	@Override
	public int compareTo(IPointable pointer) {
		return compareTo(pointer.getByteArray(), pointer.getStartOffset(),
				pointer.getLength());
	}

	@Override
	public int compareTo(byte[] bytes, int start, int length) {

		if (this.length != length) {
			return this.length - length;
		}

		for (int i = 0; i < length; i++) {
			if (this.bytes[this.start + i] < bytes[start + i]) {
				return -1;
			} else if (this.bytes[this.start + i] > bytes[start + i]) {
				return 1;
			}
		}
		return 0;
	}

	@Override
	public int hash() {// BKDRHash
		int hash = 1;
		for (int i = start + 1; i <= start + length; i++)
			hash = (31 * hash) + (int) bytes[i];
		if (hash < 0) {
			hash = -(hash + 1);
		}
		return hash;
	}

	@Override
	public byte byteValue() {
		return (byte) bytes[start + 1];
	}

	@Override
	public short shortValue() {

		return (short) ((bytes[start + 2] & 0xff) << 8 + bytes[start + 1] & 0xff);
	}

	@Override
	public int intValue() {
		return (int) ((bytes[start + 4] & 0xff) << 24 + (bytes[start + 3] & 0xff) << 16 + (bytes[start + 2] & 0xff) << 8 + bytes[start + 1] & 0xff);
	}

	@Override
	public long longValue() {
		return getLong();
	}

	@Override
	public float floatValue() {
		return getLong();
	}

	@Override
	public double doubleValue() {
		return getLong();
	}
}
