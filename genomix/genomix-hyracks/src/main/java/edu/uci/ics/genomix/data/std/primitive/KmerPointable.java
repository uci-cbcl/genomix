package edu.uci.ics.genomix.data.std.primitive;

import edu.uci.ics.genomix.data.std.accessors.KmerHashPartitioncomputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public final class KmerPointable extends AbstractPointable implements
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
			return new KmerPointable();
		}

		@Override
		public ITypeTraits getTypeTraits() {
			return TYPE_TRAITS;
		}
	};

	public static short getShortReverse(byte[] bytes, int offset, int length) {
		if (length < 2) {
			return (short) (bytes[offset] & 0xff);
		}
		return (short) (((bytes[offset + length - 1] & 0xff) << 8) + (bytes[offset
				+ length - 2] & 0xff));
	}

	public static int getIntReverse(byte[] bytes, int offset, int length) {
		int shortValue = getShortReverse(bytes, offset, length);

		if (length < 3) {
			return shortValue;
		}
		if (length == 3) {
			return (((bytes[offset + 2] & 0xff) << 16)
					+ ((bytes[offset + 1] & 0xff) << 8) + ((bytes[offset] & 0xff)));
		}
		return ((bytes[offset + length - 1] & 0xff) << 24)
				+ ((bytes[offset + length - 2] & 0xff) << 16)
				+ ((bytes[offset + length - 3] & 0xff) << 8)
				+ ((bytes[offset + length - 4] & 0xff) << 0);
	}

	public static long getLongReverse(byte[] bytes, int offset, int length) {
		if (length < 8) {
			return getIntReverse(bytes, offset, length);
		}
		return (((long) (bytes[offset + length - 1] & 0xff)) << 56)
				+ (((long) (bytes[offset + length - 2] & 0xff)) << 48)
				+ (((long) (bytes[offset + length - 3] & 0xff)) << 40)
				+ (((long) (bytes[offset + length - 4] & 0xff)) << 32)
				+ (((long) (bytes[offset + length - 5] & 0xff)) << 24)
				+ (((long) (bytes[offset + length - 6] & 0xff)) << 16)
				+ (((long) (bytes[offset + length - 7] & 0xff)) << 8)
				+ (((long) (bytes[offset + length - 8] & 0xff)) << 0);
	}

	@Override
	public int compareTo(IPointable pointer) {
		return compareTo(pointer.getByteArray(), pointer.getStartOffset(),
				pointer.getLength());
	}

	@Override
	public int compareTo(byte[] bytes, int offset, int length) {

		if (this.length != length) {
			return this.length - length;
		}
		
		// Why have we write so much ? 
		// We need to follow the normalized key and it's usage 
		int bNormKey = getIntReverse(this.bytes, this.start, this.length);
		int mNormKey = getIntReverse(bytes, offset, length);
		int cmp = bNormKey - mNormKey;
		if ( cmp != 0){
			return ((((long) bNormKey) & 0xffffffffL) < (((long) mNormKey) & 0xffffffffL)) ? -1
					: 1;
		}
		
		for (int i = length - 5; i >= 0; i--) {
			if (this.bytes[this.start + i] < bytes[offset + i]) {
				return -1;
			} else if (this.bytes[this.start + i] > bytes[offset + i]) {
				return 1;
			}
		}
		return 0;
	}

	@Override
	public int hash() {
		int hash = KmerHashPartitioncomputerFactory.hashBytes(bytes, start,
				length);
		return hash;
	}

	@Override
	public byte byteValue() {
		return bytes[start + length - 1];
	}

	@Override
	public short shortValue() {
		return getShortReverse(bytes, start, length);
	}

	@Override
	public int intValue() {
		return getIntReverse(bytes, start, length);
	}

	@Override
	public long longValue() {
		return getLongReverse(bytes, start, length);
	}

	@Override
	public float floatValue() {
		return Float.intBitsToFloat(intValue());
	}

	@Override
	public double doubleValue() {
		return Double.longBitsToDouble(longValue());
	}
}
