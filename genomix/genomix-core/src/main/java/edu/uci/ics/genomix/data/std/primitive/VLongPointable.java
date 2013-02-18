package edu.uci.ics.genomix.data.std.primitive;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public final class VLongPointable extends AbstractPointable implements
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
			return new VLongPointable();
		}

		@Override
		public ITypeTraits getTypeTraits() {
			return TYPE_TRAITS;
		}
	};

	public static long getLong(byte[] bytes, int start) {
		int l = (int) Math.ceil((double) bytes[start] / 4.0);
		int n = (l < 8) ? l : 8;

		long r = 0;
		for (int i = 0; i < n; i++) {
			r <<= 8;
			r += (long) (bytes[start + 1] & 0xff);
		}

		return r;
	}

	public long getLong() {
		return getLong(bytes, start);
	}

	public byte[] postIncrement() {
		int i = start + 1;
		int l = (int) Math.ceil(bytes[start] / 4);
		while (i <= start + l) {
			bytes[i] += 1;
			if (bytes[i] != 0) {
				break;
			}
			i += 1;
		}
		return bytes;
	}

	@Override
	public int compareTo(IPointable pointer) {
		return compareTo(pointer.getByteArray(), pointer.getStartOffset(),
				pointer.getLength());
	}

	@Override
	public int compareTo(byte[] bytes, int start, int length) {

		int be = this.start;
		
		if(this.bytes[be] != bytes[start]){
			return (this.bytes[be] < bytes[start]) ? -1 : 1;
		}
		
		int n = this.bytes[be];
		int l = (int) Math.ceil(n / 4);
		for (int i = 0; i <= l; i++) {
			if (this.bytes[be + i] < bytes[start + i]) {
				return -1;
			} else if (this.bytes[be + i] > bytes[start + i]) {
				return 1;
			}

		}
		return 0;
	}

	@Override
	public int hash() {// BKDRHash
		int hash = 1;
		for (int i = start + 1; i <= start + length; i++)
			hash = (31 * hash) + (int)bytes[i];
		if(hash < 0){
			hash = -hash;
		}
		//System.err.println(hash);
		return hash;
/*		int seed = 131; // 31 131 1313 13131 131313 etc..
		int hash = 0;
		int l = (int) Math.ceil((double) bytes[start] / 4.0);
		for (int i = start + 1; i <= start + l; i++) {
			hash = hash * seed + bytes[i];
		}
		return (hash & 0x7FFFFFFF);*/
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
