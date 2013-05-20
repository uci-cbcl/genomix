package edu.uci.ics.genomix.hyracks.data.primitive;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;

public class ReadIDPointable  extends AbstractPointable implements IHashable, IComparable, INumeric {
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
            return new IntegerPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static int getInteger(byte[] bytes, int start) {
        return Marshal.getInt(bytes, start);
    }

    public static void setInteger(byte[] bytes, int start, int value) {
        Marshal.putInt(value, bytes, start);
    }

    public int getInteger() {
        return getInteger(bytes, start);
    }

    public void setInteger(int value) {
        setInteger(bytes, start, value);
    }

    public int preIncrement() {
        int v = getInteger();
        ++v;
        setInteger(v);
        return v;
    }

    public int postIncrement() {
        int v = getInteger();
        int ov = v++;
        setInteger(v);
        return ov;
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int start, int length) {
        int v = getInteger();
        int ov = getInteger(bytes, start);
        return v < ov ? -1 : (v > ov ? 1 : 0);
    }

    @Override
    public int hash() {
        return getInteger();
    }

    @Override
    public byte byteValue() {
        return (byte) getInteger();
    }

    @Override
    public short shortValue() {
        return (short) getInteger();
    }

    @Override
    public int intValue() {
        return getInteger();
    }

    @Override
    public long longValue() {
        return getInteger();
    }

    @Override
    public float floatValue() {
        return getInteger();
    }

    @Override
    public double doubleValue() {
        return getInteger();
    }
}
