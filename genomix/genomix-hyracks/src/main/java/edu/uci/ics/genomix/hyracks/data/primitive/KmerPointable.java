/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.hyracks.data.primitive;

import edu.uci.ics.genomix.hyracks.data.accessors.KmerHashPartitioncomputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.api.AbstractPointable;
import edu.uci.ics.hyracks.data.std.api.IComparable;
import edu.uci.ics.hyracks.data.std.api.IHashable;
import edu.uci.ics.hyracks.data.std.api.INumeric;
import edu.uci.ics.hyracks.data.std.api.IPointable;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;

public final class KmerPointable extends AbstractPointable implements IHashable, IComparable, INumeric {
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
        return (short) (((bytes[offset + length - 1] & 0xff) << 8) + (bytes[offset + length - 2] & 0xff));
    }

    public static int getIntReverse(byte[] bytes, int offset, int length) {
        int shortValue = getShortReverse(bytes, offset, length) & 0xffff;

        if (length < 3) {
            return shortValue;
        }
        if (length == 3) {
            return (((bytes[offset + 2] & 0xff) << 16) + ((bytes[offset + 1] & 0xff) << 8) + ((bytes[offset] & 0xff)));
        }
        return ((bytes[offset + length - 1] & 0xff) << 24) + ((bytes[offset + length - 2] & 0xff) << 16)
                + ((bytes[offset + length - 3] & 0xff) << 8) + ((bytes[offset + length - 4] & 0xff) << 0);
    }

    public static long getLongReverse(byte[] bytes, int offset, int length) {
        if (length < 8) {
            return ((long) getIntReverse(bytes, offset, length)) & 0x0ffffffffL;
        }
        return (((long) (bytes[offset + length - 1] & 0xff)) << 56)
                + (((long) (bytes[offset + length - 2] & 0xff)) << 48)
                + (((long) (bytes[offset + length - 3] & 0xff)) << 40)
                + (((long) (bytes[offset + length - 4] & 0xff)) << 32)
                + (((long) (bytes[offset + length - 5] & 0xff)) << 24)
                + (((long) (bytes[offset + length - 6] & 0xff)) << 16)
                + (((long) (bytes[offset + length - 7] & 0xff)) << 8) + (((long) (bytes[offset + length - 8] & 0xff)));
    }

    @Override
    public int compareTo(IPointable pointer) {
        return compareTo(pointer.getByteArray(), pointer.getStartOffset(), pointer.getLength());
    }

    @Override
    public int compareTo(byte[] bytes, int offset, int length) {

        if (this.length != length) {
            return this.length - length;
        }
        for (int i = length - 1; i >= 0; i--) {
            int cmp = (this.bytes[this.start + i] & 0xff) - (bytes[offset + i] & 0xff);
            if (cmp != 0) {
                return cmp;
            }
        }

        return 0;
    }

    @Override
    public int hash() {
        int hash = KmerHashPartitioncomputerFactory.hashBytes(bytes, start, length);
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
