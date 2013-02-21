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
package edu.uci.ics.graphbuilding;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ValueBytesWritable extends BinaryComparable implements WritableComparable<BinaryComparable> {
    private static final int LENGTH_BYTES = 4;
    private static final byte[] EMPTY_BYTES = {};
    private byte size;
    private byte[] bytes;

    public ValueBytesWritable() {
        this(EMPTY_BYTES);
    }

    public ValueBytesWritable(byte[] bytes) {
        this.bytes = bytes;
        this.size = (byte) bytes.length;
    }

    @Override
    public byte[] getBytes() {
        return bytes;
    }

    @Deprecated
    public byte[] get() {
        return getBytes();
    }

    @Override
    public int getLength() {
        return (int) size;
    }

    @Deprecated
    public int getSize() {
        return getLength();
    }

    public void setSize(byte size) {
        if ((int) size > getCapacity()) {
            setCapacity((byte) (size * 3 / 2));
        }
        this.size = size;
    }

    public int getCapacity() {
        return bytes.length;
    }

    public void setCapacity(byte new_cap) {
        if (new_cap != getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (new_cap < size) {
                size = new_cap;
            }
            if (size != 0) {
                System.arraycopy(bytes, 0, new_data, 0, size);
            }
            bytes = new_data;
        }
    }

    public void set(ValueBytesWritable newData) {
        set(newData.bytes, (byte) 0, newData.size);
    }

    public void set(byte[] newData, byte offset, byte length) {
        setSize((byte) 0);
        setSize(length);
        System.arraycopy(newData, offset, bytes, 0, size);
    }

    public void readFields(DataInput in) throws IOException {
        setSize((byte) 0); // clear the old data
        setSize(in.readByte());
        in.readFully(bytes, 0, size);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(size);
        out.write(bytes, 0, size);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object right_obj) {
        if (right_obj instanceof ValueBytesWritable)
            return super.equals(right_obj);
        return false;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer(3 * size);
        for (int idx = 0; idx < (int) size; idx++) {
            // if not the first, put a blank separator in
            if (idx != 0) {
                sb.append(' ');
            }
            String num = Integer.toHexString(0xff & bytes[idx]);
            // if it is only one digit, add a leading 0.
            if (num.length() < 2) {
                sb.append('0');
            }
            sb.append(num);
        }
        return sb.toString();
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(ValueBytesWritable.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1 + LENGTH_BYTES, l1 - LENGTH_BYTES, b2, s2 + LENGTH_BYTES, l2 - LENGTH_BYTES);
        }
    }

    static { // register this comparator
        WritableComparator.define(ValueBytesWritable.class, new Comparator());
    }
}
