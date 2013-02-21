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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This class override the writablecomparable class which contain int varable
 */
public class ValueWritable implements WritableComparable<ValueWritable> {
    private byte first;
    private byte second;

    public ValueWritable() {
    }

    public ValueWritable(byte first, byte second) {
        set(first, second);
    }

    public void set(byte first, byte second) {
        this.first = first;
        this.second = second;
    }

    public byte getFirst() {
        return first;
    }

    public byte getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(first);
        out.writeByte(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readByte();
        second = in.readByte();
    }

    @Override
    public int hashCode() {
        return (int) first + (int) second;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ValueWritable) {
            ValueWritable tp = (ValueWritable) o;
            return first == tp.first && second == tp.second;
        }
        return false;
    }

    @Override
    public String toString() {
        return Integer.toString(first) + "\t" + Integer.toString(second);
    }

    @Override
    public int compareTo(ValueWritable tp) {
        int cmp;
        if (first == tp.first)
            cmp = 0;
        else
            cmp = 1;
        if (cmp != 0)
            return cmp;
        if (second == tp.second)
            return 0;
        else
            return 1;
    }

}
