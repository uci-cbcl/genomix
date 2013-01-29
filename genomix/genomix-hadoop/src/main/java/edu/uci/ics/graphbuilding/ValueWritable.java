package edu.uci.ics.graphbuilding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ValueWritable implements WritableComparable<ValueWritable> {
    private int first;
    private int second;

    public ValueWritable() {
    }

    public ValueWritable(int first, int second) {
        set(first, second);
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    public int hashCode() {
        return first + second;
    }

    public boolean equals(Object o) {
        if (o instanceof ValueWritable) {
            ValueWritable tp = (ValueWritable) o;
            return first == tp.first && second == tp.second;
        }
        return false;
    }

    public String toString() {
        return Integer.toString(first) + "\t" + Integer.toString(second);
    }

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
