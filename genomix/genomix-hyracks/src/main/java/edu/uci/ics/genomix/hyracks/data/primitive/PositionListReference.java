package edu.uci.ics.genomix.hyracks.data.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class PositionListReference implements Writable, IValueReference {
    private byte[] values;
    private int valueCount;
    private static final byte[] EMPTY = {};

    private PositionReference posIter = new PositionReference();

    public PositionListReference() {
        this.values = EMPTY;
        this.valueCount = 0;
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
    }

    protected int getCapacity() {
        return values.length;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap > getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (values.length > 0) {
                System.arraycopy(values, 0, new_data, 0, values.length);
            }
            values = new_data;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.valueCount = in.readInt();
        setSize(valueCount * PositionReference.LENGTH);
        in.readFully(values, 0, valueCount * PositionReference.LENGTH);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(values, 0, valueCount * PositionReference.LENGTH);
    }

    public PositionReference getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("Not so much positions");
        }
        posIter.setNewSpace(values, i * PositionReference.LENGTH);
        return posIter;
    }

    public void set(PositionListReference list2) {
        set(list2.valueCount, list2.values, 0);
    }

    public void set(int valueCount, byte[] newData, int offset) {
        this.valueCount = valueCount;
        setSize(valueCount * PositionReference.LENGTH);
        if (valueCount > 0) {
            System.arraycopy(newData, offset, values, 0, valueCount * PositionReference.LENGTH);
        }
    }

    public void reset() {
        valueCount = 0;
    }

    public void append(PositionReference pos) {
        setSize((1 + valueCount) * PositionReference.LENGTH);
        System.arraycopy(pos.getByteArray(), pos.getStartOffset(), values, valueCount * PositionReference.LENGTH,
                pos.getLength());
        valueCount += 1;
    }

    public void append(int readID, byte posInRead) {
        setSize((1 + valueCount) * PositionReference.LENGTH);
        IntegerSerializerDeserializer.putInt(readID, values, valueCount * PositionReference.LENGTH);
        values[valueCount * PositionReference.LENGTH + PositionReference.INTBYTES] = posInRead;
        valueCount += 1;
    }

    public int getPositionCount() {
        return valueCount;
    }

    @Override
    public byte[] getByteArray() {
        return values;
    }

    @Override
    public int getStartOffset() {
        return 0;
    }

    @Override
    public int getLength() {
        return valueCount * PositionReference.LENGTH;
    }

}
