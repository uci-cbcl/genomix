package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.Marshal;

public class PositionListWritable implements Writable, Iterable<PositionWritable> {
    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    protected static final byte[] EMPTY = {};

    protected PositionWritable posIter = new PositionWritable();

    public PositionListWritable() {
        this.storage = EMPTY;
        this.valueCount = 0;
        this.offset = 0;
    }

    public PositionListWritable(int count, byte[] data, int offset) {
        setNewReference(count, data, offset);
    }

    public void setNewReference(int count, byte[] data, int offset) {
        this.valueCount = count;
        this.storage = data;
        this.offset = offset;
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
    }

    protected int getCapacity() {
        return storage.length - offset;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap > getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (storage.length - offset > 0) {
                System.arraycopy(storage, offset, new_data, 0, storage.length - offset);
            }
            storage = new_data;
            offset = 0;
        }
    }

    public PositionWritable getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setNewReference(storage, offset + i * PositionWritable.LENGTH);
        return posIter;
    }

    @Override
    public Iterator<PositionWritable> iterator() {
        Iterator<PositionWritable> it = new Iterator<PositionWritable>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < valueCount;
            }

            @Override
            public PositionWritable next() {
                return getPosition(currentIndex++);
            }

            @Override
            public void remove() {
                // TODO Auto-generated method stub
            }
        };
        return it;
    }

    public void set(PositionListWritable list2) {
        set(list2.valueCount, list2.storage, list2.offset);
    }

    public void set(int valueCount, byte[] newData, int offset) {
        this.valueCount = valueCount;
        setSize(valueCount * PositionWritable.LENGTH);
        if (valueCount > 0) {
            System.arraycopy(newData, offset, storage, this.offset, valueCount * PositionWritable.LENGTH);
        }
    }

    public void reset() {
        valueCount = 0;
    }

    public void append(PositionWritable pos) {
        setSize((1 + valueCount) * PositionWritable.LENGTH);
        System.arraycopy(pos.getByteArray(), pos.getStartOffset(), storage, offset + valueCount
                * PositionWritable.LENGTH, pos.getLength());
        valueCount += 1;
    }

    public void append(int readID, byte posInRead) {
        setSize((1 + valueCount) * PositionWritable.LENGTH);
        Marshal.putInt(readID, storage, offset + valueCount * PositionWritable.LENGTH);
        storage[offset + valueCount * PositionWritable.LENGTH + PositionWritable.INTBYTES] = posInRead;
        valueCount += 1;
    }

    public int getCountOfPosition() {
        return valueCount;
    }

    public byte[] getByteArray() {
        return storage;
    }

    public int getStartOffset() {
        return offset;
    }

    public int getLength() {
        return valueCount * PositionWritable.LENGTH;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.valueCount = in.readInt();
        setSize(valueCount * PositionWritable.LENGTH);
        in.readFully(storage, offset, valueCount * PositionWritable.LENGTH);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset, valueCount * PositionWritable.LENGTH);
    }

    @Override
    public String toString(){
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        for(PositionWritable pos : this){
            sbuilder.append(pos.toString());
            sbuilder.append(',');
        }
        sbuilder.setCharAt(sbuilder.length()-1, ']');
        return sbuilder.toString();
    }
}
