package edu.uci.ics.genomix.hyracks.data.primitive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.hyracks.data.std.api.IValueReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class PositionListReference implements Writable, Iterable<PositionReference>, IValueReference {
    private byte[] storage;
    private int offset;
    private int valueCount;
    private static final byte[] EMPTY = {};

    private PositionReference posIter = new PositionReference();

    public PositionListReference() {
        this.storage = EMPTY;
        this.valueCount = 0;
        this.offset = 0;
    }
    
    public PositionListReference(int count, byte [] data, int offset){
        setNewReference(count, data, offset);
    }

    public void setNewReference(int count, byte[] data, int offset){
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
                System.arraycopy(storage, offset, new_data, 0, storage.length-offset);
            }
            storage = new_data;
            offset = 0;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.valueCount = in.readInt();
        setSize(valueCount * PositionReference.LENGTH);
        in.readFully(storage, offset, valueCount * PositionReference.LENGTH);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset, valueCount * PositionReference.LENGTH);
    }

    public PositionReference getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setNewReference(storage, offset + i * PositionReference.LENGTH);
        return posIter;
    }
    
    @Override
    public Iterator<PositionReference> iterator() {
        Iterator<PositionReference> it = new Iterator<PositionReference>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < valueCount;
            }

            @Override
            public PositionReference next() {
                return getPosition(currentIndex);
            }

            @Override
            public void remove() {
                // TODO Auto-generated method stub
            }
        };
        return it;
    }

    public void set(PositionListReference list2) {
        set(list2.valueCount, list2.storage, list2.offset);
    }

    public void set(int valueCount, byte[] newData, int offset) {
        this.valueCount = valueCount;
        setSize(valueCount * PositionReference.LENGTH);
        if (valueCount > 0) {
            System.arraycopy(newData, offset, storage, this.offset, valueCount * PositionReference.LENGTH);
        }
    }

    public void reset() {
        valueCount = 0;
    }

    public void append(PositionReference pos) {
        setSize((1 + valueCount) * PositionReference.LENGTH);
        System.arraycopy(pos.getByteArray(), pos.getStartOffset(), storage, offset + valueCount * PositionReference.LENGTH,
                pos.getLength());
        valueCount += 1;
    }

    public void append(int readID, byte posInRead) {
        setSize((1 + valueCount) * PositionReference.LENGTH);
        IntegerSerializerDeserializer.putInt(readID, storage, offset + valueCount * PositionReference.LENGTH);
        storage[offset + valueCount * PositionReference.LENGTH + PositionReference.INTBYTES] = posInRead;
        valueCount += 1;
    }

    public int getCountOfPosition() {
        return valueCount;
    }

    @Override
    public byte[] getByteArray() {
        return storage;
    }

    @Override
    public int getStartOffset() {
        return offset;
    }

    @Override
    public int getLength() {
        return valueCount * PositionReference.LENGTH ;
    }

}
