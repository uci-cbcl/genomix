package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.Writable;
import edu.uci.ics.genomix.data.Marshal;
public class ReadIDListWritable implements Writable, Iterable<ReadIDWritable>, Serializable{
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    protected static final byte[] EMPTY = {};
    
    protected ReadIDWritable posIter = new ReadIDWritable();
    
    public ReadIDListWritable() {
        this.storage = EMPTY;
        this.valueCount = 0;
        this.offset = 0;
    }
    
    public ReadIDListWritable(int count, byte[] data, int offset) {
        setNewReference(count, data, offset);
    }
    
    public ReadIDListWritable(List<ReadIDWritable> posns) {
        this();
        setSize(posns.size());  // reserve space for all elements
        for (ReadIDWritable p : posns) {
            append(p);
        }
    }
    
    public void setNewReference(int count, byte[] data, int offset) {
        this.valueCount = count;
        this.storage = data;
        this.offset = offset;
    }
    
    public void append(long readIDByte) {
        setSize((1 + valueCount) * ReadIDWritable.LENGTH);
        Marshal.putLong(readIDByte, storage, offset + valueCount * ReadIDWritable.LENGTH);
        valueCount += 1;
    }
    
    public void append(byte mateId, long readId){
        append(ReadIDWritable.serialize(mateId, readId));
    }
    
    public void append(ReadIDWritable pos) {
        if(pos != null)
            append(pos.deserialize());
        else
            throw new RuntimeException("This position is null pointer!");
    }
    
    /*
     * Append the otherList to the end of myList
     */
    public void appendList(ReadIDListWritable otherList) {
        if (otherList.valueCount > 0) {
            setSize((valueCount + otherList.valueCount) * ReadIDWritable.LENGTH);
            // copy contents of otherList into the end of my storage
            System.arraycopy(otherList.storage, otherList.offset,
                    storage, offset + valueCount * ReadIDWritable.LENGTH, 
                    otherList.valueCount * ReadIDWritable.LENGTH);
            valueCount += otherList.valueCount;
        }
    }
    
    public static int getCountByDataLength(int length) {
        if (length % ReadIDWritable.LENGTH != 0) {
            throw new IllegalArgumentException("Length of positionlist is invalid");
        }
        return length / ReadIDWritable.LENGTH;
    }
    
    public void set(ReadIDListWritable otherList) {
        set(otherList.valueCount, otherList.storage, otherList.offset);
    }

    public void set(int valueCount, byte[] newData, int offset) {
        this.valueCount = valueCount;
        setSize(valueCount * ReadIDWritable.LENGTH);
        if (valueCount > 0) {
            System.arraycopy(newData, offset, storage, this.offset, valueCount * ReadIDWritable.LENGTH);
        }
    }

    public void reset() {
        valueCount = 0;
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
    
    public ReadIDWritable getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setNewReference(storage, offset + i * ReadIDWritable.LENGTH);
        return posIter;
    }
    
    public void resetPosition(int i, long readIDByte) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        Marshal.putLong(readIDByte, storage, offset + i * ReadIDWritable.LENGTH);
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
        return valueCount * ReadIDWritable.LENGTH;
    }
    
    @Override
    public Iterator<ReadIDWritable> iterator() {
        Iterator<ReadIDWritable> it = new Iterator<ReadIDWritable>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < valueCount;
            }

            @Override
            public ReadIDWritable next() {
                return getPosition(currentIndex++);
            }

            @Override
            public void remove() {
                if(currentIndex < valueCount)
                    System.arraycopy(storage, offset + currentIndex * ReadIDWritable.LENGTH, 
                          storage, offset + (currentIndex - 1) * ReadIDWritable.LENGTH, 
                          (valueCount - currentIndex) * ReadIDWritable.LENGTH);
                valueCount--;
                currentIndex--;
            }
        };
        return it;
    }
    
    /*
     * remove the first instance of @toRemove. Uses a linear scan.  Throws an exception if not in this list.
     */
    public void remove(ReadIDWritable toRemove, boolean ignoreMissing) {
        Iterator<ReadIDWritable> posIterator = this.iterator();
        while (posIterator.hasNext()) {
            if(toRemove.equals(posIterator.next())) {
                posIterator.remove();
                return;
            }
        }
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the PositionWritable `" + toRemove.toString() + "` was not found in this list.");
        }
    }
    
    public void remove(ReadIDWritable toRemove) {
        remove(toRemove, false);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset, valueCount * ReadIDWritable.LENGTH);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.valueCount = in.readInt();
        setSize(valueCount * ReadIDWritable.LENGTH);
        in.readFully(storage, offset, valueCount * ReadIDWritable.LENGTH);
    }
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        for (ReadIDWritable pos : this) {
            sbuilder.append(pos.toString());
            sbuilder.append(',');
        }
        if (valueCount > 0) {
            sbuilder.setCharAt(sbuilder.length() - 1, ']');
        } else {
            sbuilder.append(']');
        }
        return sbuilder.toString();
    }
    
    @Override
    public int hashCode() {
        return Marshal.hashBytes(getByteArray(), getStartOffset(), getLength());
    }
    
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PositionListWritable))
            return false;
        PositionListWritable other = (PositionListWritable) o;
        if (this.valueCount != other.valueCount)
            return false;
        for (int i=0; i < this.valueCount; i++) {
                if (!this.getPosition(i).equals(other.getPosition(i)))
                    return false;
        }
        return true;
    }
}
