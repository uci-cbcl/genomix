package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.PositionWritable;

public class PositionListWritable implements Writable, Iterable<PositionWritable>, Serializable{
    private static final long serialVersionUID = 1L;
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
    
    public PositionListWritable(List<PositionWritable> posns) {
        this();
        setSize(posns.size());  // reserve space for all elements
        for (PositionWritable p : posns) {
            append(p);
        }
    }
    
    public void setNewReference(int count, byte[] data, int offset) {
        this.valueCount = count;
        this.storage = data;
        this.offset = offset;
    }
    
    public void append(long uuid) {
        setSize((1 + valueCount) * PositionWritable.LENGTH);
        Marshal.putLong(uuid, storage, offset + valueCount * PositionWritable.LENGTH);
        valueCount += 1;
    }
    
    public void append(byte mateId, long readId, int posId){
        append(PositionWritable.makeUUID(mateId, readId, posId));
    }
    
    public void append(PositionWritable pos) {
        if(pos != null)
            append(pos.getUUID());
        else
            throw new RuntimeException("This position is null pointer!");
    }
    
    /*
     * Append the otherList to the end of myList
     */
    public void appendList(PositionListWritable otherList) {
        if (otherList.valueCount > 0) {
            setSize((valueCount + otherList.valueCount) * PositionWritable.LENGTH);
            // copy contents of otherList into the end of my storage
            System.arraycopy(otherList.storage, otherList.offset,
                    storage, offset + valueCount * PositionWritable.LENGTH, 
                    otherList.valueCount * PositionWritable.LENGTH);
            valueCount += otherList.valueCount;
        }
    }
    
    /**
     * Save the union of my list and otherList. Uses a temporary HashSet for
     * uniquefication
     */
    public void unionUpdate(PositionListWritable otherList) {
        int newSize = valueCount + otherList.valueCount;
        HashSet<PositionWritable> uniqueElements = new HashSet<PositionWritable>(
                newSize);
        for (PositionWritable pos : this) {
            uniqueElements.add(pos);
        }
        for (PositionWritable pos : otherList) {
            uniqueElements.add(pos);
        }
        valueCount = 0;
        setSize(newSize);
        for (PositionWritable pos : uniqueElements) {
            append(pos);
        }
    }
    
    public static int getCountByDataLength(int length) {
        if (length % PositionWritable.LENGTH != 0) {
            throw new IllegalArgumentException("Length of positionlist is invalid");
        }
        return length / PositionWritable.LENGTH;
    }
    
    public void set(PositionListWritable otherList) {
        set(otherList.valueCount, otherList.storage, otherList.offset);
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
    
    public void resetPosition(int i, long uuid) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        Marshal.putLong(uuid, storage, offset + i * PositionWritable.LENGTH);
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
                if(currentIndex < valueCount)
                    System.arraycopy(storage, offset + currentIndex * PositionWritable.LENGTH, 
                          storage, offset + (currentIndex - 1) * PositionWritable.LENGTH, 
                          (valueCount - currentIndex) * PositionWritable.LENGTH);
                valueCount--;
                currentIndex--;
            }
        };
        return it;
    }
    
    /*
     * remove the first instance of @toRemove. Uses a linear scan.  Throws an exception if not in this list.
     */
    public void remove(PositionWritable toRemove, boolean ignoreMissing) {
        Iterator<PositionWritable> posIterator = this.iterator();
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
    
    public void remove(PositionWritable toRemove) {
    	remove(toRemove, false);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset, valueCount * PositionWritable.LENGTH);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        this.valueCount = in.readInt();
        setSize(valueCount * PositionWritable.LENGTH);
        in.readFully(storage, offset, valueCount * PositionWritable.LENGTH);
    }
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        for (PositionWritable pos : this) {
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
