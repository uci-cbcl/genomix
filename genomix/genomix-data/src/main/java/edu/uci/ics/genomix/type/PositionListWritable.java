package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.PositionWritable;

public class PositionListWritable extends TreeSet<PositionWritable> implements Writable, Serializable {
    private static final long serialVersionUID = 1L;
    protected static final byte[] EMPTY_BYTES = {0,0,0,0};
    protected static final int HEADER_SIZE = 4;
    
    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    protected int maxStorageSize;


    protected PositionWritable posIter = new PositionWritable();

    public PositionListWritable() {
        storage = EMPTY_BYTES;
        valueCount = 0;
        offset = 0;
        maxStorageSize = storage.length;
    }

    public PositionListWritable(byte[] data, int offset) {
        setAsReference(data, offset);
    }

    public PositionListWritable(List<PositionWritable> posns) {
        this();
        setSize(posns.size() * PositionWritable.LENGTH + HEADER_SIZE); // reserve space for all elements
        for (PositionWritable p : posns) {
            append(p);
        }
    }
  
    public PositionListWritable(PositionListWritable other) {
        this();
        set(other);
    }

    public void setAsReference(byte[] data, int offset) {
        this.valueCount = Marshal.getInt(data, offset);
        this.storage = data;
        this.offset = offset;
        maxStorageSize = valueCount * PositionWritable.LENGTH + HEADER_SIZE;
    }

    public void append(long uuid) {
        setSize((1 + valueCount) * PositionWritable.LENGTH + HEADER_SIZE);
        Marshal.putLong(uuid, storage, offset + valueCount * PositionWritable.LENGTH + HEADER_SIZE);
        valueCount += 1;
        Marshal.putInt(valueCount, storage, offset);
    }

    public void append(byte mateId, long readId, int posId) {
        append(PositionWritable.makeUUID(mateId, readId, posId));
    }

    public void append(PositionWritable pos) {
        if (pos != null)
            append(pos.getUUID());
        else
            throw new RuntimeException("This position is null pointer!");
    }
    
    public void appendReadId(long readId){
        append((byte)0, readId, 0);
    }

    /*
     * Append the otherList to the end of myList
     */
    public void appendList(PositionListWritable otherList) {
        if (otherList.valueCount > 0) {
            setSize((valueCount + otherList.valueCount) * PositionWritable.LENGTH + HEADER_SIZE);
            // copy contents of otherList into the end of my storage
            System.arraycopy(otherList.storage, otherList.offset + HEADER_SIZE, storage, offset + valueCount
                    * PositionWritable.LENGTH + HEADER_SIZE, otherList.valueCount * PositionWritable.LENGTH);
            valueCount += otherList.valueCount;
            Marshal.putInt(valueCount, storage, offset);
        }
    }

    /**
     * Save the union of my list and otherList. Uses a temporary HashSet for
     * uniquefication
     */
    public void unionUpdate(PositionListWritable otherList) {
        int newSize = valueCount + otherList.valueCount;
        HashSet<PositionWritable> uniqueElements = new HashSet<PositionWritable>(newSize);
        for (PositionWritable pos : this) {
            uniqueElements.add(new PositionWritable(pos));
        }
        for (PositionWritable pos : otherList) {
            uniqueElements.add(new PositionWritable(pos));
        }
        valueCount = 0;
        setSize(newSize * PositionWritable.LENGTH + HEADER_SIZE);
        for (PositionWritable pos : uniqueElements) {
            append(pos);
        }
    }
    
    /**
     * version of unionUpdate that imposes a maximum number on the number of positions added
     */
    public void unionUpdateCappedCount(PositionListWritable otherList) {
        HashSet<PositionWritable> uniqueElements = new HashSet<PositionWritable>(valueCount + otherList.valueCount);
        for (PositionWritable pos : this) {
//            if (uniqueElements.size() < EdgeWritable.MAX_READ_IDS_PER_EDGE)
                uniqueElements.add(new PositionWritable(pos));
        }
        for (PositionWritable pos : otherList) {
//            if (uniqueElements.size() < EdgeWritable.MAX_READ_IDS_PER_EDGE)
                uniqueElements.add(new PositionWritable(pos));
        }
        valueCount = 0;
        setSize(uniqueElements.size() * PositionWritable.LENGTH + HEADER_SIZE);
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
        setAsCopy(otherList.storage, otherList.offset);
    }

    public void setAsCopy(byte[] newData, int newOffset) {
        int newValueCount = Marshal.getInt(newData, newOffset);
        setSize(newValueCount * PositionWritable.LENGTH + HEADER_SIZE);
        if (newValueCount > 0) {
            System.arraycopy(newData, newOffset + HEADER_SIZE, storage, this.offset + HEADER_SIZE, newValueCount * PositionWritable.LENGTH);
        }
        valueCount = newValueCount;
        Marshal.putInt(valueCount, storage, this.offset);
    }

    public void reset() {
        valueCount = 0;
        Marshal.putInt(valueCount, storage, offset);
    }

    protected void setSize(int size) {
        if (size > getCapacity()) {
            setCapacity((size * 3 / 2));
        }
    }

    protected int getCapacity() {
        return maxStorageSize - offset;
    }
    
    public String printStartReadIdSet(){
        String output = "5':[";
        if(valueCount > 0){
            for(int i = 0; i < valueCount - 1; i++)
                output += getPosition(i).getReadId() + ",";
            output += getPosition(valueCount - 1).getReadId();
        }
        output += "]";
        return output;
    }
    
    public String printEndReadIdSet(){
        String output = "~5':[";
        if(valueCount > 0){
            for(int i = 0; i < valueCount - 1; i++)
                output += getPosition(i).getReadId() + ",";
            output += getPosition(valueCount - 1).getReadId();
        }
        output += "]";
        return output;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap > getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (valueCount > 0) {
                System.arraycopy(storage, offset, new_data, 0, valueCount * PositionWritable.LENGTH + HEADER_SIZE);
            }
            storage = new_data;
            offset = 0;
            maxStorageSize = storage.length;
        }
    }

    public PositionWritable getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setNewReference(storage, offset + i * PositionWritable.LENGTH + HEADER_SIZE);
        return posIter;
    }

    public void resetPosition(int i, long uuid) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        Marshal.putLong(uuid, storage, offset + i * PositionWritable.LENGTH + HEADER_SIZE);
    }
    
    public void removePosition(int i) {
        if (i < 0 || i > valueCount)
            throw new IllegalArgumentException("Invalid position specified in removePosition! Should be 0 <= " + i + " <= " + valueCount + ").");
        System.arraycopy(storage, offset + i * PositionWritable.LENGTH + HEADER_SIZE, storage, offset
                + (i - 1) * PositionWritable.LENGTH + HEADER_SIZE, (valueCount - i)
                * PositionWritable.LENGTH);
        valueCount--;
        Marshal.putInt(valueCount, storage, offset);
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

    public int getLengthInBytes() {
        return valueCount * PositionWritable.LENGTH + HEADER_SIZE;
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
                if (currentIndex < valueCount)
                    System.arraycopy(storage, offset + currentIndex * PositionWritable.LENGTH + HEADER_SIZE, storage, offset
                            + (currentIndex - 1) * PositionWritable.LENGTH + HEADER_SIZE, (valueCount - currentIndex)
                            * PositionWritable.LENGTH);
                valueCount--;
                currentIndex--;
                Marshal.putInt(valueCount, storage, offset);
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
            if (toRemove.equals(posIterator.next())) {
                posIterator.remove();
                return;  // found it. return early. 
            }
        }
        // element not found.
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the PositionWritable `" + toRemove.toString()
                    + "` was not found in this list.");
        }
    }

    public void remove(PositionWritable toRemove) {
        remove(toRemove, false);
    }
    
    public void removeReadId(long readId){
        PositionWritable toRemove = new PositionWritable();
        toRemove.set((byte)0, readId, 0);
        remove(toRemove);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset + HEADER_SIZE, valueCount * PositionWritable.LENGTH);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int newValueCount = in.readInt();
        setSize(newValueCount * PositionWritable.LENGTH + HEADER_SIZE);
        in.readFully(storage, offset + HEADER_SIZE, newValueCount * PositionWritable.LENGTH);
        valueCount = newValueCount;
        Marshal.putInt(valueCount, storage, offset);
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        long[] ids = toUUIDArray();
        Arrays.sort(ids);
        PositionWritable posn = new PositionWritable();
        String delim = "";
        for (long p : ids) {
            posn.set(p);
            sbuilder.append(delim).append(posn.toString());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString();
    }
    
    public long[] toUUIDArray() {
        long[] result = new long[valueCount];
        for (int i=0; i < valueCount; i++) {
            result[i] = getPosition(i).getUUID();
        }
        return result;
    }
    
    public long[] toReadIDArray() {
        long[] result = new long[valueCount];
        for (int i=0; i < valueCount; i++) {
            result[i] = getPosition(i).getReadId();
        }
        return result;
    }
    
    public Set<Long> getSetOfReadIds(){
        Set<Long> readIds = new HashSet<Long>();
        for(long readId : toReadIDArray()){
            readIds.add(readId);
        }
        return readIds;
    }
    
    @Override
    public int hashCode() {
        return Marshal.hashBytes(getByteArray(), getStartOffset(), getLengthInBytes());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PositionListWritable))
            return false;
        PositionListWritable other = (PositionListWritable) o;
        if (this.valueCount != other.valueCount)
            return false;
        for (int i = 0; i < this.valueCount; i++) {
            if (!this.getPosition(i).equals(other.getPosition(i)))
                return false;
        }
        return true;
    }
    
    public boolean isEmpty(){
        return this.getCountOfPosition() == 0;
    }
    
    public static PositionListWritable getIntersection(PositionListWritable list1, PositionListWritable list2){
        PositionListWritable intersection = new PositionListWritable(list1);
        intersection.retainAll(list2);
        return intersection;
    }
}
