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

import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.util.Marshal;

public class ReadHeadSet extends TreeSet<ReadHeadInfo> implements Writable, Serializable {
    private static final long serialVersionUID = 1L;
    protected static final byte[] EMPTY_BYTES = { 0, 0, 0, 0 };
    protected static final int HEADER_SIZE = 4;

    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    protected int maxStorageSize;

    protected ReadHeadInfo posIter = new ReadHeadInfo();

    public ReadHeadSet() {
        storage = EMPTY_BYTES;
        valueCount = 0;
        offset = 0;
        maxStorageSize = storage.length;
    }
    
    public ReadHeadSet(byte[] data, int offset) {
        setAsReference(data, offset);
    }
    
    public ReadHeadInfo getReadHeadInfoFromReadId(long readId){
        for(ReadHeadInfo readHeadInfo : this){
            if(readHeadInfo.getReadId() == readId)
                return readHeadInfo;
        }
        return null;
    }
    
    public ReadHeadSet(List<ReadHeadInfo> posns) {
        this();
        setSize(posns.size() * ReadHeadInfo.LENGTH + HEADER_SIZE); // reserve space for all elements
        for (ReadHeadInfo p : posns) {
            append(p);
        }
    }

    public ReadHeadSet(ReadHeadSet other) {
        this();
        set(other);
    }

    public void setAsReference(byte[] data, int offset) {
        this.valueCount = Marshal.getInt(data, offset);
        this.storage = data;
        this.offset = offset;
        maxStorageSize = valueCount * ReadHeadInfo.LENGTH + HEADER_SIZE;
    }

    public void append(long uuid) {
        setSize((1 + valueCount) * ReadHeadInfo.LENGTH + HEADER_SIZE);
        Marshal.putLong(uuid, storage, offset + valueCount * ReadHeadInfo.LENGTH + HEADER_SIZE);
        valueCount += 1;
        Marshal.putInt(valueCount, storage, offset);
    }

    public void append(byte mateId, long readId, int posId) {
        append(ReadHeadInfo.makeUUID(mateId, readId, posId));
    }

    public void append(ReadHeadInfo pos) {
        if (pos != null)
            append(pos.getUUID());
        else
            throw new RuntimeException("This position is null pointer!");
    }

    public void appendReadId(long readId) {
        append((byte) 0, readId, 0);
    }

    /*
     * Append the otherList to the end of myList
     */
    public void appendList(ReadHeadSet otherList) {
        if (otherList.valueCount > 0) {
            setSize((valueCount + otherList.valueCount) * ReadHeadInfo.LENGTH + HEADER_SIZE);
            // copy contents of otherList into the end of my storage
            System.arraycopy(otherList.storage, otherList.offset + HEADER_SIZE, storage, offset + valueCount
                    * ReadHeadInfo.LENGTH + HEADER_SIZE, otherList.valueCount * ReadHeadInfo.LENGTH);
            valueCount += otherList.valueCount;
            Marshal.putInt(valueCount, storage, offset);
        }
    }

    /**
     * Save the union of my list and otherList. Uses a temporary HashSet for
     * uniquefication
     */
    public void unionUpdate(ReadHeadSet otherList) {
        int newSize = valueCount + otherList.valueCount;
        HashSet<ReadHeadInfo> uniqueElements = new HashSet<ReadHeadInfo>(newSize);
        for (ReadHeadInfo pos : this) {
            uniqueElements.add(new ReadHeadInfo(pos));
        }
        for (ReadHeadInfo pos : otherList) {
            uniqueElements.add(new ReadHeadInfo(pos));
        }
        valueCount = 0;
        setSize(newSize * ReadHeadInfo.LENGTH + HEADER_SIZE);
        for (ReadHeadInfo pos : uniqueElements) {
            append(pos);
        }
    }

    /**
     * version of unionUpdate that imposes a maximum number on the number of positions added
     */
    public void unionUpdateCappedCount(ReadHeadSet otherList) {
        HashSet<ReadHeadInfo> uniqueElements = new HashSet<ReadHeadInfo>(valueCount + otherList.valueCount);
        for (ReadHeadInfo pos : this) {
            //            if (uniqueElements.size() < EdgeWritable.MAX_READ_IDS_PER_EDGE)
            uniqueElements.add(new ReadHeadInfo(pos));
        }
        for (ReadHeadInfo pos : otherList) {
            //            if (uniqueElements.size() < EdgeWritable.MAX_READ_IDS_PER_EDGE)
            uniqueElements.add(new ReadHeadInfo(pos));
        }
        valueCount = 0;
        setSize(uniqueElements.size() * ReadHeadInfo.LENGTH + HEADER_SIZE);
        for (ReadHeadInfo pos : uniqueElements) {
            append(pos);
        }
    }

    public static int getCountByDataLength(int length) {
        if (length % ReadHeadInfo.LENGTH != 0) {
            throw new IllegalArgumentException("Length of positionlist is invalid");
        }
        return length / ReadHeadInfo.LENGTH;
    }

    public void set(ReadHeadSet otherList) {
        setAsCopy(otherList.storage, otherList.offset);
    }

    public void setAsCopy(byte[] newData, int newOffset) {
        int newValueCount = Marshal.getInt(newData, newOffset);
        setSize(newValueCount * ReadHeadInfo.LENGTH + HEADER_SIZE);
        if (newValueCount > 0) {
            System.arraycopy(newData, newOffset + HEADER_SIZE, storage, this.offset + HEADER_SIZE, newValueCount
                    * ReadHeadInfo.LENGTH);
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

    public String printStartReadIdSet() {
        String output = "5':[";
        if (valueCount > 0) {
            for (int i = 0; i < valueCount - 1; i++)
                output += getPosition(i).getReadId() + ",";
            output += getPosition(valueCount - 1).getReadId();
        }
        output += "]";
        return output;
    }

    public String printEndReadIdSet() {
        String output = "~5':[";
        if (valueCount > 0) {
            for (int i = 0; i < valueCount - 1; i++)
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
                System.arraycopy(storage, offset, new_data, 0, valueCount * ReadHeadInfo.LENGTH + HEADER_SIZE);
            }
            storage = new_data;
            offset = 0;
            maxStorageSize = storage.length;
        }
    }

    public ReadHeadInfo getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setNewReference(storage, offset + i * ReadHeadInfo.LENGTH + HEADER_SIZE);
        return posIter;
    }

    public void resetPosition(int i, long uuid) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        Marshal.putLong(uuid, storage, offset + i * ReadHeadInfo.LENGTH + HEADER_SIZE);
    }

    public void removePosition(int i) {
        if (i < 0 || i > valueCount)
            throw new IllegalArgumentException("Invalid position specified in removePosition! Should be 0 <= " + i
                    + " <= " + valueCount + ").");
        System.arraycopy(storage, offset + i * ReadHeadInfo.LENGTH + HEADER_SIZE, storage, offset + (i - 1)
                * ReadHeadInfo.LENGTH + HEADER_SIZE, (valueCount - i) * ReadHeadInfo.LENGTH);
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
        return valueCount * ReadHeadInfo.LENGTH + HEADER_SIZE;
    }

    @Override
    public Iterator<ReadHeadInfo> iterator() {
        Iterator<ReadHeadInfo> it = new Iterator<ReadHeadInfo>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < valueCount;
            }

            @Override
            public ReadHeadInfo next() {
                return getPosition(currentIndex++);
            }

            @Override
            public void remove() {
                if (currentIndex < valueCount)
                    System.arraycopy(storage, offset + currentIndex * ReadHeadInfo.LENGTH + HEADER_SIZE, storage,
                            offset + (currentIndex - 1) * ReadHeadInfo.LENGTH + HEADER_SIZE,
                            (valueCount - currentIndex) * ReadHeadInfo.LENGTH);
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
    public void remove(ReadHeadInfo toRemove, boolean ignoreMissing) {
        Iterator<ReadHeadInfo> posIterator = this.iterator();
        while (posIterator.hasNext()) {
            if (toRemove.equals(posIterator.next())) {
                posIterator.remove();
                return; // found it. return early. 
            }
        }
        // element not found.
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the PositionWritable `" + toRemove.toString()
                    + "` was not found in this list.");
        }
    }

    public void remove(ReadHeadInfo toRemove) {
        remove(toRemove, false);
    }

    public void removeReadId(long readId) {
        ReadHeadInfo toRemove = new ReadHeadInfo();
        toRemove.set((byte) 0, readId, 0);
        remove(toRemove);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset + HEADER_SIZE, valueCount * ReadHeadInfo.LENGTH);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int newValueCount = in.readInt();
        setSize(newValueCount * ReadHeadInfo.LENGTH + HEADER_SIZE);
        in.readFully(storage, offset + HEADER_SIZE, newValueCount * ReadHeadInfo.LENGTH);
        valueCount = newValueCount;
        Marshal.putInt(valueCount, storage, offset);
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        long[] ids = toUUIDArray();
        Arrays.sort(ids);
        ReadHeadInfo posn = new ReadHeadInfo();
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
        for (int i = 0; i < valueCount; i++) {
            result[i] = getPosition(i).getUUID();
        }
        return result;
    }

    public long[] toReadIDArray() {
        long[] result = new long[valueCount];
        for (int i = 0; i < valueCount; i++) {
            result[i] = getPosition(i).getReadId();
        }
        return result;
    }

    public Set<Long> getSetOfReadIds() {
        Set<Long> readIds = new HashSet<Long>();
        for (long readId : toReadIDArray()) {
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
        if (!(o instanceof ReadHeadSet))
            return false;
        ReadHeadSet other = (ReadHeadSet) o;
        if (this.valueCount != other.valueCount)
            return false;
        for (int i = 0; i < this.valueCount; i++) {
            if (!this.getPosition(i).equals(other.getPosition(i)))
                return false;
        }
        return true;
    }

    public boolean isEmpty() {
        return this.getCountOfPosition() == 0;
    }

    public static ReadHeadSet getIntersection(ReadHeadSet list1, ReadHeadSet list2) {
        ReadHeadSet intersection = new ReadHeadSet(list1);
        intersection.retainAll(list2);
        return intersection;
    }
}
