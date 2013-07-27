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

/**
 * A list of fixed-length kmers. The length of this list is stored internally.
 */
public class KmerListWritable implements Writable, Iterable<KmerBytesWritable>, Serializable {
    private static final long serialVersionUID = 1L;
    protected static final byte[] EMPTY_BYTES = { 0, 0, 0, 0 };
    protected static final int HEADER_SIZE = 4;

    protected byte[] storage;
    protected int offset;
    protected int valueCount;

    private KmerBytesWritable posIter = new KmerBytesWritable();

    public KmerListWritable() {
        this.storage = EMPTY_BYTES;
        this.valueCount = 0;
        this.offset = 0;
    }

    public KmerListWritable(byte[] data, int offset) {
//        setNewReference(data, offset);
        this();
        setCopy(data, offset);
    }

    public KmerListWritable(List<KmerBytesWritable> kmers) {
        this();
        setSize(kmers.size() * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE); // reserve space for all elements
        for (KmerBytesWritable kmer : kmers) {
            append(kmer);
        }
    }

//    public void setNewReference(byte[] data, int offset) {
//        valueCount = Marshal.getInt(data, offset);
//        if (valueCount * KmerBytesWritable.getBytesPerKmer() > data.length - offset) {
//            throw new IllegalArgumentException("Specified data buffer (len=" + (data.length - offset)
//                    + ") is not large enough to store requested number of elements (" + valueCount + ")!");
//        }
//        this.storage = data;
//        this.offset = offset;
//    }

    public void append(KmerBytesWritable kmer) {
        setSize((1 + valueCount) * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE);
        System.arraycopy(kmer.getBytes(), 0, storage,
                offset + HEADER_SIZE + valueCount * KmerBytesWritable.getBytesPerKmer(),
                KmerBytesWritable.getBytesPerKmer());
        valueCount += 1;
        Marshal.putInt(valueCount, storage, offset);
    }

    /*
     * Append the otherList to the end of myList
     */
    public void appendList(KmerListWritable otherList) {
        if (otherList.valueCount > 0) {
            setSize((valueCount + otherList.valueCount) * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE);
            // copy contents of otherList into the end of my storage
            System.arraycopy(otherList.storage, otherList.offset + HEADER_SIZE, storage, offset + HEADER_SIZE
                    + valueCount * KmerBytesWritable.getBytesPerKmer(),
                    otherList.valueCount * KmerBytesWritable.getBytesPerKmer());
            valueCount += otherList.valueCount;
            Marshal.putInt(valueCount, storage, offset);
        }
    }

    /**
     * Save the union of my list and otherList. Uses a temporary HashSet for
     * uniquefication
     */
    public void unionUpdate(KmerListWritable otherList) {
        int newSize = valueCount + otherList.valueCount;
        HashSet<KmerBytesWritable> uniqueElements = new HashSet<KmerBytesWritable>(newSize);
        for (KmerBytesWritable kmer : this) {
            uniqueElements.add(kmer);
        }
        for (KmerBytesWritable kmer : otherList) {
            uniqueElements.add(kmer);
        }
        valueCount = 0;
        setSize(newSize * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE);
        for (KmerBytesWritable kmer : uniqueElements) {
            append(kmer);
        }
        Marshal.putInt(valueCount, storage, offset);
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

    public void reset() {
        valueCount = 0;
    }

    public KmerBytesWritable getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setAsReference(storage, offset + HEADER_SIZE + i * KmerBytesWritable.getBytesPerKmer());
        return posIter;
    }

    public void setCopy(KmerListWritable otherList) {
        setCopy(otherList.storage, otherList.offset);
    }

    /**
     * save as a copy of the given data buffer, including the header
     */
    public void setCopy(byte[] newData, int offset) {
        valueCount = Marshal.getInt(newData, offset);
        setSize(valueCount * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE);
        if (valueCount > 0) {
            System.arraycopy(newData, offset + HEADER_SIZE, storage, this.offset + HEADER_SIZE, valueCount
                    * KmerBytesWritable.getBytesPerKmer());
        }
        Marshal.putInt(valueCount, storage, this.offset);
    }

    @Override
    public Iterator<KmerBytesWritable> iterator() {
        Iterator<KmerBytesWritable> it = new Iterator<KmerBytesWritable>() {

            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
                return currentIndex < valueCount;
            }

            @Override
            public KmerBytesWritable next() {
                return getPosition(currentIndex++);
            }

            @Override
            public void remove() {
                if (currentIndex < valueCount)
                    System.arraycopy(storage, offset + currentIndex * KmerBytesWritable.getBytesPerKmer(), storage,
                            offset + (currentIndex - 1) * KmerBytesWritable.getBytesPerKmer(),
                            (valueCount - currentIndex) * KmerBytesWritable.getBytesPerKmer());
                valueCount--;
                currentIndex--;
                Marshal.putInt(valueCount, storage, offset);
            }
        };
        return it;
    }

    /*
     * remove the first instance of `toRemove`. Uses a linear scan. Throws an
     * exception if not in this list.
     */
    public void remove(KmerBytesWritable toRemove, boolean ignoreMissing) {
        Iterator<KmerBytesWritable> posIterator = this.iterator();
        while (posIterator.hasNext()) {
            if (toRemove.equals(posIterator.next())) {
                posIterator.remove();
                return;
            }
        }
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the KmerBytesWritable `" + toRemove.toString()
                    + "` was not found in this list.");
        }
    }

    public void remove(KmerBytesWritable toRemove) {
        remove(toRemove, false);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        valueCount = in.readInt();
        setSize(valueCount * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE);
        in.readFully(storage, offset + HEADER_SIZE, valueCount * KmerBytesWritable.getBytesPerKmer() - HEADER_SIZE);
        Marshal.putInt(valueCount, storage, offset);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(storage, offset, valueCount * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE);
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
        return valueCount * KmerBytesWritable.getBytesPerKmer() + HEADER_SIZE;
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        for (int i = 0; i < valueCount; i++) {
            sbuilder.append(getPosition(i).toString());
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
}
