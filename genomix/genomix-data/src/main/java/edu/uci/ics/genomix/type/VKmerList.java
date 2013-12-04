package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.util.KmerUtil;
import edu.uci.ics.genomix.util.Marshal;

/**
 * A list of fixed-length kmers. The length of this list is stored internally.
 */
public class VKmerList implements Writable, Iterable<VKmer>, Serializable {
    private static final long serialVersionUID = 1L;
    protected static final byte[] EMPTY_BYTES = { 0, 0, 0, 0 };
    protected static final int HEADER_SIZE = 4;

    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    protected int storageMaxSize; // since we may be a reference inside a larger datablock, we must track our maximum size

    private VKmer posIter = new VKmer();

    public VKmerList() {
        storage = EMPTY_BYTES;
        valueCount = 0;
        offset = 0;
        storageMaxSize = storage.length;
    }

    public VKmerList(byte[] data, int offset) {
        setAsReference(data, offset);
    }

    public VKmerList(VKmerList kmerList){
        this();
        for (VKmer kmer : kmerList) {
            append(kmer);
        }
    }
    
    public VKmerList(List<VKmer> kmers) {
        this();
        for (VKmer kmer : kmers) {
            append(kmer);
        }
    }

    public int setAsReference(byte[] data, int offset) {
        valueCount = Marshal.getInt(data, offset);
        this.storage = data;
        this.offset = offset;
        this.storageMaxSize = getLengthInBytes();
        return offset + this.storageMaxSize;
    }

    public void append(VKmer kmer) {
        setSize(getLengthInBytes() + kmer.getLength());
        System.arraycopy(kmer.getBlockBytes(), kmer.kmerStartOffset - VKmer.HEADER_SIZE, storage, offset + getLengthInBytes(),
                kmer.getLength());
        valueCount += 1;
        Marshal.putInt(valueCount, storage, offset);
    }

    public void append(int k, Kmer kmer) {
        setSize(getLengthInBytes() + HEADER_SIZE + kmer.getLength());
        Marshal.putInt(k, storage, offset + getLengthInBytes());
        System.arraycopy(kmer.getBytes(), kmer.getOffset(), storage, offset + getLengthInBytes() + HEADER_SIZE,
                kmer.getLength());
        valueCount += 1;
        Marshal.putInt(valueCount, storage, offset);
    }

    public void append(Kmer kmer) { // TODO optimize this into two separate containers...
        setSize(getLengthInBytes() + kmer.getLength() + VKmer.HEADER_SIZE);
        int myLength = getLengthInBytes();
        Marshal.putInt(Kmer.getKmerLength(), storage, offset + myLength); // write a new VKmer header
        System.arraycopy(kmer.getBytes(), kmer.offset, storage, offset + myLength + VKmer.HEADER_SIZE, kmer.getLength());
        valueCount += 1;
        Marshal.putInt(valueCount, storage, offset);
    }

    /*
     * Append the otherList to the end of myList
     */
    public void appendList(VKmerList otherList) {
        if (otherList.valueCount > 0) {
            setSize(getLengthInBytes() + otherList.getLengthInBytes() - HEADER_SIZE); // one of the headers is redundant

            // copy contents of otherList into the end of my storage
            System.arraycopy(otherList.storage, otherList.offset + HEADER_SIZE, // skip other header
                    storage, offset + getLengthInBytes(), // add to end
                    otherList.getLengthInBytes() - HEADER_SIZE);
            valueCount += otherList.valueCount;
            Marshal.putInt(valueCount, storage, offset);
        }
    }

    /**
     * Save the union of my list and otherList. Uses a temporary HashSet for
     * uniquefication
     */
    public void unionUpdate(VKmerList otherList) {
        int newSize = valueCount + otherList.valueCount;
        HashSet<VKmer> uniqueElements = new HashSet<VKmer>(newSize);
        for (VKmer kmer : this) {
            // have to make copies of my own kmers since I may overwrite them
            uniqueElements.add(new VKmer(kmer));
        }
        for (VKmer kmer : otherList) {
            uniqueElements.add(new VKmer(kmer)); // references okay
        }
        setSize(getLengthInBytes() + otherList.getLengthInBytes()); // upper bound on memory usage
        valueCount = 0;
        for (VKmer kmer : uniqueElements) {
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
        return storageMaxSize - offset;
    }

    protected void setCapacity(int new_cap) {
        if (new_cap > getCapacity()) {
            byte[] new_data = new byte[new_cap];
            if (valueCount > 0) {
                System.arraycopy(storage, offset, new_data, 0, getLengthInBytes());
            }
            storage = new_data;
            offset = 0;
            storageMaxSize = storage.length;
        }
    }

    public void clear() {
        valueCount = 0;
        Marshal.putInt(valueCount, storage, offset);
    }

    public VKmer getPosition(int i) {
        posIter.setAsReference(storage, getOffsetOfKmer(i));
        return posIter;
    }

    /**
     * Return the offset of the kmer at the i'th position
     */
    public int getOffsetOfKmer(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such position " + i + " in list " + toString());
        }
        // seek to the given position
        int posOffset = offset + HEADER_SIZE;
        for (int curIndex = 0; curIndex < i; curIndex++) {
            posOffset += KmerUtil.getByteNumFromK(Marshal.getInt(storage, posOffset)) + VKmer.HEADER_SIZE;
        }
        return posOffset;
    }

    public int setAsCopy(VKmerList otherList) {
        return setAsCopy(otherList.storage, otherList.offset);
    }

    /**
     * read a KmerListWritable from newData, which should include the header
     * @return 
     */
    public int setAsCopy(byte[] newData, int newOffset) {
        int newValueCount = Marshal.getInt(newData, newOffset);
        int newLength = getLength(newData, newOffset);
        setSize(newLength);
        if (newValueCount > 0) {
            System.arraycopy(newData, newOffset + HEADER_SIZE, storage, this.offset + HEADER_SIZE, newLength
                    - HEADER_SIZE);
        }
        valueCount = newValueCount;
        Marshal.putInt(valueCount, storage, this.offset);
        return newOffset + newLength;
    }

    @Override
    public Iterator<VKmer> iterator() {
        Iterator<VKmer> it = new Iterator<VKmer>() {

            private int currentIndex = 0;
            private int currentOffset = offset + HEADER_SIZE; // init as offset of first kmer

            @Override
            public boolean hasNext() {
                return currentIndex < valueCount;
            }

            @Override
            public VKmer next() {
                posIter.setAsReference(storage, currentOffset);
                currentOffset += KmerUtil.getByteNumFromK(Marshal.getInt(storage, currentOffset)) + VKmer.HEADER_SIZE;
                currentIndex++;
                return posIter;
            }

            @Override
            public void remove() {
                if (currentOffset <= 0) {
                    throw new IllegalStateException(
                            "You must advance the iterator using .next() before calling remove()!");
                }
                // we're removing the element at prevIndex
                int prevIndex = currentIndex - 1;
                int prevOffset = getOffsetOfKmer(prevIndex);

                if (currentIndex < valueCount) { // if it's the last element, don't have to do any copying
                    System.arraycopy(storage, currentOffset, // from the "next" element
                            storage, prevOffset, // to the one just returned (overwriting it)
                            getLengthInBytes() - currentOffset + offset); // remaining bytes except current element
                }
                valueCount--;
                currentIndex--;
                Marshal.putInt(valueCount, storage, offset);
                currentOffset = prevOffset;
            }
        };
        return it;
    }
    
    public boolean contains(VKmer kmer) {
        Iterator<VKmer> posIterator = this.iterator();
        while (posIterator.hasNext()) {
            if (kmer.equals(posIterator.next()))
                return true;
        }
        return false;
    }
    
    public int indexOf(VKmer kmer) {
        Iterator<VKmer> posIterator = this.iterator();
        int i = 0;
        while (posIterator.hasNext()) {
            if (kmer.equals(posIterator.next()))
                return i;
            i++;
        }
        return -1;
    }
    /*
     * remove the first instance of `toRemove`. Uses a linear scan. Throws an
     * exception if not in this list.
     */
    public void remove(VKmer toRemove, boolean ignoreMissing) {
        Iterator<VKmer> posIterator = this.iterator();
        while (posIterator.hasNext()) {
            if (toRemove.equals(posIterator.next())) {
                posIterator.remove();
                return; // break as soon as the element is found 
            }
        }
        // element was not found
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the KmerBytesWritable `" + toRemove.toString()
                    + "` was not found in this list.");
        }
    }

    public void remove(VKmer toRemove) {
        remove(toRemove, false);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int newValueCount = in.readInt();
        int curOffset = offset + HEADER_SIZE;
        int elemBytes = 0;
        int elemLetters = 0;
        int curLength = getLengthInBytes();
        for (int i = 0; i < newValueCount; i++) {
            elemLetters = in.readInt();
            elemBytes = KmerUtil.getByteNumFromK(elemLetters) + VKmer.HEADER_SIZE;
            setSize(curLength + elemBytes); // make sure we have room for the new element
            Marshal.putInt(elemLetters, storage, curOffset); // write header
            in.readFully(storage, curOffset + VKmer.HEADER_SIZE, elemBytes - VKmer.HEADER_SIZE); // write kmer
            curOffset += elemBytes;
            curLength += elemBytes;
            valueCount++;
        }
        valueCount = newValueCount;
        Marshal.putInt(valueCount, storage, offset);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(storage, offset, getLengthInBytes());
    }

    public int size() {
        return valueCount;
    }

    public byte[] getByteArray() {
        return storage;
    }

    public int getStartOffset() {
        return offset;
    }

    public int getLengthInBytes() {
        int totalSize = HEADER_SIZE;
        for (int curCount = 0; curCount < valueCount; curCount++) {
            totalSize += KmerUtil.getByteNumFromK(Marshal.getInt(storage, offset + totalSize)) + VKmer.HEADER_SIZE;
        }
        return totalSize;
    }

    public static int getLength(byte[] listStorage, int listOffset) {
        int totalSize = HEADER_SIZE;
        int listValueCount = Marshal.getInt(listStorage, listOffset);
        for (int curCount = 0; curCount < listValueCount; curCount++) {
            totalSize += KmerUtil.getByteNumFromK(Marshal.getInt(listStorage, listOffset + totalSize))
                    + VKmer.HEADER_SIZE;
        }
        return totalSize;
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
        return Marshal.hashBytes(getByteArray(), getStartOffset(), getLengthInBytes());
    }

}
