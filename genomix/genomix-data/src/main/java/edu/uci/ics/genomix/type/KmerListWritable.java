package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.KmerUtil;
import edu.uci.ics.genomix.data.Marshal;

public class KmerListWritable extends BinaryComparable
    implements Writable, Iterable<KmerBytesWritable>, Serializable{
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    public int kmerByteSize = 0; 
    public int kmerlength = 0;
    protected static final byte[] EMPTY = {};
    
    protected KmerBytesWritable posIter = new KmerBytesWritable();
    
    public KmerListWritable() {
        this.storage = EMPTY;
        this.valueCount = 0;
        this.offset = 0;
    }
    
    public KmerListWritable(int kmerlength) {
        this();
        this.kmerlength = kmerlength;
        this.kmerByteSize = KmerUtil.getByteNumFromK(kmerlength);
    }
    
    public KmerListWritable(int kmerlength, int count, byte[] data, int offset) {
        this.kmerlength = kmerlength;
        this.kmerByteSize = KmerUtil.getByteNumFromK(kmerlength);
        setNewReference(count, data, offset);
    }
    
    public KmerListWritable(List<KmerBytesWritable> kmers) {
        this();
        setSize(kmers.size());  // reserve space for all elements
        for (KmerBytesWritable kmer : kmers) {
            append(kmer);
        }
    }
    
    public void setNewReference(int count, byte[] data, int offset) {
        this.valueCount = count;
        this.storage = data;
        this.offset = offset;
    }
    
    public void append(KmerBytesWritable kmer){
        if(kmer != null){
            kmerByteSize = kmer.kmerByteSize;
            kmerlength = kmer.kmerlength;
            setSize((1 + valueCount) * kmerByteSize); 
            System.arraycopy(kmer.getBytes(), 0, storage, offset + valueCount * kmerByteSize, kmerByteSize);
            valueCount += 1;
        }
    }
    
    /*
     * Append the otherList to the end of myList
     */
    public void appendList(KmerListWritable otherList) {
        if (otherList.valueCount > 0) {
            setSize((valueCount + otherList.valueCount) * kmerByteSize);
            // copy contents of otherList into the end of my storage
            System.arraycopy(otherList.storage, otherList.offset,
                    storage, offset + valueCount * kmerByteSize, 
                    otherList.valueCount * kmerByteSize);
            valueCount += otherList.valueCount;
        }
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
        this.reset(0);
    }
    
    public void reset(int kmerSize) {
        kmerlength = kmerSize;
        kmerByteSize = KmerUtil.getByteNumFromK(kmerlength);
        storage = EMPTY;
        valueCount = 0;
        offset = 0;
    }
    
    public KmerBytesWritable getPosition(int i) {
        if (i >= valueCount) {
            throw new ArrayIndexOutOfBoundsException("No such positions");
        }
        posIter.setNewReference(kmerlength, storage, offset + i * kmerByteSize);
        return posIter;
    }
    
    public void set(KmerListWritable otherList) {
        this.kmerlength = otherList.kmerlength;
        this.kmerByteSize = otherList.kmerByteSize;
        set(otherList.valueCount, otherList.storage, otherList.offset);
    }

    public void set(int valueCount, byte[] newData, int offset) {
        this.valueCount = valueCount;
        setSize(valueCount * kmerByteSize);
        if (valueCount > 0) {
            System.arraycopy(newData, offset, storage, this.offset, valueCount * kmerByteSize);
        }
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
                if(currentIndex < valueCount)
                    System.arraycopy(storage, offset + currentIndex * kmerByteSize, 
                          storage, offset + (currentIndex - 1) * kmerByteSize, 
                          (valueCount - currentIndex) * kmerByteSize);
                valueCount--;
                currentIndex--;
            }
        };
        return it;
    }
    
    /*
     * remove the first instance of @toRemove. Uses a linear scan.  Throws an exception if not in this list.
     */
    public void remove(KmerBytesWritable toRemove, boolean ignoreMissing) {
        Iterator<KmerBytesWritable> posIterator = this.iterator();
        while (posIterator.hasNext()) {
            if(toRemove.equals(posIterator.next())) {
                posIterator.remove();
                return;
            }
        }
        if (!ignoreMissing) {
            throw new ArrayIndexOutOfBoundsException("the KmerBytesWritable `" + toRemove.toString() + "` was not found in this list.");
        }
    }
    
    public void remove(KmerBytesWritable toRemove) {
        remove(toRemove, false);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.valueCount = in.readInt();
        setSize(valueCount * kmerByteSize);//kmerByteSize
        in.readFully(storage, offset, valueCount * kmerByteSize);//kmerByteSize
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(valueCount);
        out.write(storage, offset, valueCount * kmerByteSize);
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
        return valueCount * kmerByteSize;
    }
    
    
    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        for(int i = 0; i < valueCount; i++){
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

    @Override
    public byte[] getBytes() {
        
        return null;
    }
}
