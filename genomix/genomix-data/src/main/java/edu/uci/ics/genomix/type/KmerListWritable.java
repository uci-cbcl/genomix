package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class KmerListWritable implements Writable, Iterable<KmerBytesWritable>, Serializable{
    private static final long serialVersionUID = 1L;
    protected byte[] storage;
    protected int offset;
    protected int valueCount;
    public int KMER_LENGTH = 3;
    protected static final byte[] EMPTY = {};
    
    protected KmerBytesWritable posIter = new KmerBytesWritable();
    
    public KmerListWritable() {
        this.storage = EMPTY;
        this.valueCount = 0;
        this.offset = 0;
    }
    
    public KmerListWritable(int kmerLength) {
        this();
        this.KMER_LENGTH = kmerLength;
    }
    
    public KmerListWritable(int count, byte[] data, int offset) {
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
        setSize((1 + valueCount) * kmer.getLength());
        System.arraycopy(kmer.getBytes(), 0, storage, offset, KMER_LENGTH);
        valueCount += 1;
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
        posIter.setNewReference(storage, offset + i * KMER_LENGTH);
        return posIter;
    }
    
    public void set(KmerListWritable otherList) {
        set(otherList.valueCount, otherList.storage, otherList.offset);
    }

    public void set(int valueCount, byte[] newData, int offset) {
        this.valueCount = valueCount;
        setSize(valueCount * KMER_LENGTH);
        if (valueCount > 0) {
            System.arraycopy(newData, offset, storage, this.offset, valueCount * KMER_LENGTH);
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
                    System.arraycopy(storage, offset + currentIndex * KMER_LENGTH, 
                          storage, offset + (currentIndex - 1) * KMER_LENGTH, 
                          (valueCount - currentIndex) * KMER_LENGTH);
                valueCount--;
                currentIndex--;
            }
        };
        return it;
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
    
    public int getCountOfPosition() {
        return valueCount;
    }

    public byte[] getByteArray() {
        return storage;
    }

    public int getStartOffset() {
        return offset;
    }
}
