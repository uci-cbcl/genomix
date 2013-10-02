package edu.uci.ics.genomix.type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.ByteArray;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.PositionWritable;

public class ReadIdListWritable extends TreeSet<Long> implements Writable, Serializable {
    private static final long serialVersionUID = 1L;
    private static final int HEADER_SIZE = 4;
    private static final int ITEM_SIZE = 8;
    
    public ReadIdListWritable() {
        super();
    }
    
    public ReadIdListWritable(Collection<? extends Long> c) {
        super(c);
    }
    
    public ReadIdListWritable(SortedSet<? extends Long> set) {
        super(set);
    }
  
    public ReadIdListWritable(Comparator<? super Long> comparator) {
        super(comparator);
    }
    
    /**
     * return the intersection of this list and another list
     */
    public static ReadIdListWritable getIntersection(ReadIdListWritable list1, ReadIdListWritable list2){
        ReadIdListWritable intersection = new ReadIdListWritable(list1);
        intersection.retainAll(list2);
        return intersection;
    }
    
    public ReadIdListWritable getIntersection(ReadIdListWritable list2){
        return getIntersection(this, list2);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size());
        for (Long id : this) {
            out.writeLong(id);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int count = in.readInt();
        for (int i=0; i < count; i++) {
            add(in.readLong());
        }
    }
    
    public int getLengthInBytes() {
        return HEADER_SIZE + ITEM_SIZE * size(); 
    }

    public void setAsCopy(byte[] data, int offset) {
        clear();
        int count = Marshal.getInt(data, offset);
        offset += HEADER_SIZE;
        for (int i=0; i < count; i++) {
            add(Marshal.getLong(data, offset));
            offset += ITEM_SIZE;
        }
    }
}
