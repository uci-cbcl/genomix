package edu.uci.ics.genomix.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.data.util.Marshal;

public class ReadIdSet extends TreeSet<Long> implements Writable, Serializable {
    private static final long serialVersionUID = 1L;
    private static final int HEADER_SIZE = 4;
    private static final int ITEM_SIZE = 8;

    public ReadIdSet() {
        super();
    }

    public ReadIdSet(Collection<? extends Long> c) {
        super(c);
    }

    public ReadIdSet(SortedSet<? extends Long> set) {
        super(set);
    }

    public ReadIdSet(Comparator<? super Long> comparator) {
        super(comparator);
    }

    /**
     * return the intersection of this list and another list (return a copy)
     */
    public static ReadIdSet getIntersection(ReadIdSet list1, ReadIdSet list2) {
        ReadIdSet intersection = new ReadIdSet(list1);
        intersection.retainAll(list2);
        return intersection;
    }

    public ReadIdSet getIntersection(ReadIdSet list2) {
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
        for (int i = 0; i < count; i++) {
            add(in.readLong());
        }
    }

    public int setAsCopy(byte[] data, int offset) {
        clear();
        int count = Marshal.getInt(data, offset);
        offset += HEADER_SIZE;
        for (int i = 0; i < count; i++) {
            add(Marshal.getLong(data, offset));
            offset += ITEM_SIZE;
        }
        return offset;
    }
}
