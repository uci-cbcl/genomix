package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
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

import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.util.Marshal;

public class ReadHeadSet extends TreeSet<ReadHeadInfo> implements Writable, Serializable {
    private static final long serialVersionUID = 1L;
    private static final int HEADER_SIZE = 4;
    
    public ReadHeadSet() {
        super();
    }

    public ReadHeadSet(Collection<? extends ReadHeadInfo> c) {
        super(c);
    }

    public ReadHeadSet(Comparator<? super ReadHeadInfo> comparator) {
        super(comparator);
    }

    public ReadHeadSet(SortedSet<ReadHeadInfo> s) {
        super(s);
    }
    
    public void add(byte mateId, long readId, int offset) {
        add(new ReadHeadInfo(mateId, readId, offset));
    }

    public ReadHeadInfo getReadHeadInfoFromReadId(long readId){
        for(ReadHeadInfo readHeadInfo : this){
            if(readHeadInfo.getReadId() == readId)
                return readHeadInfo;
        }
        return null;
    }
    
    public int getOffsetFromReadId(long readId){
        return getReadHeadInfoFromReadId(readId).getOffset();
    }
    
    public void setAsCopy(byte[] data, int offset) {
        clear();
        int count = Marshal.getInt(data, offset);
        offset += HEADER_SIZE;
        for (int i=0; i < count; i++) {
            add(new ReadHeadInfo(Marshal.getLong(data, offset)));
            offset += ReadHeadInfo.ITEM_SIZE;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size());
        for (ReadHeadInfo head : this) {
            out.writeLong(head.asLong());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int count = in.readInt();
        for (int i=0; i < count; i++) {
            add(new ReadHeadInfo(in.readLong()));
        }
    }
    
    public String toReadIdString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        String delim = "";
        for (ReadHeadInfo head : this) {
            sbuilder.append(delim).append(head.getReadId());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString(); 
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        String delim = "";
        for (ReadHeadInfo head : this) {
            sbuilder.append(delim).append(head.toString());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString();    
    }

    public static ReadHeadSet getIntersection(ReadHeadSet list1, ReadHeadSet list2) {
        ReadHeadSet intersection = new ReadHeadSet(list1);
        intersection.retainAll(list2);
        return intersection;
    }

    public int getLengthInBytes() {
        return HEADER_SIZE + ReadHeadInfo.ITEM_SIZE * size();
    }
}
