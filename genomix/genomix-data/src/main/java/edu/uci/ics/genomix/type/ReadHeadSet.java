package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.ReadHeadInfo.READHEADINFO_FIELDS;
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

    public void add(byte mateId, long readId, int offset, VKmer thisReadSequence, VKmer thatReadSequence) {
        add(new ReadHeadInfo(mateId, readId, offset, thisReadSequence, thatReadSequence));
    }

    public int getOffsetFromReadId(long readId) {
        for (ReadHeadInfo readHeadInfo : this) {
            if (readHeadInfo.getReadId() == readId)
                return readHeadInfo.getOffset();
        }
        throw new IllegalArgumentException("The input parameter readId " + readId
                + " should exist in this ReadHeadSet, but not here!");
    }

    public void setAsCopy(byte[] data, int offset) {
        clear();
        int count = Marshal.getInt(data, offset);
        offset += HEADER_SIZE;
        for (int i = 0; i < count; i++) {
            byte activeFields = data[offset];
            offset++;
            long uuid = Marshal.getLong(data, offset);
            ReadHeadInfo curInfo = new ReadHeadInfo(uuid, null, null);
            offset += ReadHeadInfo.ITEM_SIZE;
            if ((activeFields & READHEADINFO_FIELDS.THIS_READSEQUENCE) != 0) {
                curInfo.getThisReadSequence().setAsCopy(data, offset);
                offset += curInfo.getThisReadSequence().getLength();
            }
            if ((activeFields & READHEADINFO_FIELDS.THAT_READSEQUENCE) != 0) {
                curInfo.getMateReadSequence().setAsCopy(data, offset);
                offset += curInfo.getMateReadSequence().getLength();
            }
            add(curInfo);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size());
        System.out.println(size());
        for (ReadHeadInfo head : this) {
            head.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            ReadHeadInfo temp = new ReadHeadInfo();
            temp.readFields(in);
            add(temp);
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
        int totalBytes = HEADER_SIZE;
        for(ReadHeadInfo iter : this)
            totalBytes += iter.getLengthInBytes();
        return totalBytes;
    }
}
