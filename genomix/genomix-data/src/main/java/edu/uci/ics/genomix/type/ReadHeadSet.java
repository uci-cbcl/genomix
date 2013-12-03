package edu.uci.ics.genomix.type;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

import org.apache.hadoop.io.Writable;

public class ReadHeadSet implements Writable, Serializable {
    private static final long serialVersionUID = 1L;

    ExternalableTreeSet<ReadHeadInfo> readSet;

    public ReadHeadSet() {
        readSet = new ExternalableTreeSet<ReadHeadInfo>();
    }

    public void add(byte mateId, long readId, int offset, VKmer thisReadSequence, VKmer thatReadSequence) {
        readSet.add(new ReadHeadInfo(mateId, readId, offset, thisReadSequence, thatReadSequence));
    }

    public int getOffsetFromReadId(long readId) {
        ReadHeadInfo lowKey = ReadHeadInfo.getLowerBoundInfo(readId);
        ReadHeadInfo highKey = ReadHeadInfo.getUpperBoundInfo(readId);
        SortedSet<ReadHeadInfo> result = readSet.rangeSearch(lowKey, highKey);
        if (result.size() != 0) {
            for (ReadHeadInfo readHeadInfo : result) {
                if (readHeadInfo.getReadId() == readId) {
                    return readHeadInfo.getOffset();
                }
            }
        }
        throw new IllegalArgumentException("The input parameter readId " + readId
                + " should exist in this ReadHeadSet, but not here!");
    }

    /**
     * @param data
     * @param offset
     * @return How many bytes read by this function.
     * @throws IOException
     */
    public int setAsCopy(byte[] data, int offset) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(data, offset, data.length);
        DataInputStream din = new DataInputStream(bin);
        readSet.readFields(din);
        return data.length - bin.available() - offset;
    }

    public void setAsCopy(ReadHeadSet that) {
        readSet.setAsCopy(that.readSet);
    }

    public int size() {
        return readSet.size();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        readSet.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        readSet.readFields(in);
    }

    public String toReadIdString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        String delim = "";
        for (ReadHeadInfo info : readSet.inMemorySet) {
            sbuilder.append(delim).append(info.getReadId());
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
        for (ReadHeadInfo info : readSet.inMemorySet) {
            sbuilder.append(delim).append(info.toString());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString();
    }

    public void add(ReadHeadInfo readHeadInfo) {
        readSet.add(readHeadInfo);
    }

    public void unionUpdate(ReadHeadSet readIds, float lengthFactor, boolean flipOffset, int otherLength) {
        if (!flipOffset) {
            for (ReadHeadInfo p : readSet.inMemorySet) {
                this.add(p.getMateId(), p.getReadId(), (int) ((p.getOffset() + 1) * lengthFactor - lengthFactor),
                        p.getThisReadSequence(), p.getMateReadSequence());
            }
        } else {
            // int newOtherOffset = (int) ((otherLength - 1) * lengthFactor);
            // stream theirs in, offset and flipped
            for (ReadHeadInfo p : readSet.inMemorySet) {
                int newPOffset = otherLength - 1 - p.getOffset();
                this.add(p.getMateId(), p.getReadId(), (int) ((newPOffset + 1) * lengthFactor - lengthFactor),
                        p.getThisReadSequence(), p.getMateReadSequence());
            }
        }
    }

    public void postAppendOffsets(int newThisOffset) {
        for (ReadHeadInfo p : readSet.inMemorySet) {
            p.resetOffset(newThisOffset + p.getOffset());
        }
        readSet.isChanged = true;
    }

    public void unionUpdate(ReadHeadSet setB) {
        this.readSet.union(setB.readSet);
    }

    public void preAppendOffset(int newOtherOffset) {
        for (ReadHeadInfo p : readSet.inMemorySet) {
            p.resetOffset(newOtherOffset - p.getOffset());
        }
        readSet.isChanged = true;
    }

    public Set<Long> getReadIdSet() {
        HashSet<Long> set = new HashSet<Long>();
        for (ReadHeadInfo p : readSet.inMemorySet) {
            set.add(p.getReadId());
        }
        return set;
    }

}
