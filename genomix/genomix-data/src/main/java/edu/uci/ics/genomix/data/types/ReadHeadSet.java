package edu.uci.ics.genomix.data.types;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;

public class ReadHeadSet extends ExternalableTreeSet<ReadHeadInfo> {
    private static final long serialVersionUID = 1L;

    public ReadHeadSet() {
        super();
    }

    public void add(byte mateId, byte libraryId, long readId, int offset, VKmer thisReadSequence, VKmer thatReadSequence) {
        super.add(new ReadHeadInfo(mateId, libraryId, readId, offset, thisReadSequence, thatReadSequence));
    }

    // Change the primary key.
    //    public int getOffsetFromReadId(long readId) {
    //        ReadHeadInfo lowKey = ReadHeadInfo.getLowerBoundInfo(readId);
    //        ReadHeadInfo hit = super.inMemorySet.ceiling(lowKey);
    //        if (hit != null && hit.getReadId() == readId){
    //            return hit.getOffset();
    //        }
    //        throw new IllegalArgumentException("The input parameter readId " + readId
    //                + " should exist in this ReadHeadSet, but not here!");
    //    }

    /**
     * @param data
     * @param offset
     * @return The current offset after reading this object.
     * @throws IOException
     */
    public int setAsCopy(byte[] data, int offset) throws IOException {
        ByteArrayInputStream bin = new ByteArrayInputStream(data, offset, data.length);
        DataInputStream din = new DataInputStream(bin);
        super.readFields(din);
        return data.length - bin.available();
    }

    public int size() {
        return super.size();
    }

    public String toReadIdString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        String delim = "";
        for (ReadHeadInfo info : super.inMemorySet) {
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
        for (ReadHeadInfo info : super.inMemorySet) {
            sbuilder.append(delim).append(info.toString());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString();
    }

    public void unionUpdate(ReadHeadSet readIds, float lengthFactor, boolean flipOffset, int otherLength) {
        if (!flipOffset) {
            for (ReadHeadInfo p : readIds.inMemorySet) {
                this.add(p.getMateId(), p.getLibraryId(), p.getReadId(), (int) ((p.getOffset() + 1) * lengthFactor - lengthFactor),
                        p.getThisReadSequence(), p.getMateReadSequence());
            }
        } else {
            // int newOtherOffset = (int) ((otherLength - 1) * lengthFactor);
            // stream theirs in, offset and flipped
            for (ReadHeadInfo p : readIds.inMemorySet) {
                int newPOffset = otherLength - 1 - p.getOffset();
                this.add(p.getMateId(), p.getLibraryId(), p.getReadId(), (int) ((newPOffset + 1) * lengthFactor - lengthFactor),
                        p.getThisReadSequence(), p.getMateReadSequence());
            }
        }
    }

    public void prependOffsets(int newThisOffset) {
        for (ReadHeadInfo p : super.inMemorySet) {
            p.resetOffset(newThisOffset + p.getOffset());
        }
        super.isChanged = true;
    }

    public void flipOffset(int newOtherOffset) {
        for (ReadHeadInfo p : super.inMemorySet) {
            p.resetOffset(newOtherOffset - p.getOffset());
        }
        super.isChanged = true;
    }

    public void unionUpdate(ReadHeadSet setB) {
        super.union(setB);
    }

    public SortedSet<ReadHeadInfo> getOffSetRange(int lowOffset, int highOffset, boolean mate) {
        return super.rangeSearch(ReadHeadInfo.getLowerBoundInfo(lowOffset,mate), ReadHeadInfo.getUpperBoundInfo(highOffset,mate));
    }

    public Set<Long> getReadIdSet() {
        HashSet<Long> set = new HashSet<Long>();
        for (ReadHeadInfo p : super.inMemorySet) {
            set.add(p.getReadId());
        }
        return set;
    }

    @Override
    public ReadHeadInfo readNonGenericElement(DataInput in) throws IOException {
        ReadHeadInfo info = new ReadHeadInfo();
        info.readFields(in);
        return info;
    }

    @Override
    public void writeNonGenericElement(DataOutput out, ReadHeadInfo t) throws IOException {
        t.write(out);
    }

}
