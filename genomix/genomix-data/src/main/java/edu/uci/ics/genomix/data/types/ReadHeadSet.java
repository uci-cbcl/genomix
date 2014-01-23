package edu.uci.ics.genomix.data.types;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;

public class ReadHeadSet extends ExternalableTreeSet<ReadHeadInfo> implements Iterable<ReadHeadInfo> {
    private static final long serialVersionUID = 1L;

    public ReadHeadSet() {
        super();
    }

    public ReadHeadSet(boolean toLocalFile) {
        super(toLocalFile);
    }

    public void add(byte mateId, byte libraryId, long readId, int offset, VKmer thisReadSequence, VKmer thatReadSequence) {
        super.add(new ReadHeadInfo(mateId, libraryId, readId, offset, thisReadSequence, thatReadSequence));
    }

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

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('[');
        String delim = "";
        ReadIterator iter = super.readOnlyIterator();
        while (iter.hasNext()) {
            ReadHeadInfo info = iter.next();
            sbuilder.append(delim).append(info.toString());
            delim = ",";
        }
        sbuilder.append(']');
        return sbuilder.toString();
    }

    public void unionUpdate(ReadHeadSet setB) {
        super.union(setB);
    }

    public boolean verifySequence(VKmer internalKmer, boolean flip) {
        Iterator<ReadHeadInfo> iter = super.readOnlyIterator();
        while (iter.hasNext()) {
            ReadHeadInfo now = iter.next();
            if (now.getThisReadSequence() == null) {
                continue;
            }
            if (!flip) {
                if (!internalKmer.matchesExactly(now.getOffset(), now.getThisReadSequence(), 0, Kmer.getKmerLength())) {
                    return false;
                }
            }
        }
        return true;
    }

    public void unionUpdate(ReadHeadSet setB, float lengthFactor, boolean flipOffset, int otherLength) {
        if (!flipOffset) {
            ReadIterator iter = setB.readOnlyIterator();
            while (iter.hasNext()) {
                ReadHeadInfo p = iter.next();
                this.add(p.getMateId(), p.getLibraryId(), p.getReadId(),
                        (int) ((p.getOffset() + 1) * lengthFactor - lengthFactor), p.getThisReadSequence(),
                        p.getMateReadSequence());
            }
        } else {
            // int newOtherOffset = (int) ((otherLength - 1) * lengthFactor);
            // stream theirs in, offset and flipped
            ReadIterator iter = setB.readOnlyIterator();
            while (iter.hasNext()) {
                ReadHeadInfo p = iter.next();
                int newPOffset = otherLength - 1 - p.getOffset();
                this.add(p.getMateId(), p.getLibraryId(), p.getReadId(),
                        (int) ((newPOffset + 1) * lengthFactor - lengthFactor), p.getThisReadSequence(),
                        p.getMateReadSequence());
            }
        }
    }

    /**
     * ATCT --> --> TCTT => ATCTT
     * then TCTT's readSet offset need to prepend the extra length
     * 
     * @param preOffset
     */
    public void prependOffsets(int preOffset) {
        Iterator<ReadHeadInfo> iter = super.resetableIterator();
        while (iter.hasNext()) {
            ReadHeadInfo p = iter.next();
            p.resetOffset(preOffset + p.getOffset());
        }
    }

    /**
     * ATCT --> <-- AAGA => ATCTT
     * then the AAGA's readSet offset need to flip
     * TCTTAAAA original 3, now should be [5 - 1] - 3 = 1;
     * 
     * @param lastOffsetOfMerged
     */
    public void flipOffset(int lastOffsetOfMerged) {
        Iterator<ReadHeadInfo> iter = super.resetableIterator();
        while (iter.hasNext()) {
            ReadHeadInfo p = iter.next();
            p.resetOffset(lastOffsetOfMerged - p.getOffset());
        }
    }

    public SortedSet<ReadHeadInfo> getOffSetRange(int lowOffset, int highOffset) {
        if (lowOffset < 0 || highOffset > ReadHeadInfo.MAX_OFFSET_VALUE) {
            throw new IllegalArgumentException("Invalid range specified: must be 0 < " + ReadHeadInfo.MAX_OFFSET_VALUE
                    + " but saw low: " + lowOffset + ", high: " + highOffset);
        }
        return super.rangeSearch(ReadHeadInfo.getLowerBoundInfo(lowOffset), ReadHeadInfo.getUpperBoundInfo(highOffset));
    }

    public Set<Long> getReadIdSet() {
        HashSet<Long> set = new HashSet<Long>();
        ReadIterator iter = super.readOnlyIterator();
        while (iter.hasNext()) {
            ReadHeadInfo p = iter.next();
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

    public void reset() {
        super.reset();

    }

    /**
     * Please do not change the iterator's value,
     * Unless you are sure the memory and disk inconsistency will not cause any trouble
     */
    @Override
    public Iterator<ReadHeadInfo> iterator() {
        return super.readOnlyIterator();
    }

}
