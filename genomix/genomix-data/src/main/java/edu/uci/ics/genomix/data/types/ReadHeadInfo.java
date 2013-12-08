package edu.uci.ics.genomix.data.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.data.utils.Marshal;

public class ReadHeadInfo implements WritableComparable<ReadHeadInfo>, Serializable {
    private static final long serialVersionUID = 1L;
    public static final int ITEM_SIZE = 8;

    // the internal long is divided up into several pieces:
    public static final int totalBits = 64;
    private static final int bitsForMate = 1;
    private static final int bitsForLibrary = 4;
    private static final int bitsForOffset = 24;
    private static final int bitsForReadId = totalBits - bitsForOffset - bitsForLibrary - bitsForMate;
    // the offset (position) covers the leading bits, followed by the library, then mate, and finally, the readid
    // to recover each value, >>> by:
    private static final int offsetShift = bitsForLibrary + bitsForMate + bitsForReadId;
    private static final int libraryIdShift = bitsForMate + bitsForReadId;
    private static final int mateIdShift = bitsForReadId;
    private static final int readIdShift = 0;

    protected static ReadHeadInfo getLowerBoundInfo(int offset, boolean mate) {
        if (mate) {
            return new ReadHeadInfo((byte) 1, (byte) 0, 0l, offset, null, null);
        }
        return new ReadHeadInfo((byte) 0, (byte) 0, 0l, offset, null, null);
    }

    protected static ReadHeadInfo getUpperBoundInfo(int offset, boolean mate) {
        if (mate) {
            return new ReadHeadInfo((byte) 1, (byte) Byte.MAX_VALUE, Long.MAX_VALUE, offset, null, null);
        }
        return new ReadHeadInfo((byte) 0, (byte) Byte.MAX_VALUE, Long.MAX_VALUE, offset, null, null);
    }

    private long value;
    private VKmer thisReadSequence;
    private VKmer mateReadSequence;

    public ReadHeadInfo() {
        this.value = 0;
        this.thisReadSequence = null;
        this.mateReadSequence = null;
    }

    public ReadHeadInfo(byte mateId, byte libraryId, long readId, int offset, VKmer thisReadSequence, VKmer mateReadSequence) {
        set(mateId, libraryId, readId, offset, thisReadSequence, mateReadSequence);
    }

    public ReadHeadInfo(ReadHeadInfo other) {
        set(other);
    }

    public ReadHeadInfo(long uuid, VKmer thisReadSequence, VKmer mateReadSequence) {
        set(uuid, thisReadSequence, mateReadSequence);
    }

    public ReadHeadInfo(byte[] data, int offset) {
        byte activeFields = data[offset];
        offset++;
        long uuid = Marshal.getLong(data, offset);
        setUUID(uuid);
        offset += ReadHeadInfo.ITEM_SIZE;
        getThisReadSequence().setAsCopy(data, offset);
        offset += getThisReadSequence().getLength();
        if ((activeFields & READHEADINFO_FIELDS.MATE_READSEQUENCE) != 0) {
            getMateReadSequence().setAsCopy(data, offset);
            offset += getMateReadSequence().getLength();
        }
    }

    public void set(long uuid, VKmer thisReadSequence, VKmer mateReadSequence) {
        value = uuid;
        if (thisReadSequence == null) {
            throw new IllegalArgumentException("thisReadSequence can not be null!");
        }
        getThisReadSequence().setAsCopy(thisReadSequence);
        if (mateReadSequence == null) {
            this.mateReadSequence = null;
        } else {
            getMateReadSequence().setAsCopy(mateReadSequence);
        }
    }

    public void setUUID(long uuid) {
        value = uuid;
    }

    public static long makeUUID(byte mateId, byte libraryId, long readId, int offset) {
        // check to make sure we aren't losing any information
        if (mateId != (mateId & ~(-1 << bitsForMate)))
            throw new IllegalArgumentException("byte specified for mateId will lose some of its bits when saved! (was: " + mateId + " but only allowed " + bitsForMate + " bits!");
        if (libraryId != (libraryId & ~(-1 << bitsForLibrary)))
            throw new IllegalArgumentException("byte specified for libraryId will lose some of its bits when saved! (was: " + libraryId + " but only allowed " + bitsForLibrary + " bits!");
        if (readId != (readId & ~(-1 << bitsForReadId)))
            throw new IllegalArgumentException("byte specified for readId will lose some of its bits when saved! (was: " + readId + " but only allowed " + bitsForReadId + " bits!");
        if (offset != (offset & ~(-1 << bitsForLibrary)))
            throw new IllegalArgumentException("byte specified for offset will lose some of its bits when saved! (was: " + offset + " but only allowed " + offset + " bits!");
        
        return ((mateId << mateIdShift) + (libraryId << libraryIdShift) + (readId << readIdShift) + (offset << offsetShift));
    }

    public void set(byte mateId, byte libraryId, long readId, int offset) {
        value = makeUUID(mateId, libraryId, readId, offset);
    }

    public void set(byte mateId, byte libraryId, long readId, int offset, VKmer thisReadSequence, VKmer thatReadSequence) {
        value = makeUUID(mateId, libraryId, readId, offset);
        set(value, thisReadSequence, thatReadSequence);
    }

    public void set(ReadHeadInfo head) {
        set(head.value, head.thisReadSequence, head.mateReadSequence);
    }

    public VKmer getThisReadSequence() {
        if (this.thisReadSequence == null) {
            this.thisReadSequence = new VKmer();
        }
        return this.thisReadSequence;
    }

    public VKmer getMateReadSequence() {
        if (this.mateReadSequence == null) {
            this.mateReadSequence = new VKmer();
        }
        return this.mateReadSequence;
    }

    public byte getMateId() {
        return (byte) ((value & ~(-1 << (mateIdShift + bitsForMate))) >>> mateIdShift); // clear leading bits, then shift back to place
    }
    
    public byte getLibraryId() {
        return (byte) ((value & ~(-1 << (libraryIdShift + bitsForLibrary))) >>> libraryIdShift);
    }

    public long getReadId() {
        return ((value & ~(-1 << (readIdShift + bitsForReadId))) >>> readIdShift);
    }

    public int getOffset() {
        return (int) ((value & ~(-1 << (offsetShift + bitsForOffset))) >>> offsetShift);
    }

    public void resetOffset(int offset) {
        value = makeUUID(getMateId(), getLibraryId(), getReadId(), offset);
    }

    protected static class READHEADINFO_FIELDS {
        // thisReadSequence and thatReadSequence
        public static final int MATE_READSEQUENCE = 1 << 0;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte activeFields = in.readByte();
        value = in.readLong();
        getThisReadSequence().readFields(in);
        if ((activeFields & READHEADINFO_FIELDS.MATE_READSEQUENCE) != 0) {
            getMateReadSequence().readFields(in);
        }
    }

    protected byte getActiveFields() {
        byte fields = 0;
        if (this.mateReadSequence != null && this.mateReadSequence.getKmerLetterLength() > 0) {
            fields |= READHEADINFO_FIELDS.MATE_READSEQUENCE;
        }
        return fields;
    }

    public static void write(ReadHeadInfo headInfo, DataOutput out) throws IOException {
        out.writeByte(headInfo.getActiveFields());
        out.writeLong(headInfo.value);
        headInfo.getThisReadSequence().write(out);
        if (headInfo.mateReadSequence != null && headInfo.mateReadSequence.getKmerLetterLength() > 0) {
            headInfo.mateReadSequence.write(out);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        write(this, out);
    }

    @Override
    public int hashCode() {
        return Long.valueOf(value).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ReadHeadInfo))
            return false;
        return ((ReadHeadInfo) o).value == this.value;

    }

    /*
     * String of form "(readId-offset_mate)" where mate is _0 or _1
     */
    @Override
    public String toString() {
        return this.getReadId() + "-" + this.getOffset() + "_" + this.getMateId() + "-" + this.getLibraryId() + " " + "readSeq: "
                + (this.thisReadSequence != null ? this.thisReadSequence.toString() : "null") + " " + "mateReadSeq: "
                + (this.mateReadSequence != null ? this.mateReadSequence.toString() : "null");
    }

    /**
     * sort by bit significance:
     *   offset, library, mate, then readid 
     */
    @Override
    public int compareTo(ReadHeadInfo o) {
        return Long.compare(this.value, o.value);
    }
}
