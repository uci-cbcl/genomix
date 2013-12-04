package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.util.Marshal;

public class ReadHeadInfo implements WritableComparable<ReadHeadInfo>, Serializable {
    private static final long serialVersionUID = 1L;
    public static final int ITEM_SIZE = 8;

    public static final int totalBits = 64;
    private static final int bitsForMate = 1;
    private static final int bitsForPosition = 16;
    private static final int readIdShift = bitsForPosition + bitsForMate;
    private static final int positionIdShift = bitsForMate;

    private long value;
    private VKmer thisReadSequence;
    private VKmer mateReadSequence;

    public ReadHeadInfo() {
        this.value = 0;
        this.thisReadSequence = null;
        this.mateReadSequence = null;
    }

    public ReadHeadInfo(byte mateId, long readId, int offset, VKmer thisReadSequence, VKmer mateReadSequence) {
        set(mateId, readId, offset, thisReadSequence, mateReadSequence);
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

    public static long makeUUID(byte mateId, long readId, int posId) {
        return (readId << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
    }

    public void set(byte mateId, long readId, int posId) {
        value = makeUUID(mateId, readId, posId);
    }

    public void set(byte mateId, long readId, int posId, VKmer thisReadSequence, VKmer thatReadSequence) {
        value = makeUUID(mateId, readId, posId);
        set(value, thisReadSequence, thatReadSequence);
    }

    public void set(ReadHeadInfo head) {
        set(head.value, head.thisReadSequence, head.mateReadSequence);
    }

    public int getLengthInBytes() {
        int totalBytes = 0;
        totalBytes += 1; // for the activeField
        totalBytes += ReadHeadInfo.ITEM_SIZE;
        totalBytes += thisReadSequence != null ? thisReadSequence.getLength() : 0;
        totalBytes += mateReadSequence != null ? mateReadSequence.getLength() : 0;
        return totalBytes;
    }

    public VKmer getThisReadSequence() {
        if(this.thisReadSequence == null){
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
        return (byte) (value & 0b1);
    }

    public long getReadId() {
        return value >>> readIdShift;
    }

    public int getOffset() {
        return (int) ((value >>> positionIdShift) & 0xffff);
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
     * String of form "(readId-posID_mate)" where mate is _0 or _1
     */
    @Override
    public String toString() {
        return this.getReadId() + "-" + this.getOffset() + "_" + (this.getMateId()) + " " + "readSeq: "
                + (this.thisReadSequence != null ? this.thisReadSequence.toString() : "null") + " " + "mateReadSeq: "
                + (this.mateReadSequence != null ? this.mateReadSequence.toString() : "null");
    }

    /**
     * sort by readId, then mateId, then offset
     */
    @Override
    public int compareTo(ReadHeadInfo o) {
        if (this.getReadId() == o.getReadId()) {
            if (this.getMateId() == o.getMateId()) {
                return this.getOffset() - o.getOffset();
            }
            return this.getMateId() - o.getMateId();
        }
        return Long.compare(this.getReadId(), o.getReadId());
    }
}
