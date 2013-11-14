package edu.uci.ics.genomix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

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
    private VKmer thatReadSequence;

    public ReadHeadInfo() {
        this.value = 0;
        this.thisReadSequence = new VKmer();
        this.thatReadSequence = new VKmer();
    }

    public ReadHeadInfo(byte mateId, long readId, int offset, VKmer thisReadSequence, VKmer thatReadSequence) {
        set(mateId, readId, offset, thisReadSequence, thatReadSequence);
    }

    public ReadHeadInfo(ReadHeadInfo other) {
        set(other);
    }

    public ReadHeadInfo(long uuid, VKmer thisReadSequence, VKmer thatReadSequence) {
        set(uuid, thisReadSequence, thatReadSequence);
    }

    public void set(long uuid, VKmer thisReadSequence, VKmer thatReadSequence) {
        value = uuid;
        if (thisReadSequence == null) {
            this.thisReadSequence = null;
        } else {
            this.thisReadSequence.setAsCopy(thisReadSequence);
        }
        if (thatReadSequence == null) {
            this.thisReadSequence = null;
        } else {
            this.thatReadSequence.setAsCopy(thatReadSequence);
        }
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
        set(head.value, head.thisReadSequence, head.thatReadSequence);
    }

    public int getLengthInBytes() {
        int totalBytes = ReadHeadInfo.ITEM_SIZE;
        totalBytes += thisReadSequence != null ? thisReadSequence.getLength() : 0;
        totalBytes += thatReadSequence != null ? thatReadSequence.getLength() : 0;
        return totalBytes;
    }

    public long asLong() {
        return value;
    }

    public VKmer getThisReadSequence() {
        if (this.thisReadSequence == null)
            return new VKmer();
        else
            return this.thatReadSequence;
    }

    public VKmer getThatReadSequence() {
        if (this.thatReadSequence == null)
            return new VKmer();
        else
            return this.thatReadSequence;
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
        public static final int THIS_READSEQUENCE = 1 << 0;
        public static final int THAT_READSEQUENCE = 1 << 1;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        byte activeFields = in.readByte();
        value = in.readLong();
        if ((activeFields & READHEADINFO_FIELDS.THIS_READSEQUENCE) != 0) {
            getThisReadSequence().readFields(in);
        }
        if ((activeFields & READHEADINFO_FIELDS.THAT_READSEQUENCE) != 0) {
            getThisReadSequence().readFields(in);
        }
    }

    protected byte getActiveFields() {
        byte fields = 0;
        if (this.thisReadSequence != null && this.thisReadSequence.getKmerByteLength() > 0) {
            fields |= READHEADINFO_FIELDS.THIS_READSEQUENCE;
        }
        if (this.thatReadSequence != null && this.thatReadSequence.getKmerByteLength() > 0) {
            fields |= READHEADINFO_FIELDS.THAT_READSEQUENCE;
        }
        return fields;
    }

    public void write(ReadHeadInfo headInfo, DataOutput out) throws IOException {
        out.writeByte(headInfo.getActiveFields());
        out.writeLong(headInfo.value);
        if (this.thisReadSequence != null && this.thisReadSequence.getKmerByteLength() > 0) {
            headInfo.thisReadSequence.write(out);
        }
        if (this.thatReadSequence != null && this.thatReadSequence.getKmerByteLength() > 0) {
            headInfo.thatReadSequence.write(out);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        write(this, out);
    }

    @Override
    public int hashCode() {
        return Long.valueOf(value).hashCode(); //TODO I don't think need add readSequence's hashcode; Nan.
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ReadHeadInfo))
            return false;
        return ((ReadHeadInfo) o).value == this.value; //TODO I don't think need to compare readSequence, otherwise it's hard to find readHeadInfo in the treeSet

    }

    /*
     * String of form "(readId-posID_mate)" where mate is _0 or _1
     */
    @Override
    public String toString() {
        return this.getReadId() + "-" + this.getOffset() + "_" + (this.getMateId()) + "thisSeq: "
                + this.thisReadSequence.toString() + "thatSeq: " + this.thatReadSequence.toString();
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
        //TODO do we need to compare the read sequence? I don't think so. Nan.
    }

}
