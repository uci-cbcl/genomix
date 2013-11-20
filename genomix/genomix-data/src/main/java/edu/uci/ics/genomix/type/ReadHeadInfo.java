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
    private VKmer readSequence;
    private VKmer mateReadSequence;

    public ReadHeadInfo() {
        this.value = 0;
        this.readSequence = new VKmer();
        this.mateReadSequence = new VKmer();
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

    public void set(long uuid, VKmer thisReadSequence, VKmer mateReadSequence) {
        value = uuid;
        if (thisReadSequence == null) {
            this.readSequence = null;
        } else {
            this.readSequence.setAsCopy(thisReadSequence);
        }
        if (mateReadSequence == null) {
            this.mateReadSequence = null;
        } else {
            this.mateReadSequence.setAsCopy(mateReadSequence);
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
        set(head.value, head.readSequence, head.mateReadSequence);
    }

    public int getLengthInBytes() {
        int totalBytes = ReadHeadInfo.ITEM_SIZE;
        totalBytes += readSequence != null ? readSequence.getLength() : 0;
        totalBytes += mateReadSequence != null ? mateReadSequence.getLength() : 0;
        return totalBytes;
    }

    public long asLong() {
        return value;
    }

    public VKmer getThisReadSequence() {
        if (this.readSequence == null) {
            this.readSequence = new VKmer();
        }
        return this.readSequence;
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
            getMateReadSequence().readFields(in);
        }
    }

    protected byte getActiveFields() {
        byte fields = 0;
        if (this.readSequence != null && this.readSequence.getKmerLetterLength() > 0) {
            fields |= READHEADINFO_FIELDS.THIS_READSEQUENCE;
        }
        if (this.mateReadSequence != null && this.mateReadSequence.getKmerLetterLength() > 0) {
            fields |= READHEADINFO_FIELDS.THAT_READSEQUENCE;
        }
        return fields;
    }

    public void write(ReadHeadInfo headInfo, DataOutput out) throws IOException {
        out.writeByte(headInfo.getActiveFields());
        System.out.println();
        out.writeLong(headInfo.value);
        System.out.println();
        if (this.readSequence != null && this.readSequence.getKmerLetterLength() > 0) {
            System.out.println(headInfo.readSequence.toString());
            headInfo.readSequence.write(out);
        }
        if (this.mateReadSequence != null && this.mateReadSequence.getKmerLetterLength() > 0) {
            System.out.println(headInfo.mateReadSequence.toString());
            headInfo.mateReadSequence.write(out);
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
        return this.getReadId() + "-" + this.getOffset() + "_" + (this.getMateId()) + " " + "readSeq: "
                + (this.readSequence != null ? this.readSequence.toString() : "") + " " + "mateReadSeq: "
                + (this.mateReadSequence != null ? this.mateReadSequence.toString() : "");
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
