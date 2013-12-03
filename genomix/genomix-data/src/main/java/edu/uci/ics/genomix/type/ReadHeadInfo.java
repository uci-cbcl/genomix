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

    public ReadHeadInfo(byte mateId, long readId, int offset) {
        set(mateId, readId, offset);
    }

    public ReadHeadInfo(ReadHeadInfo other) {
        set(other);
    }

    public ReadHeadInfo(long uuid) {
        set(uuid);
    }

    public void set(long uuid) {
        value = uuid;
    }

    public static long makeUUID(byte mateId, long readId, int posId) {
        return (readId << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
    }

    public void set(byte mateId, long readId, int posId) {
        value = makeUUID(mateId, readId, posId);
    }

    public void set(ReadHeadInfo head) {
        set(head.value);
    }

    public long asLong() {
        return value;
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

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(value);
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
        return this.getReadId() + "-" + this.getOffset() + "_" + (this.getMateId());
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
