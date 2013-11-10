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
    private VKmer mate0ReadSequence = null;
    private VKmer mate1ReadSequence = null;

    public ReadHeadInfo() {
        this.value = 0;
        this.mate0ReadSequence = new VKmer();
        this.mate1ReadSequence = new VKmer();
    }

    public ReadHeadInfo(byte mateId, long readId, int offset, VKmer mate0ReadSequence, VKmer mate1ReadSequence) {
        set(mateId, readId, offset, mate0ReadSequence, mate1ReadSequence);
    }

    public ReadHeadInfo(ReadHeadInfo other) {
        set(other);
    }

    public ReadHeadInfo(long uuid, VKmer mate0ReadSequence, VKmer mate1ReadSequence) {
        set(uuid, mate0ReadSequence, mate1ReadSequence);
    }

    public void set(long uuid, VKmer mate0ReadSequence, VKmer mate1ReadSequence) {
        value = uuid;
        if (mate0ReadSequence != null)
            this.mate0ReadSequence.setAsCopy(mate0ReadSequence);
        if (mate1ReadSequence != null)
            this.mate1ReadSequence.setAsCopy(mate1ReadSequence);
    }

    public static long makeUUID(byte mateId, long readId, int posId) {
        return (readId << 17) + ((posId & 0xFFFF) << 1) + (mateId & 0b1);
    }
    
    public void set(byte mateId, long readId, int posId) {
        value = makeUUID(mateId, readId, posId);
    }
    
    public void set(byte mateId, long readId, int posId, VKmer mate0ReadSequence, VKmer mate1ReadSequence) {
        value = makeUUID(mateId, readId, posId);
        set(value, mate0ReadSequence, mate1ReadSequence);
    }

    public void set(ReadHeadInfo head) {
        set(head.value, head.mate0ReadSequence, head.mate1ReadSequence);
    }

    public int getLengthInBytes() {
        return ReadHeadInfo.ITEM_SIZE + mate0ReadSequence.getLength() + mate1ReadSequence.getLength();
    }

    public long asLong() {
        return value;
    }

    public VKmer getReadSequenceSameWithMateId() {
        if (getMateId() == 0)
            return this.mate0ReadSequence;
        else
            return this.mate1ReadSequence;
    }

    public VKmer getReadSequenceDiffWithMateId() {
        if (getMateId() == 0)
            return this.mate1ReadSequence;
        else
            return this.mate0ReadSequence;
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
        mate0ReadSequence.readFields(in);
        mate1ReadSequence.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(value);
        mate0ReadSequence.write(out);
        mate1ReadSequence.write(out);
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
        return this.getReadId() + "-" + this.getOffset() + "_" + (this.getMateId()) + "mate0rSeq: "
                + this.mate0ReadSequence.toString() + "mate1rSeq: " + this.mate1ReadSequence.toString();
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
