package edu.uci.ics.pregelix.dataflow.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

public class ChunkId implements ITupleReference {
    public static ITypeTraits TypeTrait = ShortPointable.TYPE_TRAITS;
    public static IBinaryComparatorFactory BinaryComparatorFactory = PointableBinaryComparatorFactory
            .of(ShortPointable.FACTORY);

    private byte[] fieldData;
    private int fieldStart;

    public ChunkId(short v) {
        fieldData = new byte[TypeTrait.getFixedLength()];
        fieldStart = 0;
        ShortPointable.setShort(fieldData, fieldStart, v);
    }

    public short getID() {
        return ShortPointable.getShort(fieldData, fieldStart);
    }

    public int getLength() {
        return TypeTrait.getFixedLength();
    }

    public void increaseId() {
        ShortPointable.setShort(fieldData, fieldStart, (short) (ShortPointable.getShort(fieldData, fieldStart) + 1));;
    }

    public void reset(byte[] fieldData, int fieldStart, int fieldLength) {
        this.fieldData = fieldData;
        this.fieldStart = fieldStart;
    }

    public void reset() {
        ShortPointable.setShort(fieldData, fieldStart, (short) 0);
    }

    public boolean isFirstChunk() {
        return getID() == 0;
    }

    public boolean checkIfMyNextChunk(byte[] fieldData, int fieldStart) {
        short next = ShortPointable.getShort(fieldData, fieldStart);
        return getID() + 1 == next;
    }

    @Override
    public int getFieldCount() {
        return 1;
    }

    @Override
    public byte[] getFieldData(int fIdx) {
        return fieldData;
    }

    @Override
    public int getFieldStart(int fIdx) {
        return fieldStart;
    }

    @Override
    public int getFieldLength(int fIdx) {
        return TypeTrait.getFixedLength();
    }
}
