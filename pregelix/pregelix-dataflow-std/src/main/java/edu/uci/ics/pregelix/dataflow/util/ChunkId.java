package edu.uci.ics.pregelix.dataflow.util;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.ShortPointable;

public class ChunkId {
    public static ITypeTraits TypeTrait = ShortPointable.TYPE_TRAITS;
    public static IBinaryComparatorFactory BinaryComparatorFactory = PointableBinaryComparatorFactory
            .of(ShortPointable.FACTORY);

    private short value;

    public ChunkId(short v) {
        value = v;
    }

    public short getId() {
        return value;
    }

    public int getLength() {
        return TypeTrait.getFixedLength();
    }

    public void increaseId() {
        ++value;
    }

    public void reset(byte[] fieldData, int fieldStart, int fieldLength) {
        value = ShortPointable.getShort(fieldData, fieldStart);
    }

    public boolean isFirstChunk() {
        return value == 0;
    }

    public boolean checkIfMyNextChunk(byte[] fieldData, int fieldStart) {
        short next = ShortPointable.getShort(fieldData, fieldStart);
        return value + 1 == next;
    }
}
