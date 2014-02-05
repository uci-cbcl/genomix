package edu.uci.ics.pregelix.dataflow.util;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;

public class KeyChunkValueFrameTupleAppender {
    private final int frameSize;

    private ByteBuffer buffer;

    private int tupleCount;

    private int tupleDataEndOffset;

    public KeyChunkValueFrameTupleAppender(int frameSize) {
        this.frameSize = frameSize;
    }

    public void reset(ByteBuffer buffer, boolean clear) {
        this.buffer = buffer;
        if (clear) {
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), 0);
            tupleCount = 0;
            tupleDataEndOffset = 0;
        } else {
            tupleCount = buffer.getInt(FrameHelper.getTupleCountOffset(frameSize));
            tupleDataEndOffset = tupleCount == 0 ? 0 : buffer.getInt(FrameHelper.getTupleCountOffset(frameSize)
                    - tupleCount * 4);
        }
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public boolean append(ArrayTupleBuilder keyBuilder, ChunkId id, byte[] bs, int offset, int bsLength) {
        if (tupleDataEndOffset + keyBuilder.getSize() + 4 * keyBuilder.getFieldEndOffsets().length + 4 + 2 + 4
                + bsLength > frameSize) {
            return false;
        }
        int size = 0;
        // write Offsets
        int slot0 = keyBuilder.getFieldEndOffsets().length * 4;
        for (int i = 0; i < keyBuilder.getFieldEndOffsets().length; ++i) {
            buffer.putInt(tupleDataEndOffset + i * 4, keyBuilder.getFieldEndOffsets()[i]);
        }
        size += slot0;
        buffer.putInt(tupleDataEndOffset + size, ChunkId.TypeTrait.getFixedLength());
        size += 4;
        buffer.putInt(tupleDataEndOffset + size, bsLength);
        size += 4;

        // write real data
        System.arraycopy(keyBuilder.getFieldData().getByteArray(), 0, buffer.array(), tupleDataEndOffset + size,
                keyBuilder.getFieldData().getLength());
        size += keyBuilder.getFieldData().getLength();

        buffer.putShort(tupleDataEndOffset + size, id.getID());
        size += id.getLength();

        System.arraycopy(bs, offset, buffer.array(), tupleDataEndOffset + size, bsLength);
        size += bsLength;

        // update frame meta data;
        tupleDataEndOffset += size;
        buffer.putInt(FrameHelper.getTupleEndOffsetOffset(frameSize, tupleCount), tupleDataEndOffset);
        ++tupleCount;
        buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
        return true;
    }
}
