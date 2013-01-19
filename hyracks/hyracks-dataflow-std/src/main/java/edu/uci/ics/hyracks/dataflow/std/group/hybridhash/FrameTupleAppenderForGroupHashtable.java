/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;

public class FrameTupleAppenderForGroupHashtable {
    private final int frameSize;

    private ByteBuffer buffer;

    private int tupleCount;

    private int tupleDataEndOffset;

    public FrameTupleAppenderForGroupHashtable(int frameSize) {
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

    public boolean append(int[] fieldSlots, byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + fieldSlots.length * 4 + length + 2 * 4 + 4 + (tupleCount + 1) * 4 <= frameSize) {
            for (int i = 0; i < fieldSlots.length; ++i) {
                buffer.putInt(tupleDataEndOffset + i * 4, fieldSlots[i]);
            }
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset + fieldSlots.length * 4, length);
            buffer.putInt(tupleDataEndOffset + fieldSlots.length * 4 + length, -1);
            buffer.putInt(tupleDataEndOffset + fieldSlots.length * 4 + length + 4, -1);
            tupleDataEndOffset += fieldSlots.length * 4 + length + 2 * 4;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + length + 2 * 4 + 4 + (tupleCount + 1) * 4 <= frameSize) {
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset, length);
            buffer.putInt(tupleDataEndOffset + length, -1);
            buffer.putInt(tupleDataEndOffset + length + 4, -1);
            tupleDataEndOffset += length + 2 * 4;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean appendSkipEmptyField(int[] fieldSlots, byte[] bytes, int offset, int length) {
        if (tupleDataEndOffset + fieldSlots.length * 4 + length + 2 * 4 + 4 + (tupleCount + 1) * 4 <= frameSize) {
            int effectiveSlots = 0;
            for (int i = 0; i < fieldSlots.length; ++i) {
                if (fieldSlots[i] > 0) {
                    buffer.putInt(tupleDataEndOffset + i * 4, fieldSlots[i]);
                    effectiveSlots++;
                }
            }
            System.arraycopy(bytes, offset, buffer.array(), tupleDataEndOffset + effectiveSlots * 4, length);
            buffer.putInt(tupleDataEndOffset + effectiveSlots * 4 + length, -1);
            buffer.putInt(tupleDataEndOffset + effectiveSlots * 4 + length + 4, -1);
            tupleDataEndOffset += effectiveSlots * 4 + length + 2 * 4;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(IFrameTupleAccessor tupleAccessor, int tStartOffset, int tEndOffset) {
        int length = tEndOffset - tStartOffset;
        if (tupleDataEndOffset + length + 2 * 4 + 4 + (tupleCount + 1) * 4 <= frameSize) {
            ByteBuffer src = tupleAccessor.getBuffer();
            System.arraycopy(src.array(), tStartOffset, buffer.array(), tupleDataEndOffset, length);
            buffer.putInt(tupleDataEndOffset + length, -1);
            buffer.putInt(tupleDataEndOffset + length + 4, -1);
            tupleDataEndOffset += length + 2 * 4;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleCount + 1), tupleDataEndOffset);
            ++tupleCount;
            buffer.putInt(FrameHelper.getTupleCountOffset(frameSize), tupleCount);
            return true;
        }
        return false;
    }

    public boolean append(IFrameTupleAccessor tupleAccessor, int tIndex) {
        int tStartOffset = tupleAccessor.getTupleStartOffset(tIndex);
        int tEndOffset = tupleAccessor.getTupleEndOffset(tIndex);
        return append(tupleAccessor, tStartOffset, tEndOffset);
    }

    public int getTupleCount() {
        return tupleCount;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }
}

