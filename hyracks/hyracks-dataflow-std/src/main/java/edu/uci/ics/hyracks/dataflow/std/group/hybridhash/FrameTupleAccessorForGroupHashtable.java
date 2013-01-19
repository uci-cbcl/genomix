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
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class FrameTupleAccessorForGroupHashtable implements IFrameTupleAccessor {
    private final int frameSize;
    private final RecordDescriptor recordDescriptor;

    private final static int INT_SIZE = 4;

    private ByteBuffer buffer;

    public FrameTupleAccessorForGroupHashtable(int frameSize, RecordDescriptor recordDescriptor) {
        this.frameSize = frameSize;
        this.recordDescriptor = recordDescriptor;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getFieldCount()
     */
    @Override
    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getFieldSlotsLength()
     */
    @Override
    public int getFieldSlotsLength() {
        return getFieldCount() * 4;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getFieldEndOffset(int, int)
     */
    @Override
    public int getFieldEndOffset(int tupleIndex, int fIdx) {
        return buffer.getInt(getTupleStartOffset(tupleIndex) + fIdx * 4);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getFieldStartOffset(int, int)
     */
    @Override
    public int getFieldStartOffset(int tupleIndex, int fIdx) {
        return fIdx == 0 ? 0 : buffer.getInt(getTupleStartOffset(tupleIndex) + (fIdx - 1) * 4);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getFieldLength(int, int)
     */
    @Override
    public int getFieldLength(int tupleIndex, int fIdx) {
        return getFieldEndOffset(tupleIndex, fIdx) - getFieldStartOffset(tupleIndex, fIdx);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getTupleEndOffset(int)
     */
    @Override
    public int getTupleEndOffset(int tupleIndex) {
        return buffer.getInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleIndex + 1)) - 2 * INT_SIZE;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getTupleStartOffset(int)
     */
    @Override
    public int getTupleStartOffset(int tupleIndex) {
        return tupleIndex == 0 ? 0 : buffer.getInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * tupleIndex);
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getTupleCount()
     */
    @Override
    public int getTupleCount() {
        return buffer.getInt(FrameHelper.getTupleCountOffset(frameSize));
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#getBuffer()
     */
    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor#reset(java.nio.ByteBuffer)
     */
    @Override
    public void reset(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public int getTupleHashReferenceOffset(int tupleIndex) {
        return getTupleEndOffset(tupleIndex);
    }

    public int getTupleEndOffsetWithHashReference(int tupleIndex) {
        return buffer.getInt(FrameHelper.getTupleCountOffset(frameSize) - 4 * (tupleIndex + 1));
    }

    public int getHashReferenceNextFrameIndex(int tupleIndex) {
        return buffer.getInt(getTupleHashReferenceOffset(tupleIndex));
    }

    public int getHashReferenceNextTupleIndex(int tupleIndex) {
        return buffer.getInt(getTupleHashReferenceOffset(tupleIndex) + INT_SIZE);
    }

}
