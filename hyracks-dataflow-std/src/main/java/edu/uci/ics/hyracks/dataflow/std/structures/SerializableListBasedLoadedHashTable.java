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
package edu.uci.ics.hyracks.dataflow.std.structures;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.FrameHelper;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

/**
 * An implementation of a linked-list based hash table.
 * <p/>
 * Such a loaded hash table consists of two parts in its memory organization: a set of frames for hash table headers,
 * and a set of frames for the hash table contents.
 * <p/>
 * The header frames contain one reference for each hash table entry. The reference size is fixed, so a random access to
 * a reference given the entry id is possible.
 * <p/>
 * The content frames contain all records inserted into the hash table. The frame structure is the same as an ordinary
 * frame in Hyracks system, however in order to handle the hash collision, a record reference is added at the end of
 * each record inserted.
 * <p/>
 */
public class SerializableListBasedLoadedHashTable implements ISerializableLoadedTable {

    private static final int INT_SIZE = 4;

    private final ByteBuffer[] headers;
    private final ByteBuffer[] contents;

    private final ILoadedHashTableMatchComparator matchComparator;

    private final IHyracksTaskContext ctx;
    private final int frameCapacity;
    private int currentLargestFrameIndex = 0;
    private int totalTupleCount = 0;

    public SerializableListBasedLoadedHashTable(int tableSize, int framesLimit, final IHyracksTaskContext ctx,
            final ILoadedHashTableMatchComparator matchComparator) {
        this.ctx = ctx;
        this.frameCapacity = ctx.getFrameSize();

        int residual = tableSize * INT_SIZE * 2 % frameCapacity == 0 ? 0 : 1;
        this.headers = new ByteBuffer[tableSize * INT_SIZE * 2 / frameCapacity + residual];

        this.contents = new ByteBuffer[framesLimit];
        this.currentLargestFrameIndex = 0;

        this.matchComparator = matchComparator;

    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable#getFrameCount()
     */
    @Override
    public int getFrameCount() {
        return headers.length + contents.length;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable#getTupleCount()
     */
    @Override
    public int getTupleCount() {
        return totalTupleCount;
    }

    /**
     * Reset the hash table. All headers are reset to contain -1, and the tuple counter of each
     * content frame is reset to 0.
     */
    @Override
    public void reset() {
        // reset the head frames
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null)
                resetHeader(i);
        }
        // reset the tuple counter for each content frame
        for (int i = 0; i < contents.length; i++) {
            if (contents[i] != null) {
                contents[i].putInt(FrameHelper.getTupleCountOffset(frameCapacity), 0);
            }
        }
        // reset the current content frame index
        currentLargestFrameIndex = 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.structures.ISerializableTable#close()
     */
    @Override
    public void close() {
        for (int i = 0; i < headers.length; i++) {
            headers[i] = null;
        }
        for (int i = 0; i < contents.length; i++) {
            contents[i] = null;
        }
    }

    private void resetHeader(int headerFrameIndex) {
        for (int i = 0; i < frameCapacity; i += INT_SIZE) {
            headers[headerFrameIndex].putInt(i, -1);
        }
    }

    private int getHeaderFrameIndex(int entry) {
        int frameIndex = entry * 2 / frameCapacity;
        return frameIndex;
    }

    private int getHeaderFrameOffset(int entry) {
        int offset = entry * 2 % frameCapacity;
        return offset;
    }

    @Override
    public boolean insert(int entry, byte[] data, int offset, int length) throws HyracksDataException {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderFrameOffset(entry);

        if (headers[headerFrameIndex] == null) {
            headers[headerFrameIndex] = ctx.allocateFrame();
            resetHeader(headerFrameIndex);
        }

        ByteBuffer pointerFrame = headers[headerFrameIndex];
        int refOffset = headerFrameOffset;
        int entryFrameIndex = pointerFrame.getInt(refOffset);
        int entryTupleIndex = pointerFrame.getInt(refOffset + INT_SIZE);

        int prevFrameIndex;
        while (entryFrameIndex >= 0) {
            // Move to the next record in this entry following the linked list
            refOffset = getTupleEndOffset(contents[entryFrameIndex], entryTupleIndex) - 2 * INT_SIZE;
            prevFrameIndex = entryFrameIndex;
            entryFrameIndex = pointerFrame.getInt(refOffset);
            entryTupleIndex = pointerFrame.getInt(refOffset + INT_SIZE);
            pointerFrame = contents[prevFrameIndex];
        }

        // find an available slot for insertion
        for (int i = 0; i < currentLargestFrameIndex; i++) {
            if (!insertTupleIntoFrame(i, data, offset, length, pointerFrame, refOffset)) {
                continue;
            } else {
                totalTupleCount++;
                return true;
            }
        }

        // if we can allocate more frame for the hash table
        if (currentLargestFrameIndex < contents.length) {
            currentLargestFrameIndex++;
            if (contents[currentLargestFrameIndex] == null) {
                contents[currentLargestFrameIndex] = ctx.allocateFrame();
            }
            if (!insertTupleIntoFrame(currentLargestFrameIndex, data, offset, length, pointerFrame, refOffset)) {
                throw new HyracksDataException(
                        "Failed to insert a new record into the hash table: not enough space in a frame");
            } else {
                totalTupleCount++;
                return true;
            }
        }

        return false;
    }

    private boolean insertTupleIntoFrame(int contentFrameIndex, byte[] data, int offset, int length,
            ByteBuffer pointerFrame, int refOffset) {
        ByteBuffer frame = contents[contentFrameIndex];
        int tupleCount = frame.getInt(FrameHelper.getTupleCountOffset(frameCapacity));
        int endOffsetPosition = FrameHelper.getTupleCountOffset(frameCapacity) - tupleCount * INT_SIZE;
        int lastTupleEndOffset = tupleCount == 0 ? 0 : frame.getInt(endOffsetPosition);
        if (lastTupleEndOffset + length < endOffsetPosition - INT_SIZE) {
            // copy the record
            System.arraycopy(data, offset, frame.array(), lastTupleEndOffset, length);
            // add two references
            frame.putInt(lastTupleEndOffset + length, -1);
            frame.putInt(lastTupleEndOffset + length + INT_SIZE, -1);
            // update tuple counter
            tupleCount++;
            frame.putInt(FrameHelper.getTupleCountOffset(frameCapacity), tupleCount);
            // update new tuple end offset
            frame.putInt(endOffsetPosition - INT_SIZE, lastTupleEndOffset + length + 2 * INT_SIZE);

            // update the reference from previous record
            pointerFrame.putInt(refOffset, contentFrameIndex);
            pointerFrame.putInt(refOffset + INT_SIZE, tupleCount - 1);

            return true;
        }
        return false;
    }

    @Override
    public void getTuplePointer(int entry, int offset, TuplePointer tuplePointer) {
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderFrameOffset(entry);

        int refOffset = headerFrameOffset;
        int entryFrameIndex = headers[headerFrameIndex].getInt(refOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(refOffset + INT_SIZE);

        while (offset > 0 && entryFrameIndex >= 0) {
            // Move to the next record in this entry following the linked list
            refOffset = getTupleEndOffset(contents[entryFrameIndex], entryTupleIndex) - 2 * INT_SIZE;
            entryFrameIndex = contents[entryFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[entryFrameIndex].getInt(refOffset + INT_SIZE);
            offset--;
        }
        if (offset == 0) {
            tuplePointer.frameIndex = entryFrameIndex;
            tuplePointer.tupleIndex = entryTupleIndex;
        } else {
            tuplePointer.frameIndex = -1;
            tuplePointer.tupleIndex = -1;
        }
    }

    @Override
    public ByteBuffer getContentFrame(int contentFrameIndex) {
        if (contentFrameIndex >= 0 || contentFrameIndex < currentLargestFrameIndex) {
            return contents[contentFrameIndex];
        }
        return null;
    }

    private int getTupleOffset(ByteBuffer frame, int tupleIndex) {
        int tupleCount = frame.getInt(frame.capacity() - INT_SIZE);
        if (tupleIndex < 0 || tupleIndex >= tupleCount) {
            return -1;
        }
        return tupleIndex == 0 ? 0 : frame.getInt(FrameHelper.getTupleCountOffset(frameCapacity) - 4 * tupleIndex);
    }

    private int getTupleEndOffset(ByteBuffer frame, int tupleIndex) {
        return frame.getInt(FrameHelper.getTupleCountOffset(frameCapacity) - 4 * (tupleIndex + 1));
    }

    @Override
    public int findMatch(int entry, FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {
        int offset = 0;
        int headerFrameIndex = getHeaderFrameIndex(entry);
        int headerFrameOffset = getHeaderFrameOffset(entry);

        int refOffset = headerFrameOffset;
        int entryFrameIndex = headers[headerFrameIndex].getInt(refOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(refOffset + INT_SIZE);

        while (entryFrameIndex >= 0) {
            int recordStartOffset = getTupleOffset(contents[entryFrameIndex], entryTupleIndex);
            int recordEndOffset = getTupleEndOffset(contents[entryFrameIndex], entryTupleIndex);
            int recordLength = recordEndOffset - recordStartOffset - 2 * INT_SIZE;
            if (matchComparator.compare(accessor, tupleIndex, contents[entryFrameIndex].array(), refOffset,
                    recordLength) == 0) {
                return offset;
            }
            // Move to the next record in this entry following the linked list
            refOffset = recordEndOffset - 2 * INT_SIZE;
            entryFrameIndex = contents[entryFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[entryFrameIndex].getInt(refOffset + INT_SIZE);
            offset++;
        }

        return -1;
    }
}
