/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class TupleInFrameAccessor {

    private final RecordDescriptor recordDescriptor;

    private ByteBuffer buffer;

    private int tupleOffset;

    public TupleInFrameAccessor(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    public void reset(ByteBuffer buffer, int tupleOffset) {
        this.buffer = buffer;
        this.tupleOffset = tupleOffset;
    }

    public int getTupleStartOffset() {
        return tupleOffset;
    }

    public int getTupleEndOffset() {
        return tupleOffset + getFieldCount() * 4 + buffer.getInt(tupleOffset + (getFieldCount() - 1) * 4);
    }
    
    public int getFieldStartOffset(int fIdx) {
        return fIdx == 0 ? 0 : buffer.getInt(getTupleStartOffset() + (fIdx - 1) * 4);
    }

    public int getFieldEndOffset(int fIdx) {
        return buffer.getInt(getTupleStartOffset() + fIdx * 4);
    }
    
    public int getFieldLength(int fIdx) {
        return getFieldEndOffset(fIdx) - getFieldStartOffset(fIdx);
    }
    
    public int getFieldSlotsLength() {
        return getFieldCount() * 4;
    }

    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }
    
    public ByteBuffer getBuffer(){
        return buffer;
    }
    
    public int getTupleLength(){
        return getTupleEndOffset() - tupleOffset;
    }

}
