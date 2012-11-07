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
package edu.uci.ics.hyracks.dataflow.std.group.struct;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;

public class TupleAccessHelper {

    private final static int INT_SIZE = 4;

    private final RecordDescriptor recordDescriptor;

    public TupleAccessHelper(RecordDescriptor recordDescriptor) {
        this.recordDescriptor = recordDescriptor;
    }

    public int getFieldCount() {
        return recordDescriptor.getFieldCount();
    }

    public int getFieldSlotLength() {
        return getFieldCount() * 4;
    }

    public int getFieldEndOffset(ByteBuffer buf, int tupleStartOffset, int fieldIndex) {
        return buf.getInt(tupleStartOffset + fieldIndex * INT_SIZE);
    }

    public int getFieldStartOffset(ByteBuffer buf, int tupleStartOffset, int fieldIndex) {
        return fieldIndex == 0 ? 0 : buf.getInt(tupleStartOffset + (fieldIndex - 1) * INT_SIZE);
    }

    public int getFieldLength(ByteBuffer buf, int tupleStartOffset, int fieldIndex) {
        return getFieldEndOffset(buf, tupleStartOffset, fieldIndex)
                - getFieldStartOffset(buf, tupleStartOffset, fieldIndex);
    }

    /**
     * Get the end offset of the actual tuple. Note that the tuple does not contain any other
     * structure, like the hash table references.
     * 
     * @param buf
     * @param tupleStartOffset
     * @return
     */
    public int getTupleEndOffset(ByteBuffer buf, int tupleStartOffset) {
        return getFieldEndOffset(buf, tupleStartOffset, getFieldCount() - 1) + tupleStartOffset + getFieldSlotLength();
    }

    public int getTupleLength(ByteBuffer buf, int tupleStartOffset) {
        return getTupleEndOffset(buf, tupleStartOffset) - tupleStartOffset;
    }
}
