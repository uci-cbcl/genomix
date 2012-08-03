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

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;

public class LoadedHashTableMatchComparator implements ILoadedHashTableMatchComparator {

    private final int[] fields1, fields2;

    private final IBinaryComparator[] comparators;

    private final RecordDescriptor recDesc;

    private final static int INT_SIZE = 4;

    public LoadedHashTableMatchComparator(int[] fields1, int[] fields2, RecordDescriptor recDesc,
            IBinaryComparator[] comparators) {
        this.fields1 = fields1;
        this.fields2 = fields2;
        this.comparators = comparators;
        this.recDesc = recDesc;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.dataflow.std.structures.ILoadedHashTableMatchComparator#compare(edu.uci.ics.hyracks.dataflow
     * .common.comm.io.FrameTupleAccessor, int, byte[], int, int)
     */
    @Override
    public int compare(FrameTupleAccessor accessor, int tupleIndex, byte[] data, int offset, int length)
            throws HyracksDataException {
        int tStart0 = accessor.getTupleStartOffset(tupleIndex);
        int fStartOffset0 = accessor.getFieldSlotsLength() + tStart0;

        int fStartOffset1 = offset + recDesc.getFieldCount() * INT_SIZE;
        for (int i = 0; i < fields1.length; i++) {
            int fStart0 = accessor.getFieldStartOffset(tupleIndex, fields1[i]);
            int fEnd0 = accessor.getFieldEndOffset(tupleIndex, fields1[i]);
            int fLen0 = fEnd0 - fStart0;

            int fStart1 = fields2[i] == 0 ? 0 : getInt(data, offset + INT_SIZE * (fields2[i] - 1));
            int fEnd1 = getInt(data, offset + INT_SIZE * fields2[i]);
            int fLen1 = fEnd1 - fStart1;

            int c = comparators[i].compare(accessor.getBuffer().array(), fStart0 + fStartOffset0, fLen0, data, fStart1
                    + fStartOffset1, fLen1);

            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    private int getInt(byte[] data, int offset) {
        return ((data[offset] & 0xff) << 24) | ((data[offset + 1] & 0xff) << 16) | ((data[offset + 2] & 0xff) << 8)
                | (data[offset + 3] & 0xff);
    }

    @Override
    public int compare(byte[] data1, int offset1, int length1, byte[] data2, int offset2, int length2)
            throws HyracksDataException {
        int fStartOffset1 = offset1 + recDesc.getFieldCount() * INT_SIZE;
        int fStartOffset2 = offset2 + recDesc.getFieldCount() * INT_SIZE;

        for (int i = 0; i < fields2.length; i++) {
            int fStart1 = fields2[i] == 0 ? 0 : getInt(data1, offset1 + INT_SIZE * (fields2[i] - 1));
            int fEnd1 = getInt(data1, offset1 + INT_SIZE * fields2[i]);
            int fLen1 = fEnd1 - fStart1;

            int fStart2 = fields2[i] == 0 ? 0 : getInt(data2, offset2 + INT_SIZE * (fields2[i] - 1));
            int fEnd2 = getInt(data2, offset2 + INT_SIZE * fields2[i]);
            int fLen2 = fEnd2 - fStart2;

            int c = comparators[i]
                    .compare(data1, fStartOffset1 + fStart1, fLen1, data2, fStartOffset2 + fStart2, fLen2);

            if (c != 0) {
                return c;
            }
        }

        return 0;
    }
}
