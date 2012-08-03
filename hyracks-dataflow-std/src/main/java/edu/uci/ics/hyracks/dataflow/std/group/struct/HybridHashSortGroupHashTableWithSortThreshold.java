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

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class HybridHashSortGroupHashTableWithSortThreshold extends HybridHashSortGroupHashTable {

    private final int sortThreshold;

    private int recMaxLength;

    public HybridHashSortGroupHashTableWithSortThreshold(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int[] keys, IBinaryComparator[] comparators, ITuplePartitionComputer tpc,
            INormalizedKeyComputer firstNormalizerComputer, IAggregatorDescriptor aggregator,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc, int sortThreshold) {
        super(ctx, frameLimits, tableSize, keys, comparators, tpc, firstNormalizerComputer, aggregator, inRecDesc,
                outRecDesc);
        this.sortThreshold = sortThreshold;
    }

    protected int sortEntry(int entryID) {

        if (tPointers == null)
            tPointers = new int[INIT_REF_COUNT * PTR_SIZE];
        int ptr = 0;

        int headerFrameIndex = entryID * 2 * INT_SIZE / frameSize;
        int headerFrameOffset = entryID * 2 * INT_SIZE % frameSize;

        if (headers[headerFrameIndex] == null) {
            return 0;
        }

        int entryFrameIndex = headers[headerFrameIndex].getInt(headerFrameOffset);
        int entryTupleIndex = headers[headerFrameIndex].getInt(headerFrameOffset + INT_SIZE);

        do {
            if (entryFrameIndex < 0) {
                break;
            }
            hashtableRecordAccessor.reset(contents[entryFrameIndex]);
            tPointers[ptr * PTR_SIZE] = entryFrameIndex;
            tPointers[ptr * PTR_SIZE + 1] = entryTupleIndex;
            int tStart = hashtableRecordAccessor.getTupleStartOffset(entryTupleIndex);
            int f0StartRel = hashtableRecordAccessor.getFieldStartOffset(entryTupleIndex, internalKeys[0]);
            int f0EndRel = hashtableRecordAccessor.getFieldEndOffset(entryTupleIndex, internalKeys[0]);
            int f0Start = f0StartRel + tStart + hashtableRecordAccessor.getFieldSlotsLength();

            int tEnd = hashtableRecordAccessor.getTupleEndOffset(entryTupleIndex);
            if (tEnd - tStart > recMaxLength) {
                recMaxLength = tEnd - tStart;
            }

            tPointers[ptr * PTR_SIZE + 2] = firstNormalizer == null ? 0 : firstNormalizer.normalize(
                    hashtableRecordAccessor.getBuffer().array(), f0Start, f0EndRel - f0StartRel);

            ptr++;

            if (ptr * PTR_SIZE >= tPointers.length) {
                int[] newTPointers = new int[tPointers.length * 2];
                System.arraycopy(tPointers, 0, newTPointers, 0, tPointers.length);
                tPointers = newTPointers;
            }

            // move to the next record
            int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(entryTupleIndex);
            int prevFrameIndex = entryFrameIndex;
            entryFrameIndex = contents[prevFrameIndex].getInt(refOffset);
            entryTupleIndex = contents[prevFrameIndex].getInt(refOffset + INT_SIZE);

        } while (true);

        // sort records
        if (ptr > sortThreshold) {
            long timer = System.nanoTime();
            sort(sortThreshold, ptr - sortThreshold);
            actualSortTimerInNS += System.nanoTime() - timer;
        }

        return ptr;
    }

    public int getMaxRecordLength() {
        return recMaxLength;
    }

}
