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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.dynamicpartitionalloc;

import java.io.IOException;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class HybridHashStaticRangePartitionGroupHashTable extends HybridHashDynamicDestagingGroupHashTable {

    private final int partitionZeroKeyRange;

    public HybridHashStaticRangePartitionGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int numOfPartitions, int partitionZeroKeyRange, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily tpcf, IAggregatorDescriptor aggregator, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter) throws HyracksDataException {
        this(ctx, frameLimits, tableSize, numOfPartitions, partitionZeroKeyRange, procLevel, keys, comparators, tpcf,
                aggregator, inRecDesc, outRecDesc, outputWriter, true);
    }

    public HybridHashStaticRangePartitionGroupHashTable(IHyracksTaskContext ctx, int framesLimit, int tableSize,
            int numOfPartitions, int partitionZeroKeyRange, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily tpcf, IAggregatorDescriptor aggregator, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter, boolean pickMostToFlush)
            throws HyracksDataException {

        super(ctx, framesLimit, tableSize, numOfPartitions, procLevel, keys, comparators, tpcf, aggregator, inRecDesc,
                outRecDesc, outputWriter, pickMostToFlush);

        this.partitionZeroKeyRange = partitionZeroKeyRange;
    }

    /**
     * Compute the partition id for the given hash key.
     * 
     * @param hashKey
     * @return
     */
    @Override
    protected int partition(int hashKey) {

        if (headers.length == 0) {
            // only partition
            return hashKey % numOfPartitions;
        }

        if (hashKey < partitionZeroKeyRange) {
            return 0;
        } else {
            return (hashKey - partitionZeroKeyRange) % (numOfPartitions - 1) + 1;
        }
    }

    /**
     * Pick a resident partition to flush. This partition will be marked as a spilled partition, and
     * only one frame is assigned to it after flushing.
     * <p/>
     * Note that partition 0 has a lower priority to be spilled if there is any other resident partition. Partition 0
     * would be spilled only when it is the only resident partition, and the table is full.
     * <p/>
     * 
     * @throws HyracksDataException
     */
    @Override
    protected void chooseResidentPartitionToFlush() throws HyracksDataException {
        int partitionToPick = -1;
        for (int i = partitionSpillFlags.nextClearBit(1); i >= 0 && i < numOfPartitions; i = partitionSpillFlags
                .nextClearBit(i + 1)) {
            // as far as the partitionToPick is not initialized, try to 
            // initialize it using a resident partition with at least 2 pages.
            if (partitionToPick < 0) {
                if (partitionSizeInFrame[i] > 1) {
                    partitionToPick = i;
                }
                continue;
            }
            if (pickLargestPartitionForFlush) {
                if (partitionSizeInFrame[i] > partitionSizeInFrame[partitionToPick]) {
                    partitionToPick = i;
                }
            } else {
                if (partitionSizeInFrame[i] < partitionSizeInFrame[partitionToPick] && partitionSizeInFrame[i] > 1) {
                    partitionToPick = i;
                }
            }
        }

        // If no proper partition is found from partitions 1 to P:
        if (partitionToPick < 0 || partitionToPick >= numOfPartitions) {
            if (!partitionSpillFlags.get(0) && partitionSizeInFrame[0] > 1) {
                partitionToPick = 0;
            } else {
                if (partitionSpillFlags.nextClearBit(0) >= numOfPartitions) {
                    return;
                } else {
                    throw new HyracksDataException(
                            "Cannot make more space by spilling a resident partition: each resident partition only occupies one page.");
                }
            }
        }

        spilledRuns++;

        // spill the picked partition
        int frameToFlush = partitionCurrentFrameIndex[partitionToPick];
        if (frameToFlush != END_REF) {
            if (runWriters[partitionToPick] == null) {
                FileReference runFile;
                try {
                    runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                            HybridHashStaticRangePartitionGroupHashTable.class.getSimpleName());
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                runWriters[partitionToPick] = new RunFileWriter(runFile, ctx.getIOManager());
            }
        }
        runWriters[partitionToPick].open();

        // flush the partition picked
        spillPartition(partitionToPick);

        // mark the picked partition as spilled
        partitionSpillFlags.set(partitionToPick);
    }
}
