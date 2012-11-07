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
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class HybridHashStrictStaticRangePartitionGroupHashTable extends HybridHashDynamicDestagingGroupHashTable {

    public HybridHashStrictStaticRangePartitionGroupHashTable(IHyracksTaskContext ctx, int frameLimits, int tableSize,
            int numOfPartitions, int procLevel, int[] keys, IBinaryComparator[] comparators,
            ITuplePartitionComputerFamily tpcf, IAggregatorDescriptor aggregator, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IFrameWriter outputWriter) throws HyracksDataException {
        super(ctx, frameLimits, tableSize, numOfPartitions, procLevel, keys, comparators, tpcf, aggregator, inRecDesc,
                outRecDesc, outputWriter, true);
    }

    /**
     * Compute the partition id for the given hash key.
     * 
     * @param hashKey
     * @return
     */
    @Override
    public void insert(FrameTupleAccessor accessor, int tupleIndex) throws HyracksDataException {
        // FIXME
        long timer = System.nanoTime();
        insertedRawRecords++;

        int entry = tpc.partition(accessor, tupleIndex, tableSize);
        int pid = partition(entry);

        partitionRawRecordCounts[pid]++;

        if (partitionSpillFlags.get(pid)) {
            // for spilled partition: direct insertion
            insertSpilledPartition(pid, accessor, tupleIndex);
            totalTupleCount++;
            runSizeInTuples[pid]++;
            partitionRecordsInMemory[pid]++;
        } else {
            hashedRawRecords++;
            boolean foundMatch = findMatch(entry, accessor, tupleIndex);
            if (foundMatch) {
                // find match; do aggregation
                hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                aggregator.aggregate(accessor, tupleIndex, hashtableRecordAccessor, matchPointer.tupleIndex,
                        partitionAggregateStates[pid]);
            } else {

                if (partitionAggregateStates[pid] == null) {
                    partitionAggregateStates[pid] = aggregator.createAggregateStates();
                }

                internalTupleBuilder.reset();
                for (int k = 0; k < keys.length; k++) {
                    internalTupleBuilder.addField(accessor, tupleIndex, keys[k]);
                }
                aggregator.init(internalTupleBuilder, accessor, tupleIndex, partitionAggregateStates[pid]);

                internalAppender.reset(contents[partitionCurrentFrameIndex[pid]], false);
                if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                        internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {

                    // if the same partition is flushed, the aggregation value should be
                    // re-initialized since the aggregate state is reset.
                    boolean isSelfSpilled = false;

                    // try to allocate a new frame
                    while (true) {
                        int newFrame = allocateFrame();
                        if (newFrame != END_REF) {
                            frameNext[newFrame] = partitionCurrentFrameIndex[pid];
                            partitionCurrentFrameIndex[pid] = newFrame;
                            partitionSizeInFrame[pid]++;
                            break;
                        } else {
                            if (partitionSpillFlags.get(1)) {
                                // if other partitions are spilled, spill the only partition 0
                                chooseResidentPartitionToFlush();
                            } else {
                                // spill all partitions but one
                                spillAllSpillingPartitions();
                            }
                            if (partitionSpillFlags.get(pid)) {
                                // in case that the current partition is spilled
                                isSelfSpilled = true;
                                break;
                            }
                        }
                    }

                    if (isSelfSpilled) {
                        // re-insert
                        insertSpilledPartition(pid, accessor, tupleIndex);
                    } else {
                        internalAppender.reset(contents[partitionCurrentFrameIndex[pid]], true);
                        if (!internalAppender.append(internalTupleBuilder.getFieldEndOffsets(),
                                internalTupleBuilder.getByteArray(), 0, internalTupleBuilder.getSize())) {
                            throw new HyracksDataException("Failed to insert an aggregation value to the hash table.");
                        }
                    }
                }

                // update hash table reference, only if the insertion to a resident partition is successful
                if (!partitionSpillFlags.get(pid)) {

                    hashedKeys++;

                    // update hash reference
                    if (matchPointer.frameIndex < 0) {
                        // first record for this entry; update the header references
                        int headerFrameIndex = getHeaderFrameIndex(entry);
                        int headerFrameOffset = getHeaderTupleIndex(entry);
                        if (headers[headerFrameIndex] == null) {
                            headers[headerFrameIndex] = ctx.allocateFrame();
                            resetHeader(headerFrameIndex);
                        }
                        headers[headerFrameIndex].putInt(headerFrameOffset, partitionCurrentFrameIndex[pid]);
                        headers[headerFrameIndex].putInt(headerFrameOffset + INT_SIZE,
                                internalAppender.getTupleCount() - 1);

                        partitionUsedHashEntries[pid]++;

                    } else {
                        // update the previous reference
                        hashtableRecordAccessor.reset(contents[matchPointer.frameIndex]);
                        int refOffset = hashtableRecordAccessor.getTupleHashReferenceOffset(matchPointer.tupleIndex);
                        contents[matchPointer.frameIndex].putInt(refOffset, partitionCurrentFrameIndex[pid]);
                        contents[matchPointer.frameIndex].putInt(refOffset + INT_SIZE,
                                internalAppender.getTupleCount() - 1);
                    }
                }

                totalTupleCount++;
                runSizeInTuples[pid]++;
                partitionRecordsInMemory[pid]++;
            }
        }
        partitionRecordsInserted[pid]++;

        // FIXME
        insertTimer += System.nanoTime() - timer;
    }

    /**
     * Spill all partitions but the first partition.
     * 
     * @throws HyracksDataException
     */
    private void spillAllSpillingPartitions() throws HyracksDataException {
        for (int i = 1; i < numOfPartitions; i++) {
            // spill the picked partition
            int frameToFlush = partitionCurrentFrameIndex[i];
            if (frameToFlush != END_REF) {
                if (runWriters[i] == null) {
                    FileReference runFile;
                    try {
                        runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                HybridHashDynamicDestagingGroupHashTable.class.getSimpleName());
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                    runWriters[i] = new RunFileWriter(runFile, ctx.getIOManager());
                    runSizeInFrames[i] = 0;
                }
            }
            runWriters[i].open();
            spillPartition(i);
            partitionSpillFlags.set(i);
        }
    }

}
