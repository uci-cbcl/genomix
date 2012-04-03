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

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class ImprovedHybridHashGroupFactory implements IImprovedHybridHashGroupFactory {

    private static final long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.dataflow.std.group.IImprovedHybridHashGroupFactory#createHybridHashGrouper(edu.uci.ics.hyracks.api.context.IHyracksTaskContext, int, int[], int[], int, int, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor, edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator[], edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer, edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer, edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory, edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory, edu.uci.ics.hyracks.api.comm.IFrameWriter)
     */
    @Override
    public AbstractImprovedHybridHashGrouper createHybridHashGrouper(IHyracksTaskContext ctx, int framesLimit,
            int[] keys, int[] storedKeys, int targetNumOfPartitions, int hashtableSize, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc, IBinaryComparator[] comparators,
            ITuplePartitionComputer aggregateHashtablePartitionComputer,
            ITuplePartitionComputer mergeHashtablePartitionComputer, IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory, IFrameWriter finalOutputWriter) throws HyracksDataException {
        return new AbstractImprovedHybridHashGrouper(ctx, framesLimit, keys, storedKeys, targetNumOfPartitions,
                hashtableSize, inRecDesc, outRecDesc, comparators, aggregateHashtablePartitionComputer,
                mergeHashtablePartitionComputer, aggregatorFactory, mergerFactory, finalOutputWriter) {

            @Override
            public int getReservedFramesCount() {
                // use one frame for output buffer
                return 1;
            }

            /**
             * Pick the resident partition with largest size (in frame).
             */
            @Override
            public int selectPartitionToSpill() {
                int pid = -1;
                for (int i = 0; i < numOfPartitions; i++) {
                    if (!partitionStatus.get(i)) {
                        // only pick resident partition
                        if (pid < 0) {
                            pid = i;
                        } else {
                            if (partitionSizeInFrame[i] > partitionSizeInFrame[pid])
                                pid = i;
                        }
                    }
                }
                return pid;
            }
        };
    }

}
