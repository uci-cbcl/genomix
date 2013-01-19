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
package edu.uci.ics.hyracks.dataflow.std.group.hashsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractStateObject;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class HybridHashSortGroupOperatorDescriptor extends AbstractOperatorDescriptor {

    private static final int AGGREGATE_ACTIVITY_ID = 0;

    private static final int MERGE_ACTIVITY_ID = 1;

    private static final long serialVersionUID = 1L;
    private final int[] keyFields, storedKeyFields;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final IAggregatorDescriptorFactory mergerFactory;

    private final ITuplePartitionComputerFactory aggTpcf, mergeTpcf;

    private final int framesLimit;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final int tableSize;

    private final boolean isLoadOptimized;

    public HybridHashSortGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFactory aggTpcf,
            ITuplePartitionComputerFactory mergeTpcf, IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory, RecordDescriptor recordDescriptor) {
        this(spec, keyFields, framesLimit, tableSize, comparatorFactories, aggTpcf, mergeTpcf, null, aggregatorFactory,
                mergerFactory, recordDescriptor, false);
    }

    public HybridHashSortGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFactory aggTpcf,
            ITuplePartitionComputerFactory mergeTpcf, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor) {
        this(spec, keyFields, framesLimit, tableSize, comparatorFactories, aggTpcf, mergeTpcf, firstNormalizerFactory,
                aggregatorFactory, mergerFactory, recordDescriptor, false);
    }

    public HybridHashSortGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFactory aggTpcf,
            ITuplePartitionComputerFactory mergeTpcf, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor, boolean isLoadOpt) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        if (framesLimit <= 2) {
            /**
             * Minimum of 3 frames: 2 for in-memory hash table, and 1 for output
             * aggregation results.
             */
            throw new IllegalStateException("frame limit should at least be 3, but it is " + framesLimit + "!");
        }

        storedKeyFields = new int[keyFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.aggTpcf = aggTpcf;
        this.mergeTpcf = mergeTpcf;
        this.tableSize = tableSize;

        /**
         * Set the record descriptor. Note that since this operator is a unary
         * operator, only the first record descriptor is used here.
         */
        recordDescriptors[0] = recordDescriptor;

        this.isLoadOptimized = isLoadOpt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor#contributeActivities(edu.uci.ics.hyracks.api.dataflow.
     * IActivityGraphBuilder)
     */
    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        AggregateActivity aggregateAct = new AggregateActivity(new ActivityId(getOperatorId(), AGGREGATE_ACTIVITY_ID));
        MergeActivity mergeAct = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(this, aggregateAct);
        builder.addSourceEdge(0, aggregateAct, 0);

        builder.addActivity(this, mergeAct);
        builder.addTargetEdge(0, mergeAct, 0);

        builder.addBlockingEdge(aggregateAct, mergeAct);
    }

    public static class AggregateActivityState extends AbstractStateObject {

        private HybridHashSortGroupHashTable gTable;

        public AggregateActivityState() {
        }

        private AggregateActivityState(JobId jobId, TaskId tId) {
            super(jobId, tId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void fromBytes(DataInput in) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    private class AggregateActivity extends AbstractActivityNode {

        private static final long serialVersionUID = 1L;

        public AggregateActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                final IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
                throws HyracksDataException {
            return new AbstractUnaryInputSinkOperatorNodePushable() {

                HybridHashSortGroupHashTable serializableGroupHashtable;

                FrameTupleAccessor accessor;

                @Override
                public void open() throws HyracksDataException {

                    RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparatorFactories.length; i++) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }

                    serializableGroupHashtable = new HybridHashSortGroupHashTable(ctx, framesLimit, tableSize,
                            keyFields, comparators, aggTpcf.createPartitioner(),
                            firstNormalizerFactory.createNormalizedKeyComputer(), aggregatorFactory.createAggregator(
                                    ctx, inRecDesc, recordDescriptors[0], keyFields, storedKeyFields), inRecDesc,
                            recordDescriptors[0]);
                    accessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    accessor.reset(buffer);
                    int tupleCount = accessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        serializableGroupHashtable.insert(accessor, i);
                    }
                }

                @Override
                public void fail() throws HyracksDataException {
                }

                @Override
                public void close() throws HyracksDataException {
                    serializableGroupHashtable.finishup();
                    AggregateActivityState state = new AggregateActivityState(ctx.getJobletContext().getJobId(),
                            new TaskId(getActivityId(), partition));
                    state.gTable = serializableGroupHashtable;
                    ctx.setStateObject(state);
                }
            };
        }
    }

    private class MergeActivity extends AbstractActivityNode {

        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, final int nPartitions)
                throws HyracksDataException {

            return new AbstractUnaryOutputSourceOperatorNodePushable() {

                public void initialize() throws HyracksDataException {

                    AggregateActivityState aggState = (AggregateActivityState) ctx.getStateObject(new TaskId(
                            new ActivityId(getOperatorId(), AGGREGATE_ACTIVITY_ID), partition));

                    LinkedList<RunFileReader> runs = aggState.gTable.getRunFileReaders();

                    writer.open();
                    if (runs.size() <= 0) {
                        aggState.gTable.flushHashtableToOutput(writer);
                        aggState.gTable.close();
                    } else {
                        aggState.gTable.close();

                        IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                        for (int i = 0; i < comparatorFactories.length; i++) {
                            comparators[i] = comparatorFactories[i].createBinaryComparator();
                        }

                        HybridHashSortRunMerger merger = new HybridHashSortRunMerger(ctx, runs, storedKeyFields,
                                comparators, recordDescriptors[0], mergeTpcf.createPartitioner(),
                                mergerFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                                        storedKeyFields, storedKeyFields), framesLimit, tableSize, writer,
                                isLoadOptimized);

                        merger.process();
                    }

                    writer.close();
                }

            };
        }
    }

}
