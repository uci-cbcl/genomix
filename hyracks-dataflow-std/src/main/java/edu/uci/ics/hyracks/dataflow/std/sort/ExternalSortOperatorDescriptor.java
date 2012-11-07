/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.ActivityId;
import edu.uci.ics.hyracks.api.dataflow.IActivityGraphBuilder;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractActivityNode;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractTaskState;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class ExternalSortOperatorDescriptor extends AbstractOperatorDescriptor {
    private static final long serialVersionUID = 1L;

    private static final int SORT_ACTIVITY_ID = 0;
    private static final int MERGE_ACTIVITY_ID = 1;

    private final int[] sortFields;
    private final INormalizedKeyComputerFactory firstKeyNormalizerFactory;
    private final IBinaryComparatorFactory[] comparatorFactories;
    private final int framesLimit;

    private final boolean isLoadBuffered;

    public ExternalSortOperatorDescriptor(JobSpecification spec, int framesLimit, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields, null, comparatorFactories, recordDescriptor, false);
    }

    public ExternalSortOperatorDescriptor(JobSpecification spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) {
        this(spec, framesLimit, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor, false);
    }

    public ExternalSortOperatorDescriptor(JobSpecification spec, int framesLimit, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, boolean isLoadBuffered) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.sortFields = sortFields;
        this.firstKeyNormalizerFactory = firstKeyNormalizerFactory;
        this.comparatorFactories = comparatorFactories;
        if (framesLimit <= 1) {
            throw new IllegalStateException();// minimum of 2 fames (1 in,1 out)
        }
        recordDescriptors[0] = recordDescriptor;
        this.isLoadBuffered = isLoadBuffered;
    }

    public ExternalSortOperatorDescriptor(JobSpecification spec, int framesLimit, int[] sortFields,
            IBinaryComparatorFactory[] comparatorFactories, RecordDescriptor recordDescriptor, boolean isLoadBuffered) {
        this(spec, framesLimit, sortFields, null, comparatorFactories, recordDescriptor, isLoadBuffered);
    }

    @Override
    public void contributeActivities(IActivityGraphBuilder builder) {
        SortActivity sa = new SortActivity(new ActivityId(odId, SORT_ACTIVITY_ID));
        MergeActivity ma = new MergeActivity(new ActivityId(odId, MERGE_ACTIVITY_ID));

        builder.addActivity(sa);
        builder.addSourceEdge(0, sa, 0);

        builder.addActivity(ma);
        builder.addTargetEdge(0, ma, 0);

        builder.addBlockingEdge(sa, ma);
    }

    public static class SortTaskState extends AbstractTaskState {
        private List<IFrameReader> runs;
        private FrameSorter frameSorter;

        public SortTaskState() {
        }

        private SortTaskState(JobId jobId, TaskId taskId) {
            super(jobId, taskId);
        }

        @Override
        public void toBytes(DataOutput out) throws IOException {

        }

        @Override
        public void fromBytes(DataInput in) throws IOException {

        }
    }

    private class SortActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public SortActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryInputSinkOperatorNodePushable() {
                private ExternalSortRunGenerator runGen;

                // FIXME
                private long timer;

                @Override
                public void open() throws HyracksDataException {
                    // FIXME
                    timer = System.nanoTime();

                    runGen = new ExternalSortRunGenerator(ctx, sortFields, firstKeyNormalizerFactory,
                            comparatorFactories, recordDescriptors[0], framesLimit);
                    runGen.open();

                    ctx.getCounterContext().getCounter("must.algo", true).set(1);

                    ctx.getCounterContext().getCounter("must.mem", true).set(framesLimit);
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    runGen.nextFrame(buffer);
                }

                @Override
                public void close() throws HyracksDataException {
                    timer = System.nanoTime() - timer;

                    SortTaskState state = new SortTaskState(ctx.getJobletContext().getJobId(), new TaskId(
                            getActivityId(), partition));
                    runGen.close();
                    state.runs = runGen.getRuns();
                    state.frameSorter = runGen.getFrameSorter();
                    ctx.setTaskState(state);

                    ctx.getCounterContext().getCounter("optional.sort.time", true).set(timer);

                    ctx.getCounterContext().getCounter("optional.sort.runs.count", true).set(state.runs.size());

                    ctx.getCounterContext().getCounter("optional.sort.io", true)
                            .set(Long.valueOf(ctx.getIOManager().toString()));
                }

                @Override
                public void fail() throws HyracksDataException {
                    runGen.fail();
                }
            };
            return op;
        }
    }

    private class MergeActivity extends AbstractActivityNode {
        private static final long serialVersionUID = 1L;

        public MergeActivity(ActivityId id) {
            super(id);
        }

        @Override
        public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
                IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions) {
            IOperatorNodePushable op = new AbstractUnaryOutputSourceOperatorNodePushable() {
                @Override
                public void initialize() throws HyracksDataException {
                    // FIXME
                    long timer = System.nanoTime();

                    ctx.getCounterContext().getCounter("must.merge.comps", true).set(0);
                    ctx.getCounterContext().getCounter("must.merge.swaps", true).set(0);

                    SortTaskState state = (SortTaskState) ctx.getTaskState(new TaskId(new ActivityId(getOperatorId(),
                            SORT_ACTIVITY_ID), partition));
                    List<IFrameReader> runs = state.runs;

                    FrameSorter frameSorter = state.frameSorter;
                    IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
                    for (int i = 0; i < comparatorFactories.length; ++i) {
                        comparators[i] = comparatorFactories[i].createBinaryComparator();
                    }
                    int necessaryFrames = Math.min(runs.size() + 2, framesLimit);
                    ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, frameSorter, runs, sortFields,
                            comparators, recordDescriptors[0], isLoadBuffered ? framesLimit : necessaryFrames, writer,
                            isLoadBuffered);
                    merger.process();

                    // FIXME
                    timer = System.nanoTime() - timer;

                    ctx.getCounterContext().getCounter("optional.merge.time", true).set(timer);

                    ctx.getCounterContext().getCounter("must.io", true)
                            .set(Long.valueOf(ctx.getIOManager().toString()));
                }
            };
            return op;
        }
    }
}