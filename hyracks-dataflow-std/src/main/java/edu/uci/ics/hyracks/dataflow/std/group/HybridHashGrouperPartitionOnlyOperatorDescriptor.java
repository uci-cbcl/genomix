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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;

public class HybridHashGrouperPartitionOnlyOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int framesLimit;
    private final int tableSize;

    private final int[] keyFields;

    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;

    private final IAggregatorDescriptorFactory aggFactory;

    private final RecordDescriptor outRecDesc;

    public HybridHashGrouperPartitionOnlyOperatorDescriptor(JobSpecification spec, int framesLimit, int tableSize,
            int[] keyFields, IBinaryHashFunctionFamily[] hashFunctionFamilies, IAggregatorDescriptorFactory aggFactory,
            RecordDescriptor outRecDesc) {
        super(spec, 1, 0);

        this.framesLimit = framesLimit;
        this.keyFields = keyFields;
        this.tableSize = tableSize;

        this.hashFunctionFamilies = hashFunctionFamilies;

        this.aggFactory = aggFactory;

        this.outRecDesc = outRecDesc;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * edu.uci.ics.hyracks.api.dataflow.IActivity#createPushRuntime(edu.uci.ics.hyracks.api.context.IHyracksTaskContext,
     * edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int, int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            final IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions)
            throws HyracksDataException {

        return new AbstractUnaryInputSinkOperatorNodePushable() {

            ByteBuffer[] partitionOutputBuffers;

            FrameTupleAccessor tupleAccessor;

            ITuplePartitionComputer tpc;

            FrameTupleAppender appender;

            RunFileWriter[] partitionWriters;

            IAggregatorDescriptor aggregator;

            AggregateState aggState;

            ArrayTupleBuilder tupleBuilder;

            @Override
            public void open() throws HyracksDataException {
                partitionOutputBuffers = new ByteBuffer[framesLimit];
                for (int i = 0; i < partitionOutputBuffers.length; i++) {
                    partitionOutputBuffers[i] = ctx.allocateFrame();
                }
                tupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescProvider.getInputRecordDescriptor(
                        getOperatorId(), 0));
                ITuplePartitionComputerFamily tpcf = new FieldHashPartitionComputerFamily(keyFields,
                        hashFunctionFamilies);
                tpc = tpcf.createPartitioner(0);
                appender = new FrameTupleAppender(ctx.getFrameSize());
                partitionWriters = new RunFileWriter[framesLimit];
                this.aggregator = aggFactory.createAggregator(ctx,
                        recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0), outRecDesc, keyFields,
                        keyFields);
                this.aggState = aggregator.createAggregateStates();
                this.tupleBuilder = new ArrayTupleBuilder(outRecDesc.getFieldCount());
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                tupleAccessor.reset(buffer);
                int tupleCount = tupleAccessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    int hid = tpc.partition(tupleAccessor, i, tableSize);
                    int pid = hid % partitionOutputBuffers.length;
                    insertPartiton(tupleAccessor, i, pid);
                }
            }

            private void insertPartiton(FrameTupleAccessor accessor, int tIndex, int pid) throws HyracksDataException {
                appender.reset(partitionOutputBuffers[pid], false);

                tupleBuilder.reset();
                for (int k = 0; k < keyFields.length; k++) {
                    tupleBuilder.addField(accessor, tIndex, keyFields[k]);
                }

                aggregator.init(tupleBuilder, accessor, tIndex, aggState);

                if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                        tupleBuilder.getSize())) {
                    if (partitionWriters[pid] == null) {
                        FileReference runFile;
                        try {
                            runFile = ctx.getJobletContext().createManagedWorkspaceFile(
                                    HybridHashGrouperPartitionOnlyOperatorDescriptor.class.getSimpleName());
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                        partitionWriters[pid] = new RunFileWriter(runFile, ctx.getIOManager());
                        partitionWriters[pid].open();
                    }
                    FrameUtils.flushFrame(partitionOutputBuffers[pid], partitionWriters[pid]);
                    appender.reset(partitionOutputBuffers[pid], true);
                    if (!appender.append(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray(), 0,
                            tupleBuilder.getSize())) {
                        throw new HyracksDataException("Failed to insert a record into partition " + pid);
                    }
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                for (int i = 0; i < partitionOutputBuffers.length; i++) {
                    appender.reset(partitionOutputBuffers[i], false);
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(partitionOutputBuffers[i], partitionWriters[i]);
                    }
                    partitionWriters[i].close();
                }
            }
        };
    }
}
