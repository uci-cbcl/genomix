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
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;

public class ImprovedHybridHashGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private final int framesLimit;
    private final int inputSize;
    private final double factor;
    private final int[] keys;
    private final int hashtableSize;

    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final IImprovedHybridHashGroupFactory hybridHashGrouperFactory;

    private final IAggregatorDescriptorFactory aggregatorFactory, mergerFactory;

    private final static Logger LOGGER = Logger.getLogger(ImprovedHybridHashGroupOperatorDescriptor.class
            .getSimpleName());

    private static final double HYBRID_SWITCH_THRESHOLD = 0.8;

    public ImprovedHybridHashGroupOperatorDescriptor(JobSpecification spec, int framesLimit, int inputSize,
            double factor, int hashtableSize, int[] keys, IBinaryHashFunctionFamily[] hashFunctionFamilies,
            IBinaryComparatorFactory[] comparatorFactories, IImprovedHybridHashGroupFactory hybridHashGrouperFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor outRecordDescriptor) {
        super(spec, 1, 1);
        this.inputSize = inputSize;
        this.framesLimit = framesLimit;
        this.factor = factor;
        this.keys = keys;
        this.hashtableSize = hashtableSize;

        this.hashFunctionFamilies = hashFunctionFamilies;
        this.comparatorFactories = comparatorFactories;

        this.hybridHashGrouperFactory = hybridHashGrouperFactory;

        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;

        recordDescriptors[0] = outRecordDescriptor;
    }

    private static final long serialVersionUID = 1L;

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.IActivity#createPushRuntime(edu.uci.ics.hyracks.api.context.IHyracksTaskContext, edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int, int)
     */
    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, final int nPartitions)
            throws HyracksDataException {

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);

        final ITuplePartitionComputer aggregateTpc = new FieldHashPartitionComputerFamily(keys, hashFunctionFamilies)
                .createPartitioner(0);

        // Keys for stored records
        final int[] storedKeys = new int[keys.length];
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer[] storedKeySerDeser = new ISerializerDeserializer[keys.length];
        for (int i = 0; i < keys.length; i++) {
            storedKeys[i] = i;
            storedKeySerDeser[i] = inRecDesc.getFields()[keys[i]];
        }

        final ITuplePartitionComputer mergeTpc = new FieldHashPartitionComputerFamily(storedKeys, hashFunctionFamilies)
                .createPartitioner(0);

        final RecordDescriptor internalRecordDescriptor;

        if (keys.length >= recordDescriptors[0].getFields().length) {
            // for the case of zero-aggregations
            ISerializerDeserializer<?>[] fields = recordDescriptors[0].getFields();
            ITypeTraits[] types = recordDescriptors[0].getTypeTraits();
            ISerializerDeserializer<?>[] newFields = new ISerializerDeserializer[fields.length + 1];
            for (int i = 0; i < fields.length; i++)
                newFields[i] = fields[i];
            ITypeTraits[] newTypes = null;
            if (types != null) {
                newTypes = new ITypeTraits[types.length + 1];
                for (int i = 0; i < types.length; i++)
                    newTypes[i] = types[i];
            }
            internalRecordDescriptor = new RecordDescriptor(newFields, newTypes);
        } else {
            internalRecordDescriptor = recordDescriptors[0];
        }

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private AbstractImprovedHybridHashGrouper hybridHashGrouper;

            private int numOfPartitions;

            private ByteBuffer runLoadFrame;

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                hybridHashGrouper = hybridHashGrouperFactory.createHybridHashGrouper(ctx, framesLimit, keys,
                        storedKeys, getNumberOfPartitions(framesLimit, inputSize, factor), hashtableSize, inRecDesc,
                        recordDescriptors[0], comparators, aggregateTpc, mergeTpc, aggregatorFactory, mergerFactory,
                        writer);
                numOfPartitions = hybridHashGrouper.getNumOfPartitions();
            }

            private int getNumberOfPartitions(int framesLimit, int inputSize, double factor) {
                int numberOfPartitions = (int) (Math.ceil((double) (inputSize * factor / nPartitions - framesLimit)
                        / (double) (framesLimit - 1)));
                if (numberOfPartitions <= 0) {
                    numberOfPartitions = 1;
                }
                return numberOfPartitions;
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                hybridHashGrouper.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                hybridHashGrouper.close();
                // process run files
                for (int i = hybridHashGrouper.partitionStatus.nextSetBit(0); i >= 0; i = hybridHashGrouper.partitionStatus
                        .nextSetBit(i + 1)) {
                    if (hybridHashGrouper.getSpilledRunWriter(i) == null) {
                        continue;
                    }
                    RunFileReader runReader = hybridHashGrouper.getSpilledRunWriter(i).createReader();
                    int runFileSizeInFrame = hybridHashGrouper.getPartitionSizeInFrame(i);
                    processRunFile(runReader, runFileSizeInFrame, hashtableSize / numOfPartitions, 0);
                }
                writer.close();
            }

            private void processRunFile(RunFileReader runReader, int runFileSize, int htSize, int level)
                    throws HyracksDataException {
                LOGGER.warning("processRunFile " + runFileSize + " " + htSize);
                int partitionCount = getNumberOfPartitions(framesLimit, runFileSize, factor);
                ITuplePartitionComputer partitionComputer = new FieldHashPartitionComputerFamily(storedKeys,
                        hashFunctionFamilies).createPartitioner(level + 1);
                AbstractImprovedHybridHashGrouper grouper = hybridHashGrouperFactory.createHybridHashGrouper(ctx,
                        framesLimit, storedKeys, storedKeys, partitionCount, hashtableSize, internalRecordDescriptor,
                        recordDescriptors[0], comparators, partitionComputer, partitionComputer, mergerFactory,
                        mergerFactory, writer);

                runReader.open();

                if (runLoadFrame == null) {
                    runLoadFrame = ctx.allocateFrame();
                }

                while (runReader.nextFrame(runLoadFrame)) {
                    grouper.nextFrame(runLoadFrame);
                }

                runReader.close();
                grouper.close();

                if (grouper.hasSpilledPartition()) {
                    if (grouper.getPartitionMaxFrameSize() < HYBRID_SWITCH_THRESHOLD * runFileSize) {
                        // recursive hybrid-hash-group
                        for (int i = grouper.partitionStatus.nextSetBit(0); i >= 0; i = grouper.partitionStatus
                                .nextSetBit(i + 1)) {
                            RunFileReader runFileReader = grouper.getSpilledRunWriter(i).createReader();
                            int runFileSizeInFrame = grouper.getPartitionSizeInFrame(i);
                            processRunFile(runFileReader, runFileSizeInFrame, htSize / partitionCount, level + 1);
                        }
                    } else {
                        // switch to external-hash-group
                        int tableSize = 0;

                        for (int i = grouper.partitionStatus.nextSetBit(0); i >= 0; i = grouper.partitionStatus
                                .nextSetBit(i + 1)) {
                            tableSize += grouper.partitionSizeInTuple[i] * 2;
                        }

                        // use external-hash-group
                        ExternalHashGrouperAggregate aggregator = new ExternalHashGrouperAggregate(ctx, storedKeys,
                                framesLimit, tableSize, comparatorFactories, null, mergerFactory,
                                internalRecordDescriptor, recordDescriptors[0], new HashSpillableFrameSortTableFactory(
                                        new ITuplePartitionComputerFactory() {

                                            private static final long serialVersionUID = 1L;

                                            @Override
                                            public ITuplePartitionComputer createPartitioner() {
                                                return new FieldHashPartitionComputerFamily(storedKeys,
                                                        hashFunctionFamilies).createPartitioner(0);
                                            }
                                        }), writer, false);
                        aggregator.initGrouperForAggregate();

                        for (int i = grouper.partitionStatus.nextSetBit(0); i >= 0; i = grouper.partitionStatus
                                .nextSetBit(i + 1)) {
                            RunFileReader rReader = grouper.getSpilledRunWriter(i).createReader();
                            rReader.open();
                            while (rReader.nextFrame(runLoadFrame)) {
                                aggregator.insertFrame(runLoadFrame);
                            }
                            rReader.close();
                        }
                        aggregator.finishAggregation();
                        LinkedList<RunFileReader> runFiles = aggregator.getRunReaders();
                        if (!runFiles.isEmpty()) {
                            ExternalHashGrouperMerge merger = new ExternalHashGrouperMerge(ctx, storedKeys,
                                    framesLimit, comparatorFactories, mergerFactory, internalRecordDescriptor,
                                    recordDescriptors[0], false, writer);
                            merger.initialize(aggregator.getGroupTable(), runFiles);
                        }
                    }
                }
            }

        };
    }

}
