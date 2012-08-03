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

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputer;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.HybridHashSortELGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.HybridHashSortGroupHashTableWithSortThreshold;

public class HybridHashSortELGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private static final int INT_SIZE = 4;

    private final int[] keyFields, storedKeyFields;
    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final IAggregatorDescriptorFactory mergerFactory;

    private final int framesLimit;

    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final int tableSize;

    private final int sortThreshold;

    private final boolean allowMultiEntriesMerging;

    private final double partitionFactor;

    private static final Logger LOGGER = Logger.getLogger(HybridHashSortELGrouperBucketMerge.class.getSimpleName());

    public HybridHashSortELGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, int sortThreshold, double partitionFactor, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor) {
        this(spec, keyFields, framesLimit, tableSize, sortThreshold, partitionFactor, comparatorFactories,
                hashFunctionFamilies, firstNormalizerFactory, aggregatorFactory, mergerFactory, recordDescriptor, false);
    }

    public HybridHashSortELGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, int sortThreshold, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor) {
        this(spec, keyFields, framesLimit, tableSize, sortThreshold, 1.0, comparatorFactories, hashFunctionFamilies,
                firstNormalizerFactory, aggregatorFactory, mergerFactory, recordDescriptor, false);
    }

    public HybridHashSortELGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int tableSize, int sortThreshold, double partitionFactor, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor, boolean allowMultiEntriesMerging) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;

        storedKeyFields = new int[keyFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        this.aggregatorFactory = aggregatorFactory;
        this.mergerFactory = mergerFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;

        this.hashFunctionFamilies = hashFunctionFamilies;

        this.tableSize = tableSize;
        this.sortThreshold = sortThreshold;

        this.allowMultiEntriesMerging = allowMultiEntriesMerging;

        this.partitionFactor = partitionFactor;

        /**
         * Set the record descriptor. Note that since this operator is a unary
         * operator, only the first record descriptor is used here.
         */
        recordDescriptors[0] = recordDescriptor;
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
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        int hashtableMinimumFrameRequirement = HybridHashSortELGroupHashTable.getMinimumFramesLimit(tableSize,
                ctx.getFrameSize());

        if (framesLimit < hashtableMinimumFrameRequirement) {
            /**
             * Minimum frames: 1 for input records, and 1 for output
             * aggregation results.
             */
            throw new HyracksDataException("frame limit should at least be " + hashtableMinimumFrameRequirement
                    + ", but it is " + framesLimit + "!");
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);

        final RecordDescriptor outRecDesc = recordDescriptors[0];

        final ITuplePartitionComputerFamily tpcf = new FieldHashPartitionComputerFamily(storedKeyFields,
                hashFunctionFamilies);

        final ITuplePartitionComputer aggTpc = tpcf.createPartitioner(0);

        final INormalizedKeyComputer firstNormalizerComputer = firstNormalizerFactory.createNormalizedKeyComputer();

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final IAggregatorDescriptor aggregator = aggregatorFactory.createAggregator(ctx, inRecDesc, outRecDesc,
                keyFields, storedKeyFields);

        final IAggregatorDescriptor merger = mergerFactory.createAggregator(ctx, outRecDesc, outRecDesc,
                storedKeyFields, storedKeyFields);

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            HybridHashSortGroupHashTableWithSortThreshold hashtable;

            FrameTupleAccessor accessor;

            // FIXME
            long timer1, timer2;

            @Override
            public void open() throws HyracksDataException {

                // FIXME
                timer1 = System.currentTimeMillis();
                LOGGER.warning("HybridHashSortEL-Phase1-Open\t" + ctx.getIOManager().toString());

                writer.open();
                //                hashtable = new HybridHashSortELGroupHashTable(ctx, framesLimit, tableSize, keyFields, sortThreshold,
                //                        partitionFactor, comparators, aggTpc, firstNormalizerComputer, aggregator, merger, inRecDesc,
                //                        outRecDesc, writer);

                hashtable = new HybridHashSortGroupHashTableWithSortThreshold(ctx, framesLimit, tableSize, keyFields,
                        comparators, aggTpc, firstNormalizerComputer, aggregator, inRecDesc, outRecDesc, sortThreshold);

                accessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                accessor.reset(buffer);
                int tupleCount = accessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    hashtable.insert(accessor, i);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                hashtable.finishup();

                int maxRecLength = hashtable.getMaxRecordLength();
                if (maxRecLength <= 0) {
                    maxRecLength = outRecDesc.getFieldCount() * INT_SIZE * 2 + INT_SIZE;
                }

                LinkedList<RunFileReader> runs = hashtable.getRunFileReaders();
                // FIXME
                timer1 = System.currentTimeMillis() - timer1;
                LOGGER.warning("HybridHashSortEL-Phase1-Close\t" + timer1 + "\t" + ctx.getIOManager().toString() + "\t"
                        + runs.size());
                timer2 = System.currentTimeMillis();

                int minHashTableSize = maxRecLength * (sortThreshold + 1) / ctx.getFrameSize();

                if (minHashTableSize > framesLimit - 1 - runs.size()) {
                    LOGGER.warning("Not enough memory for HHS-EL algorithm: there are " + runs.size()
                            + " run files but only " + framesLimit
                            + " frames. The algorithm continues to make its best effort to allocate memory.");
                }
                if (runs == null || runs.size() <= 0) {
                    hashtable.flushHashtableToOutput(writer);
                    hashtable.close();
                } else {
                    hashtable.close();

                    //                    HybridHashSortELGrouperBucketMerge mergeProcessor = new HybridHashSortELGrouperBucketMerge(ctx,
                    //                            storedKeyFields, framesLimit, tableSize, sortThreshold, maxRecLength, aggTpc, tpcf,
                    //                            comparators, firstNormalizerComputer, merger, outRecDesc, outRecDesc, writer);
                    //                    mergeProcessor.initialize(runs);

                    IFrameReader[] runCursors = new RunFileReader[runs.size()];
                    for (int i = 0; i < runs.size(); i++) {
                        runCursors[i] = runs.get(i);
                    }

                    HybridHashSortELRunMerger mergeProcessor = new HybridHashSortELRunMerger(ctx, runCursors,
                            framesLimit, tableSize, sortThreshold, maxRecLength, storedKeyFields, aggTpc,
                            tpcf.createPartitioner(1), comparators, firstNormalizerComputer, merger, outRecDesc,
                            writer, false, allowMultiEntriesMerging);
                    mergeProcessor.process();
                }
                writer.close();

                // FIXME
                timer2 = System.currentTimeMillis() - timer2;
                LOGGER.warning("HybridHashSortEL-Phase2-Close\t" + timer2 + "\t" + ctx.getIOManager().toString());
            }
        };
    }
}
