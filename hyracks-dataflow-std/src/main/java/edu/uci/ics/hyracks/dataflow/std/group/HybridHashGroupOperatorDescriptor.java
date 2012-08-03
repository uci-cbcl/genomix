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

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
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
import edu.uci.ics.hyracks.dataflow.std.group.struct.HybridHashGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.struct.HybridHashSortGroupHashTable;

public class HybridHashGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[] keyFields, storedKeyFields;

    private final IAggregatorDescriptorFactory aggregatorFactory;
    private final IAggregatorDescriptorFactory mergerFactory;

    private final int framesLimit;
    private final IBinaryHashFunctionFamily[] hashFunctionFamilies;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final int tableSize, inputSize;

    private final double fudgeFactor;

    private static final double HYBRID_SWITCH_THRESHOLD = 0.8;

    private final boolean allowReload;

    private static final Logger LOGGER = Logger.getLogger(HybridHashGroupOperatorDescriptor.class.getSimpleName());

    public HybridHashGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit, int inputSize,
            int tableSize, double fudgeFactor, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFunctionFamilies, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor recordDescriptor, boolean allowReload) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.inputSize = inputSize;

        this.fudgeFactor = fudgeFactor;

        if (framesLimit <= 3) {
            /**
             * Minimum of 2 frames: 2 for in-memory hash table, and 1 for output
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
        this.hashFunctionFamilies = hashFunctionFamilies;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.tableSize = tableSize;

        /**
         * Set the record descriptor. Note that since this operator is a unary
         * operator, only the first record descriptor is used here.
         */
        recordDescriptors[0] = recordDescriptor;

        // allow the hybrid hash to reload before moving to the next level
        this.allowReload = allowReload;
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
            IRecordDescriptorProvider recordDescProvider, int partition, final int nPartitions)
            throws HyracksDataException {

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);

        final ITuplePartitionComputerFamily aggTpcf = new FieldHashPartitionComputerFamily(keyFields,
                hashFunctionFamilies);
        final ITuplePartitionComputerFamily mergeTpcf = new FieldHashPartitionComputerFamily(storedKeyFields,
                hashFunctionFamilies);

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            // FIXME
            private long timer1, timer2;

            HybridHashGroupHashTable hashtable;

            /**
             * Compute the initial partition fan-out. The following cases are considered:
             * <p/>
             * If (M - 1) > RF, then we have enough space to do in-memory aggregation. So we just use simple-hash
             * strategy, with P = 1. - If (M - 1) < sqrt(RF), meaning that we do not have enough memory to even
             * partition the data into small partitions. In this case we use the maximum pages to do the partition,
             * without constructing the in-memory hash table. So in this case P = (M - 1);<br/>
             * - Otherwise, the hybrid hash join equation will be used. Here we make small modification to consider the
             * output buffer, so P = (RF - 1) / (M - 2). Note that using this number of partitions may be too large to
             * have enough space for the in-memory hash table. If so, no hash table will be built.
             * 
             * @param tableSize
             * @param framesLimit
             * @param inputSize
             * @param partitionInOperator
             * @param factor
             * @return
             */
            private int getNumberOfPartitions(int tableSize, int framesLimit, int inputSize, int partitionInOperator,
                    double factor) {
                double inputSizeForThisPartition = (double) (inputSize / partitionInOperator);

                int numberOfPartitions = 1;

                int usableFrameSize = framesLimit - 1;

                double sqrtInputSize = Math.sqrt(inputSizeForThisPartition * factor);

                if (inputSizeForThisPartition * factor > usableFrameSize) {
                    if (sqrtInputSize > usableFrameSize) {
                        numberOfPartitions = usableFrameSize;
                    } else {

                        // memory is large enough so that extra memory can be used for in-memory aggregation
                        numberOfPartitions = (int) ((Math.ceil((double) (inputSizeForThisPartition * factor - 1)
                                / (double) (usableFrameSize - 1))));

                        // in case that the input size is just slightly larger than the memory: this will
                        // guarantee that at least one partition can be processed; and the fragmentation 
                        // between partitions can be minimized (although it is larger than a single partition)
                        if (numberOfPartitions <= 1) {
                            numberOfPartitions = 2;
                        }
                    }
                    if (numberOfPartitions > usableFrameSize) {
                        numberOfPartitions = usableFrameSize;
                    }
                } else {
                    // enough space for in-memory aggregation
                    numberOfPartitions = 1;
                }

                return numberOfPartitions;
            }

            IAggregatorDescriptor aggregator = aggregatorFactory.createAggregator(ctx, inRecDesc, recordDescriptors[0],
                    keyFields, storedKeyFields);

            IAggregatorDescriptor merger = mergerFactory.createAggregator(ctx, recordDescriptors[0],
                    recordDescriptors[0], storedKeyFields, storedKeyFields);

            FrameTupleAccessor inBufferFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), inRecDesc);
            FrameTupleAccessor runBufferFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), recordDescriptors[0]);

            ByteBuffer runProcessBuff = null;

            long reloadReadTime = 0;

            @Override
            public void open() throws HyracksDataException {
                // FIXME
                timer1 = System.currentTimeMillis();

                writer.open();
                hashtable = new HybridHashGroupHashTable(ctx, framesLimit, tableSize, getNumberOfPartitions(tableSize,
                        framesLimit, inputSize, nPartitions, fudgeFactor), 0, keyFields, comparators, aggTpcf,
                        aggregator, merger, inRecDesc, recordDescriptors[0], writer, allowReload);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                inBufferFrameAccessor.reset(buffer);
                int tupleCount = inBufferFrameAccessor.getTupleCount();
                for (int i = 0; i < tupleCount; i++) {
                    hashtable.insert(inBufferFrameAccessor, i);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
                LOGGER.severe("Hybrid Hash Grouper failed on the top-hashing phase.");
            }

            @Override
            public void close() throws HyracksDataException {

                hashtable.finishup();

                LinkedList<RunFileReader> runReaders = hashtable.getRunFileReaders();
                LinkedList<Integer> runSizesInFrames = hashtable.getRunFileSizesInFrame();
                LinkedList<Integer> runSizesInTuples = hashtable.getRunFileSizesInTuple();
                hashtable.close();

                hashtable = null;

                // FIXME
                timer1 = System.currentTimeMillis() - timer1;
                LOGGER.warning("TopPartition\t" + timer1 + "\t" + ctx.getIOManager().toString() + "\t"
                        + runReaders.size());
                timer2 = System.currentTimeMillis();

                RunFileReader runReader;
                int runSizeInFrames, runSizeInTuples;
                if (runProcessBuff == null) {
                    runProcessBuff = ctx.allocateFrame();
                }
                while (!runReaders.isEmpty()) {
                    runReader = runReaders.remove();
                    runSizeInFrames = runSizesInFrames.remove();
                    runSizeInTuples = runSizesInTuples.remove();
                    runReader.open();
                    processRunFile(runReader, runSizeInFrames, runSizeInTuples, 0);
                }
                writer.close();

                // FIXME
                timer2 = System.currentTimeMillis() - timer2;
                LOGGER.warning("RecursivePartitions\t" + timer2 + "\t" + ctx.getIOManager().toString() + "\t"
                        + reloadReadTime);
            }

            private void processRunFile(RunFileReader runReader, int runFileSizeInFrames, int runFileSizeInTuples,
                    int level) throws HyracksDataException {

                long timer = System.currentTimeMillis();
                long readTimer = 0;
                long tempTimer;

                LOGGER.warning("Level-" + level + "-HybridHash-Open\t" + ctx.getIOManager().toString());

                int newTableSize = tableSize;
                // resize the hash table size, if the input size is less
                if (newTableSize > 2 * runFileSizeInTuples) {
                    newTableSize = 2 * runFileSizeInTuples;
                }

                int partitionCount = getNumberOfPartitions(newTableSize, framesLimit, runFileSizeInFrames, 1,
                        fudgeFactor);

                HybridHashGroupHashTable ht = new HybridHashGroupHashTable(ctx, framesLimit, newTableSize,
                        partitionCount, level + 1, storedKeyFields, comparators, mergeTpcf, merger, merger,
                        recordDescriptors[0], recordDescriptors[0], writer, allowReload);

                tempTimer = System.currentTimeMillis();
                boolean hasNextFrame = runReader.nextFrame(runProcessBuff);
                readTimer += System.currentTimeMillis() - tempTimer;

                while (hasNextFrame) {
                    runBufferFrameAccessor.reset(runProcessBuff);
                    int tupleCount = runBufferFrameAccessor.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        ht.insert(runBufferFrameAccessor, i);
                    }
                    tempTimer = System.currentTimeMillis();
                    hasNextFrame = runReader.nextFrame(runProcessBuff);
                    readTimer += System.currentTimeMillis() - tempTimer;
                }

                ht.finishup();

                // close the run reader
                runReader.close();

                LinkedList<RunFileReader> runReaders = ht.getRunFileReaders();
                LinkedList<Integer> runSizesInFrames = ht.getRunFileSizesInFrame();
                LinkedList<Integer> runSizesInTuples = ht.getRunFileSizesInTuple();

                ht.close();
                ht = null;

                LOGGER.warning("Level-" + level + "-HybridHash-Close\t" + (System.currentTimeMillis() - timer) + "\t"
                        + ctx.getIOManager().toString() + "\t" + runReaders.size() + "\t" + readTimer);

                if (runReaders.size() <= 0) {
                    return;
                }

                readTimer = 0;

                int subRunIndex = 0;
                while (!runReaders.isEmpty()) {
                    RunFileReader subRunReader = runReaders.remove();
                    int subRunSizeInFrame = runSizesInFrames.remove();
                    int subRunSizeInTuple = runSizesInTuples.remove();
                    subRunReader.open();
                    if (subRunSizeInFrame > runFileSizeInFrames * HYBRID_SWITCH_THRESHOLD) {
                        // fall back to the hybrid-hash-sort algorithm, in case that the hash function is bad
                        timer = System.currentTimeMillis();

                        LOGGER.warning("Level-" + level + "-Run-" + subRunIndex + "-HybridHash-FallbackHS-Open\t"
                                + ctx.getIOManager().toString() + "\t" + runReaders.size() + "\t" + subRunSizeInFrame
                                + "\t" + subRunSizeInTuple);

                        int runTableSize = 2 * subRunSizeInTuple;

                        if (runTableSize > tableSize) {
                            runTableSize = tableSize;
                        }

                        ITuplePartitionComputer mergeTpc = mergeTpcf.createPartitioner(level + 2);

                        // fallback to hybrid-hash-sort
                        HybridHashSortGroupHashTable hhsTable = new HybridHashSortGroupHashTable(ctx, framesLimit,
                                runTableSize, storedKeyFields, comparators, mergeTpc,
                                firstNormalizerFactory.createNormalizedKeyComputer(), merger, recordDescriptors[0],
                                recordDescriptors[0]);

                        tempTimer = System.currentTimeMillis();
                        hasNextFrame = subRunReader.nextFrame(runProcessBuff);
                        readTimer += System.currentTimeMillis() - tempTimer;

                        while (hasNextFrame) {
                            runBufferFrameAccessor.reset(runProcessBuff);
                            int tupleCount = runBufferFrameAccessor.getTupleCount();
                            for (int i = 0; i < tupleCount; i++) {
                                hhsTable.insert(runBufferFrameAccessor, i);
                            }

                            tempTimer = System.currentTimeMillis();
                            hasNextFrame = subRunReader.nextFrame(runProcessBuff);
                            readTimer += System.currentTimeMillis() - tempTimer;
                        }

                        hhsTable.finishup();
                        subRunReader.close();

                        LinkedList<RunFileReader> hhsRuns = hhsTable.getRunFileReaders();

                        LOGGER.warning("Level-" + level + "-Run-" + subRunIndex + "-HybridHash-FallbackHS-Hash-Close\t"
                                + ctx.getIOManager().toString() + "\t" + hhsRuns.size() + "\t" + readTimer);
                        if (hhsRuns.isEmpty()) {
                            hhsTable.flushHashtableToOutput(writer);
                            hhsTable.close();
                        } else {
                            hhsTable.close();
                            HybridHashSortRunMerger bMerger = new HybridHashSortRunMerger(ctx, hhsRuns,
                                    storedKeyFields, comparators, recordDescriptors[0], mergeTpc, merger, framesLimit,
                                    runTableSize, writer, false);
                            bMerger.process();
                        }

                        timer = System.currentTimeMillis() - timer;
                        LOGGER.warning("Level-" + level + "-Run-" + subRunIndex
                                + "-HybridHash-FallbackHS-Merge-Close\t" + timer + "\t" + ctx.getIOManager().toString());
                    } else {
                        // recursively processing
                        processRunFile(subRunReader, subRunSizeInFrame, subRunSizeInTuple, level + 1);
                    }
                    subRunReader.close();
                    subRunIndex++;
                }
            }
        };
    }
}
