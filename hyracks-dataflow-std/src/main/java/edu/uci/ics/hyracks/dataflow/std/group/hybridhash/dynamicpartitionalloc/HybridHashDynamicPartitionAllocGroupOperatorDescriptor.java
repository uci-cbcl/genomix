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

import java.nio.ByteBuffer;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortRunMerger;
import edu.uci.ics.hyracks.dataflow.std.group.struct.ISerializableGroupHashTableForHashHash;

/**
 * An implementation of the static range partition hybrid-hash algorithm described in [DBLP:conf/sigmod/ShatdalN95].
 * <p/>
 * The basic algorithm works as follows:<br/>
 * - Calculate the total partition number using the hybrid-hash formula from [DBLP:journals/tods/Shapiro86];<br/>
 * - Keys inserted into the hash table are partitioned, so that partition 0 is treated specially as the in-memory
 * partition: when the table is full and a partition is requesting more space, one resident partition among [1, P] will
 * be spilled, if any. Partition 0 will be spilled if it is the only resident partition.<br/>
 * - Memory is dynamically allocated for partitions.
 */
public class HybridHashDynamicPartitionAllocGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;
    private final int[] keyFields, storedKeyFields;

    private final IAggregatorDescriptorFactory topAggregatorFactory;
    private final IAggregatorDescriptorFactory recurAggregatorFactory;

    private final int framesLimit;
    private final ITuplePartitionComputerFamily topTpcf, recursiveTpcf;
    private final IBinaryComparatorFactory[] comparatorFactories;

    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final int tableSize, inputKeySizeInFrames, estimatedRecordSize;

    private static final double HYBRID_SWITCH_THRESHOLD = 0.8;

    private static final double ESTIMATOR_MAGNIFIER = 1.2;

    private final int algoType;

    public HybridHashDynamicPartitionAllocGroupOperatorDescriptor(JobSpecification spec, int[] keyFields,
            int framesLimit, int inputKeySizeInFrames, int tableSize, int estimatedRecordSize,
            IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFamily topTpcf,
            ITuplePartitionComputerFamily recursiveTpcf, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory topAggregatorFactory, IAggregatorDescriptorFactory recurAggregatorFactory,
            RecordDescriptor recordDescriptor) {
        // use dynamic-destaging by default
        this(spec, keyFields, framesLimit, inputKeySizeInFrames, tableSize, estimatedRecordSize, comparatorFactories,
                topTpcf, recursiveTpcf, firstNormalizerFactory, topAggregatorFactory, recurAggregatorFactory,
                recordDescriptor, 0);
    }

    public HybridHashDynamicPartitionAllocGroupOperatorDescriptor(JobSpecification spec, int[] keyFields,
            int framesLimit, int inputKeySizeInFrames, int tableSize, int estimatedRecordSize,
            IBinaryComparatorFactory[] comparatorFactories, ITuplePartitionComputerFamily topTpcf,
            ITuplePartitionComputerFamily recursiveTpcf, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory topAggregatorFactory, IAggregatorDescriptorFactory recurAggregatorFactory,
            RecordDescriptor recordDescriptor, int algoType) {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.inputKeySizeInFrames = inputKeySizeInFrames;

        this.estimatedRecordSize = estimatedRecordSize;

        if (framesLimit <= 4) {
            /**
             * Minimum of 4 frames: 3 for in-memory hash table (3 pages for at least 2 partitions), and 1 for output
             * aggregation results.
             */
            throw new IllegalStateException("frame limit should at least be 4, but it is " + framesLimit + "!");
        }

        storedKeyFields = new int[keyFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        this.topAggregatorFactory = topAggregatorFactory;
        this.recurAggregatorFactory = recurAggregatorFactory;
        this.keyFields = keyFields;
        this.comparatorFactories = comparatorFactories;
        this.topTpcf = topTpcf;
        this.recursiveTpcf = recursiveTpcf;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.tableSize = tableSize;
        this.algoType = algoType;

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
            IRecordDescriptorProvider recordDescProvider, int partition, final int nPartitions)
            throws HyracksDataException {

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparatorFactories.length; ++i) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);

        final int frameSize = ctx.getFrameSize();

        final double fudgeFactor = HybridHashDynamicDestagingGroupHashTable.getHashTableFudgeFactor(tableSize,
                estimatedRecordSize, frameSize, framesLimit);

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            // FIXME
            private long timer;

            ISerializableGroupHashTableForHashHash hashtable;

            /**
             * Compute the initial partition fan-out. The initial partition number is computed using the original
             * hybrid-hash formula. Then the following cases are considered:
             * <p/>
             * - if the partition number is no larger than 1, use 2 partitions, in case that all input will be
             * partitioned into a single partition and spilled.<br/>
             * - if the partition number is larger than the available memory (total memory minus the header pages for
             * hash table and the output buffer), use (framesLimit - 1) partitions to do partition. <br/>
             * 
             * @param tableSize
             * @param framesLimit
             * @param inputSize
             * @param partitionInOperator
             * @param factor
             * @return
             */
            private int getNumberOfPartitions(int tableSize, int framesLimit, int inputKeySize,
                    int partitionInOperator, double factor) {
                double inputKeySizeForThisPartition = (double) (inputKeySize / partitionInOperator);

                int numberOfPartitions, hashtableHeaderPages;

                switch (algoType) {
                    case 3:
                        hashtableHeaderPages = HybridHashDynamicDestagingGroupHashTable.getHeaderPages(tableSize,
                                frameSize);
                        if (inputKeySizeForThisPartition * factor >= framesLimit * framesLimit) {
                            numberOfPartitions = framesLimit - 1;
                        } else {
                            // use hybrid-hash formula to compute the partition number
                            numberOfPartitions = (int) Math
                                    .ceil((inputKeySizeForThisPartition * factor - framesLimit + 1) / (framesLimit - 2)) + 1;
                        }

                        // if hybrid-hash: adjust the partition output buffers to occupy 50%~80% of the total memory
                        if (numberOfPartitions <= framesLimit - 1 - hashtableHeaderPages) {
                            if (numberOfPartitions < 0.5 * framesLimit) {
                                numberOfPartitions = (int) (0.5 * framesLimit);
                            } else if (numberOfPartitions > 0.8 * framesLimit) {
                                numberOfPartitions = (int) (0.8 * framesLimit);
                            }
                        }

                        break;
                    case 0:
                        hashtableHeaderPages = HybridHashDynamicDestagingGroupHashTable.getHeaderPages(tableSize,
                                frameSize);
                        if (inputKeySizeForThisPartition * factor >= framesLimit * framesLimit) {
                            numberOfPartitions = framesLimit - 1;
                        } else {
                            // Let the partition size to be the smallest size of all partitions, if partitioning as
                            // the hybrid-hash algorithm
                            double partitionSize = Math.min((framesLimit - 1) / factor, ((framesLimit - 1)
                                    * (framesLimit - 1) - inputKeySizeForThisPartition * factor)
                                    / (factor * (framesLimit - 2)));

                            numberOfPartitions = (int) Math.floor(inputKeySizeForThisPartition / partitionSize);
                        }
                        break;
                    case 4:
                    case 2:
                    case 1:
                    default:
                        hashtableHeaderPages = HybridHashStaticRangePartitionGroupHashTable.getHeaderPages(tableSize,
                                frameSize);
                        // Compute the partition count according to the hybrid-hash partition formula.
                        numberOfPartitions = (int) Math.ceil((inputKeySizeForThisPartition * factor - framesLimit + 1)
                                / (framesLimit - 2)) + 1;
                }

                if (numberOfPartitions <= 1) {
                    // at least two partitions
                    numberOfPartitions = 2;
                }

                // Pure partition if partition count is larger than the available frame size, 
                if (numberOfPartitions > framesLimit - 1 - hashtableHeaderPages) {
                    // at most to occupy all available frame
                    numberOfPartitions = framesLimit - 1;
                }

                return numberOfPartitions;
            }

            IAggregatorDescriptor topAggregator = topAggregatorFactory.createAggregator(ctx, inRecDesc,
                    recordDescriptors[0], keyFields, storedKeyFields);

            IAggregatorDescriptor recurAggregator = recurAggregatorFactory.createAggregator(ctx, recordDescriptors[0],
                    recordDescriptors[0], storedKeyFields, storedKeyFields);

            FrameTupleAccessor inBufferFrameAccessor = new FrameTupleAccessor(frameSize, inRecDesc);
            FrameTupleAccessor runBufferFrameAccessor = new FrameTupleAccessor(frameSize, recordDescriptors[0]);

            ByteBuffer runProcessBuff = null;

            /**
             * Raw size of the input data set in frames and tuples
             */
            int inputRawSizeInFrames, inputRawSizeInTuples;

            @Override
            public void open() throws HyracksDataException {
                // FIXME
                timer = System.nanoTime();

                inputRawSizeInFrames = 0;
                inputRawSizeInTuples = 0;

                writer.open();

                ctx.getCounterContext().getCounter("must.algo", true).set(4);

                ctx.getCounterContext().getCounter("must.mem", true).set(framesLimit);

                ctx.getCounterContext().getCounter("must.hash.slots.count", true).set(tableSize);

                ctx.getCounterContext().getCounter("optional.fudgeFactor", true).set((int) (fudgeFactor * 10000));

                ctx.getCounterContext().getCounter("optional.levels.0.inputKeySize", true).set(inputKeySizeInFrames);

                ctx.getCounterContext().getCounter("must.sort.comps", true).set(0);
                ctx.getCounterContext().getCounter("must.sort.swaps", true).set(0);
                ctx.getCounterContext().getCounter("must.merge.comps", true).set(0);
                ctx.getCounterContext().getCounter("must.merge.swaps", true).set(0);

                // calculate the number of partitions (including partition 0)
                int topParts = getNumberOfPartitions(tableSize, framesLimit,
                        (int) (inputKeySizeInFrames * ESTIMATOR_MAGNIFIER), nPartitions, fudgeFactor);

                ctx.getCounterContext().getCounter("optional.levels.0.partitions", true).set(topParts);

                switch (algoType) {
                    case 2:
                        // calculate the hash key range for partition 0:
                        hashtable = new HybridHashStrictStaticRangePartitionGroupHashTable(ctx, framesLimit, tableSize,
                                topParts, 0, keyFields, comparators, topTpcf, topAggregator, inRecDesc,
                                recordDescriptors[0], writer);
                        break;
                    case 1:
                        // calculate the hash key range for partition 0:
                        int partitionZeroKeyRange = (int) Math.ceil((double) tableSize * (framesLimit - topParts)
                                / (inputKeySizeInFrames * ESTIMATOR_MAGNIFIER / nPartitions * fudgeFactor));
                        hashtable = new HybridHashStaticRangePartitionGroupHashTable(ctx, framesLimit, tableSize,
                                topParts, partitionZeroKeyRange, 0, keyFields, comparators, topTpcf, topAggregator,
                                inRecDesc, recordDescriptors[0], writer);
                        break;
                    case 4:
                        hashtable = new HybridHashOriginalStaticRangePartitionGroupHashTable(ctx, framesLimit,
                                tableSize, topParts, 0, keyFields, comparators, topTpcf, recursiveTpcf, topAggregator,
                                inRecDesc, recordDescriptors[0], writer);
                        break;
                    case 3:
                    case 0:
                    default:
                        hashtable = new HybridHashDynamicDestagingGroupHashTable(ctx, framesLimit, tableSize, topParts,
                                0, keyFields, comparators, topTpcf, topAggregator, inRecDesc, recordDescriptors[0],
                                writer);
                }

            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                inputRawSizeInFrames++;
                inBufferFrameAccessor.reset(buffer);
                int tupleCount = inBufferFrameAccessor.getTupleCount();
                inputRawSizeInTuples += tupleCount;
                for (int i = 0; i < tupleCount; i++) {
                    hashtable.insert(inBufferFrameAccessor, i);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {

                hashtable.finishup();

                LinkedList<RunFileReader> runReaders = hashtable.getRunFileReaders();

                // Get the raw record size of each partition (in records but not in pages)
                LinkedList<Integer> partitionRawRecordsCount = hashtable.getSpilledPartitionRawRecordCount();

                int hashedRawRecords = hashtable.getHashedRawRecords();
                int hashedKeys = hashtable.getHashedKeys();

                // get a new estimation on the number of keys in the input data set: if no statistics have been
                // collected, use the input key size; otherwise, use the estimated key size. Either case the magnifier
                // is applied to make sure that we will not have an underestimated size.
                int newKeySizeInFrames = (int) Math.ceil((hashedRawRecords == 0) ? inputKeySizeInFrames
                        * ESTIMATOR_MAGNIFIER : (int) Math.ceil((double) hashedKeys / hashedRawRecords
                        * inputRawSizeInFrames * ESTIMATOR_MAGNIFIER));

                ctx.getCounterContext().getCounter("optional.levels.0.estiInputKeySize", true).set(newKeySizeInFrames);

                hashtable.close();

                hashtable = null;

                ctx.getCounterContext().getCounter("optional.levels.0.time", true).set(System.nanoTime() - timer);

                ctx.getCounterContext().getCounter("optional.levels.0.io", true)
                        .set(Long.valueOf(ctx.getIOManager().toString()));

                ctx.getCounterContext().getCounter("optional.levels.0.runs.count", true).set(runReaders.size());

                timer = System.nanoTime();

                RunFileReader runReader;
                if (runProcessBuff == null) {
                    runProcessBuff = ctx.allocateFrame();
                }

                // process each run files
                while (!runReaders.isEmpty()) {
                    runReader = runReaders.remove();

                    int partitionRawRecords = partitionRawRecordsCount.remove();

                    runReader.open();

                    // estimate the partition key size using the partition raw size and the estimated total key size.
                    int runKeySizeInFrames = (int) Math.ceil((double) newKeySizeInFrames * partitionRawRecords
                            / inputRawSizeInTuples);

                    if (runKeySizeInFrames > HYBRID_SWITCH_THRESHOLD * newKeySizeInFrames) {
                        fallBackAlgorithm(runReader, 1);
                    } else {
                        processRunFile(runReader, runKeySizeInFrames, 1);
                    }
                }
                writer.close();

                // FIXME
                ctx.getCounterContext().getCounter("optional.recursiveHash.time", true).set(System.nanoTime() - timer);

                ctx.getCounterContext().getCounter("must.io", true).set(Long.valueOf(ctx.getIOManager().toString()));
            }

            private void processRunFile(RunFileReader runReader, int runFileKeySizeInFrames, int level)
                    throws HyracksDataException {

                long rtimer = System.nanoTime();

                int newTableSize = tableSize;

                ctx.getCounterContext().getCounter("optional.levels." + level + ".inputKeySize", true)
                        .update(runFileKeySizeInFrames);

                // partition counts for this run file
                int partitionCount = getNumberOfPartitions(newTableSize, framesLimit, runFileKeySizeInFrames, 1,
                        fudgeFactor);

                ctx.getCounterContext().getCounter("optional.levels." + level + ".partitions", true)
                        .update(runFileKeySizeInFrames);

                ISerializableGroupHashTableForHashHash ht;

                switch (algoType) {
                    case 2:
                        ht = new HybridHashStrictStaticRangePartitionGroupHashTable(ctx, framesLimit, newTableSize,
                                partitionCount, level, storedKeyFields, comparators, recursiveTpcf, recurAggregator,
                                recordDescriptors[0], recordDescriptors[0], writer);
                        break;
                    case 1:
                        // calculate the hash key range for partition 0: note that the run file key size is not adjusted
                        // by the magnifier, as it has been done before this method is called.
                        int partitionZeroKeyRange = (int) Math.ceil((double) tableSize * (framesLimit - partitionCount)
                                / (runFileKeySizeInFrames / nPartitions * fudgeFactor));
                        ht = new HybridHashStaticRangePartitionGroupHashTable(ctx, framesLimit, newTableSize,
                                partitionCount, partitionZeroKeyRange, level, storedKeyFields, comparators,
                                recursiveTpcf, recurAggregator, recordDescriptors[0], recordDescriptors[0], writer);
                        break;
                    case 4:
                        ht = new HybridHashOriginalStaticRangePartitionGroupHashTable(ctx, framesLimit, newTableSize,
                                partitionCount, level, storedKeyFields, comparators, recursiveTpcf, recursiveTpcf,
                                recurAggregator, recordDescriptors[0], recordDescriptors[0], writer);
                        break;
                    case 3:
                    case 0:
                    default:
                        ht = new HybridHashDynamicDestagingGroupHashTable(ctx, framesLimit, newTableSize,
                                partitionCount, level, storedKeyFields, comparators, recursiveTpcf, recurAggregator,
                                recordDescriptors[0], recordDescriptors[0], writer);

                }

                int inputSizesInFrames = 0, inputSizesInTuples = 0;

                while (runReader.nextFrame(runProcessBuff)) {
                    inputSizesInFrames++;
                    runBufferFrameAccessor.reset(runProcessBuff);
                    int tupleCount = runBufferFrameAccessor.getTupleCount();
                    inputSizesInTuples += tupleCount;
                    for (int i = 0; i < tupleCount; i++) {
                        ht.insert(runBufferFrameAccessor, i);
                    }
                }

                ht.finishup();

                // close the run reader
                runReader.close();

                LinkedList<RunFileReader> runReaders = ht.getRunFileReaders();
                LinkedList<Integer> partitionRawRecords = ht.getSpilledPartitionRawRecordCount();

                int hashedKeys = ht.getHashedKeys();
                int hashedRawRecords = ht.getHashedRawRecords();

                // estimate the new input key size based on the statistics collected. note that
                // the magnifier is not used if no statistics is gained and the part of the previous
                // partition size is used
                int newEstimatedKeysForInput = hashedRawRecords == 0 ? runFileKeySizeInFrames : (int) Math
                        .ceil((double) hashedKeys / hashedRawRecords * inputSizesInFrames * ESTIMATOR_MAGNIFIER);

                ctx.getCounterContext().getCounter("optional.levels." + level + ".estiInputKeySize", true)
                        .update(newEstimatedKeysForInput);

                ctx.getCounterContext().getCounter("optional.levels." + level + ".runs.count", true)
                        .update(runReaders.size());

                ctx.getCounterContext().getCounter("optional.levels." + level + ".time", true)
                        .update(System.nanoTime() - rtimer);

                ctx.getCounterContext().getCounter("optional.levels." + level + ".io", true)
                        .update(Long.valueOf(ctx.getIOManager().toString()));

                ht.close();
                ht = null;

                if (runReaders.size() <= 0) {
                    return;
                }

                while (!runReaders.isEmpty()) {
                    RunFileReader subRunReader = runReaders.remove();

                    int subPartitionRawRecords = partitionRawRecords.remove();

                    subRunReader.open();

                    int subRunKeySizeInFrame = (int) Math.ceil((double) subPartitionRawRecords
                            * newEstimatedKeysForInput / inputSizesInTuples);
                    //(runFileKeySizeInFrames - htDirectOutputSizeInFrames)
                    //* subRunRawSizeInFrame / runFileRawSizeInFrames;

                    if (subRunKeySizeInFrame > newEstimatedKeysForInput * HYBRID_SWITCH_THRESHOLD) {
                        // fall back to the hybrid-hash-sort algorithm, in case that the hash function is bad
                        rtimer = System.nanoTime();

                        fallBackAlgorithm(subRunReader, level + 1);

                    } else {
                        // recursively processing
                        processRunFile(subRunReader, subRunKeySizeInFrame, level + 1);
                    }
                    subRunReader.close();
                }
            }

            /**
             * Fall back to hash-sort algorithm for the given run file.
             * 
             * @param recurRunReader
             * @param runLevel
             * @throws HyracksDataException
             */
            private void fallBackAlgorithm(IFrameReader recurRunReader, int runLevel) throws HyracksDataException {
                long currentIO = Long.valueOf(ctx.getIOManager().toString());
                FrameTupleAccessor runFrameTupleAccessor = new FrameTupleAccessor(ctx.getFrameSize(),
                        recordDescriptors[0]);
                HybridHashSortGroupHashTable hhsTable = new HybridHashSortGroupHashTable(ctx, framesLimit, tableSize,
                        storedKeyFields, comparators, recursiveTpcf.createPartitioner(runLevel + 2),
                        firstNormalizerFactory.createNormalizedKeyComputer(), recurAggregatorFactory.createAggregator(
                                ctx, recordDescriptors[0], recordDescriptors[0], storedKeyFields, storedKeyFields),
                        recordDescriptors[0], recordDescriptors[0]);

                recurRunReader.open();
                while (recurRunReader.nextFrame(runProcessBuff)) {
                    runFrameTupleAccessor.reset(runProcessBuff);
                    int tupleCount = runFrameTupleAccessor.getTupleCount();
                    for (int j = 0; j < tupleCount; j++) {
                        hhsTable.insert(runFrameTupleAccessor, j);
                    }
                }

                recurRunReader.close();
                hhsTable.finishup();

                LinkedList<RunFileReader> hhsRuns = hhsTable.getRunFileReaders();

                if (hhsRuns.isEmpty()) {
                    hhsTable.flushHashtableToOutput(writer);
                    hhsTable.close();
                } else {
                    hhsTable.close();
                    HybridHashSortRunMerger hhsMerger = new HybridHashSortRunMerger(ctx, hhsRuns, storedKeyFields,
                            comparators, recordDescriptors[0], recursiveTpcf.createPartitioner(runLevel + 2),
                            recurAggregatorFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                                    storedKeyFields, storedKeyFields), framesLimit, tableSize, writer, false);
                    hhsMerger.process();
                }

                ctx.getCounterContext().getCounter("optional.fallback.io", true)
                        .update(Long.valueOf(ctx.getIOManager().toString()) - currentIO);
            }
        };
    }
}
