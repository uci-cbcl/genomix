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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash.prepart;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFamily;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerOffsetFamily;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortRunMerger;

public class HashHashGroupOpeartorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final double HYBRID_FALLBACK_THRESHOLD = 0.8;

    private static final double ESTIMATOR_MAGNIFIER = 1.2;

    // input key fields
    private final int[] keyFields;

    // intermediate and final key fields
    private final int[] storedKeyFields;

    // hash table size
    private final int tableSize;

    // input size in pages
    private final int inputKeySizeInFrames;

    // estimated record size: used for compute the fudge factor
    private final int estimatedRecordSizeInBytes;

    // aggregator
    private final IAggregatorDescriptorFactory aggregatorFactory;

    // merger, in case of falling back to the hash-sort algorithm for hash skewness
    private final IAggregatorDescriptorFactory mergerFactory;

    // for the sort fall-back algorithm
    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    // total memory in pages
    private final int framesLimit;

    // comparator factories for key fields.
    private final IBinaryComparatorFactory[] comparatorFactories;

    /**
     * hash families for each field: a hash function family is need as we may have
     * more than one levels of hashing
     */
    private final IBinaryHashFunctionFamily[] hashFamilies;

    /**
     * hash seed starting offset. This is used to have different hash function series for multiple levels
     * of hash-hash aggregations (as in a cluster).
     */
    private int hashSeedOffset;

    /**
     * Hash table factory.
     */
    private final IHashHashSerializableHashTableFactory hashtableFactory;

    /****************************
     * For Logging
     ****************************/
    private static final Logger LOGGER = Logger.getLogger(HashHashGroupOpeartorDescriptor.class.getSimpleName());

    /**
     * @param spec
     * @param keyFields
     * @param framesLimit
     * @param inputSizeInFrames
     * @param inputKeySizeInFrames
     * @param recordSizeInBytes
     * @param tableSize
     * @param comparatorFactories
     * @param hashFamilies
     * @param hashSeedOffset
     * @param hashtableFactory
     * @param firstNormalizerFactory
     * @param aggregatorFactory
     * @param mergerFactory
     * @param outRecDesc
     * @throws HyracksDataException
     */
    public HashHashGroupOpeartorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            int inputKeySizeInFrames, int recordSizeInBytes, int tableSize,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFamily[] hashFamilies,
            int hashSeedOffset, IHashHashSerializableHashTableFactory hashtableFactory,
            INormalizedKeyComputerFactory firstNormalizerFactory, IAggregatorDescriptorFactory aggregatorFactory,
            IAggregatorDescriptorFactory mergerFactory, RecordDescriptor outRecDesc) throws HyracksDataException {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        this.inputKeySizeInFrames = inputKeySizeInFrames;
        this.estimatedRecordSizeInBytes = recordSizeInBytes;

        if (framesLimit <= 3) {
            // at least 3 frames: 2 for in-memory hash table, and 1 for output buffer
            throw new HyracksDataException(
                    "Not enough memory for Hash-Hash Aggregation algorithm: at least 3 frames are necessary, but only "
                            + framesLimit + " available.");
        }

        this.keyFields = keyFields;
        storedKeyFields = new int[keyFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }

        this.aggregatorFactory = aggregatorFactory;

        this.mergerFactory = mergerFactory;
        this.firstNormalizerFactory = firstNormalizerFactory;

        this.comparatorFactories = comparatorFactories;

        this.hashtableFactory = hashtableFactory;

        this.hashFamilies = hashFamilies;

        this.hashSeedOffset = hashSeedOffset;

        recordDescriptors[0] = outRecDesc;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, final int nPartitions)
            throws HyracksDataException {

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getOperatorId(), 0);

        final int frameSize = ctx.getFrameSize();

        final double fudgeFactor = hashtableFactory.getHashTableFudgeFactor(tableSize, estimatedRecordSizeInBytes,
                frameSize, framesLimit);

        final ByteBuffer tempBuf = ctx.allocateFrame();

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            IHashHashSpillableFrameWriter topProcessor;

            int inputSizeInFrames;

            long level0Timer;

            boolean topLevelFallbackCheck = true;

            ITuplePartitionComputerFamily tpcf = new FieldHashPartitionComputerOffsetFamily(keyFields, hashFamilies,
                    hashSeedOffset);

            /**
             * Compute the expected number of spilling partitions, using the hybrid-hash algorithm from [Shapiro86]
             * 
             * @param inputSizeInFrames
             * @param memorySizeInFrames
             * @param fudgeFactor
             * @return
             */
            private int hybridHashPartitionComputer(int inputSizeInFrames, int memorySizeInFrames, double fudgeFactor) {
                return Math.max(
                        (int) Math.ceil((inputSizeInFrames * fudgeFactor - memorySizeInFrames)
                                / (memorySizeInFrames - 2)), 1);
            }

            private int getNumberOfPartitions(int tableSize, int framesLimit, int inputKeySize,
                    int partitionInOperator, double factor) {
                double inputKeySizeForThisPartition = (double) (inputKeySize / partitionInOperator);

                int hashtableHeaderPages = hashtableFactory.getHeaderPages(tableSize, frameSize);

                int numberOfPartitions = hybridHashPartitionComputer((int) Math.ceil(inputKeySizeForThisPartition),
                        framesLimit, factor);

                if (numberOfPartitions < 1) {
                    numberOfPartitions = 1;
                }

                if (numberOfPartitions >= framesLimit - hashtableHeaderPages) {
                    // pure partition
                    numberOfPartitions = framesLimit;
                }

                return numberOfPartitions;
            }

            @Override
            public void open() throws HyracksDataException {

                ctx.getCounterContext().getCounter("must.algo", true).set(2);

                ctx.getCounterContext().getCounter("must.mem", true).set(framesLimit);

                ctx.getCounterContext().getCounter("must.hash.slots.count", true).set(tableSize);

                ctx.getCounterContext().getCounter("optional.fudgeFactor", true).set((int) (fudgeFactor * 10000));

                ctx.getCounterContext().getCounter("optional.levels.0.inputKeySize", true).set(inputKeySizeInFrames);

                ctx.getCounterContext().getCounter("must.sort.comps", true).set(0);
                ctx.getCounterContext().getCounter("must.sort.swaps", true).set(0);
                ctx.getCounterContext().getCounter("must.merge.comps", true).set(0);
                ctx.getCounterContext().getCounter("must.merge.swaps", true).set(0);

                level0Timer = System.nanoTime();

                inputSizeInFrames = 0;

                int topPartitions = getNumberOfPartitions(tableSize, framesLimit,
                        (int) Math.ceil(inputKeySizeInFrames * ESTIMATOR_MAGNIFIER), nPartitions, fudgeFactor);

                if (topPartitions == framesLimit) {
                    // pure partition
                    topProcessor = new HashHashPartitionGenerateFrameWriter(ctx, framesLimit,
                            tpcf.createPartitioner(0), inRecDesc);

                    ctx.getCounterContext().getCounter("optional.levels.0.type", true).update(1);
                } else {
                    // skip fallback check in case that only one spilling partition is produced
                    if (topPartitions == 1) {
                        topLevelFallbackCheck = false;
                    }

                    topProcessor = hashtableFactory.createHashTable(ctx, framesLimit, tableSize, topPartitions,
                            keyFields, comparators, 0, tpcf, aggregatorFactory.createAggregator(ctx, inRecDesc,
                                    recordDescriptors[0], keyFields, storedKeyFields), mergerFactory.createAggregator(
                                    ctx, recordDescriptors[0], recordDescriptors[0], storedKeyFields, storedKeyFields),
                            inRecDesc, recordDescriptors[0], writer);

                    ctx.getCounterContext().getCounter("optional.levels.0.type", true).update(100000000);

                    ctx.getCounterContext().getCounter("optional.levels.0.partitions", true).update(topPartitions);
                }

                writer.open();
                topProcessor.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                inputSizeInFrames++;
                topProcessor.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {

                topProcessor.finishup();

                ctx.getCounterContext().getCounter("optional.levels.0.time", true).set(System.nanoTime() - level0Timer);

                ctx.getCounterContext().getCounter("optional.levels.0.io", true)
                        .set(Long.valueOf(ctx.getIOManager().toString()));

                List<IFrameReader> runs = topProcessor.getSpilledRuns();
                List<Integer> runSizesInPages = topProcessor.getSpilledRunsSizeInPages();

                // get statistics from the hash table
                int directFlushedKeysInTuples = topProcessor.getDirectFlushSizeInTuples();
                int directFlushedRawRecordsInTuples = topProcessor.getDirectFlushRawSizeInTuples();

                ctx.getCounterContext().getCounter("optional.levels.0.runs.count", true).update(runs.size());

                topProcessor.close();

                // get a new estimation on the number of keys in the input data set
                int newKeySizeInPages = (directFlushedRawRecordsInTuples == 0) ? (int) Math.ceil(inputKeySizeInFrames
                        * ESTIMATOR_MAGNIFIER) : (int) Math.ceil((double) directFlushedKeysInTuples
                        / directFlushedRawRecordsInTuples * inputSizeInFrames * ESTIMATOR_MAGNIFIER);

                ctx.getCounterContext().getCounter("optional.levels.0.estiInputKeySize", true)
                        .update(newKeySizeInPages);

                for (int i = 0; i < runs.size(); i++) {

                    // compute the estimated key size in frames for the run file
                    int runKeySize = (int) Math.ceil((double) newKeySizeInPages * runSizesInPages.get(i)
                            / inputSizeInFrames);

                    if (topLevelFallbackCheck && runKeySize > HYBRID_FALLBACK_THRESHOLD * newKeySizeInPages) {
                        fallBack(runs.get(i), runSizesInPages.get(i), runKeySize, 1);
                    } else {
                        processRunFiles(runs.get(i), runKeySize, 1);
                    }
                }

                writer.close();

                ctx.getCounterContext().getCounter("must.io", true).update(Long.valueOf(ctx.getIOManager().toString()));

            }

            private void processRunFiles(IFrameReader runReader, int runKeySizeInFrames, int runLevel)
                    throws HyracksDataException {

                //LOGGER.warning(HashHashGroupOpeartorDescriptor.class.getSimpleName() + "-Process\t" + runLevel + "\t"
                //        + runKeySizeInFrames + "\t" + fudgeFactor + "\t" + framesLimit);

                long timer = System.nanoTime(), rio = Long.valueOf(ctx.getIOManager().toString());

                List<IFrameReader> runs;
                List<Integer> runSizes;
                IHashHashSpillableFrameWriter processor;

                boolean checkFallback = true;

                int numOfPartitions = getNumberOfPartitions(tableSize, framesLimit, runKeySizeInFrames, 1, fudgeFactor);

                if (numOfPartitions == framesLimit) {
                    processor = new HashHashPartitionGenerateFrameWriter(ctx, framesLimit,
                            tpcf.createPartitioner(runLevel), inRecDesc);

                    ctx.getCounterContext().getCounter("optional.levels." + runLevel + ".type", true).update(1);
                } else {
                    if (numOfPartitions == 1) {
                        checkFallback = false;
                    }

                    processor = hashtableFactory.createHashTable(ctx, framesLimit, tableSize, numOfPartitions,
                            keyFields, comparators, runLevel, tpcf, aggregatorFactory.createAggregator(ctx, inRecDesc,
                                    recordDescriptors[0], keyFields, storedKeyFields),
                            aggregatorFactory.createAggregator(ctx, inRecDesc, recordDescriptors[0], keyFields,
                                    storedKeyFields), inRecDesc, recordDescriptors[0], writer);

                    ctx.getCounterContext().getCounter("optional.levels." + runLevel + ".type", true).update(100000000);
                }

                processor.open();

                runReader.open();

                int inputRunRawSizeInFrames = 0;

                while (runReader.nextFrame(tempBuf)) {
                    processor.nextFrame(tempBuf);
                    inputRunRawSizeInFrames++;
                }

                runReader.close();

                processor.finishup();

                runs = processor.getSpilledRuns();
                runSizes = processor.getSpilledRunsSizeInPages();

                ctx.getCounterContext().getCounter("optional.levels." + runLevel + ".runs.count", true)
                        .update(runs.size());

                ctx.getCounterContext().getCounter("optional.levels." + runLevel + ".io", true)
                        .update(Long.valueOf(ctx.getIOManager().toString()) - rio);

                ctx.getCounterContext().getCounter("optional.levels." + runLevel + ".time", true)
                        .update(System.nanoTime() - timer);

                int directFlushKeysInTuples = processor.getDirectFlushSizeInTuples();
                int directFlushRawRecordsInTuples = processor.getDirectFlushRawSizeInTuples();

                processor.close();

                int newKeySizeInPages = directFlushRawRecordsInTuples == 0 ? runKeySizeInFrames : (int) Math
                        .ceil((double) directFlushKeysInTuples / directFlushRawRecordsInTuples * inputRunRawSizeInFrames
                                * ESTIMATOR_MAGNIFIER);

                ctx.getCounterContext().getCounter("optional.levels." + runLevel + ".estiInputKeySize", true)
                        .update(newKeySizeInPages);

                for (int i = 0; i < runs.size(); i++) {
                    IFrameReader recurRunReader = runs.get(i);

                    int newRunKeySize = (int) Math.ceil((double) newKeySizeInPages * runSizes.get(i)
                            / inputRunRawSizeInFrames);

                    if (checkFallback && newRunKeySize > HYBRID_FALLBACK_THRESHOLD * newKeySizeInPages) {
                        fallBack(recurRunReader, runSizes.get(i), newRunKeySize, runLevel);
                    } else {
                        processRunFiles(runs.get(i), newRunKeySize, runLevel + 1);
                    }

                }
            }

            private void fallBack(IFrameReader recurRunReader, int runSizeInPages, int runKeySizeInPages, int runLevel)
                    throws HyracksDataException {
                long currentIO = Long.valueOf(ctx.getIOManager().toString());
                fallbackHashSortAlgorithm(recurRunReader, runLevel + 1);
                ctx.getCounterContext().getCounter("optional.fallback.io", true)
                        .update(Long.valueOf(ctx.getIOManager().toString()) - currentIO);
            }

            private void fallbackHashSortAlgorithm(IFrameReader recurRunReader, int runLevel)
                    throws HyracksDataException {
                // fall back
                FrameTupleAccessor runFrameTupleAccessor = new FrameTupleAccessor(frameSize, inRecDesc);
                HybridHashSortGroupHashTable hhsTable = new HybridHashSortGroupHashTable(ctx, framesLimit, tableSize,
                        keyFields, comparators, tpcf.createPartitioner(runLevel + 1),
                        firstNormalizerFactory.createNormalizedKeyComputer(), aggregatorFactory.createAggregator(ctx,
                                inRecDesc, recordDescriptors[0], keyFields, storedKeyFields), inRecDesc,
                        recordDescriptors[0]);

                recurRunReader.open();
                while (recurRunReader.nextFrame(tempBuf)) {
                    runFrameTupleAccessor.reset(tempBuf);
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
                            comparators, recordDescriptors[0], tpcf.createPartitioner(runLevel + 1),
                            mergerFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                                    storedKeyFields, storedKeyFields), framesLimit, tableSize, writer, false);
                    hhsMerger.process();
                }
            }
        };
    }
}
