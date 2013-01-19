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
package edu.uci.ics.hyracks.dataflow.std.group.hybridhash;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

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
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFamily;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortGroupHashTable;
import edu.uci.ics.hyracks.dataflow.std.group.hashsort.HybridHashSortRunMerger;

public class HybridHashGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private static final double HYBRID_FALLBACK_THRESHOLD = 0.8;

    // merge with fudge factor
    private static final double ESTIMATOR_MAGNIFIER = 1.2;

    // input key fields
    private final int[] keyFields;

    // intermediate and final key fields
    private final int[] storedKeyFields;

    /**
     * Input sizes as the count of the raw records.
     */
    private final long inputSizeInRawRecords;

    /**
     * Input size as the count of the unique keys.
     */
    private final long inputSizeInUniqueKeys;

    // hash table size
    private final int tableSize;

    // estimated record size: used for compute the fudge factor
    private final int userProvidedRecordSizeInBytes;

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
     * Flag for input adjustment
     */
    private final boolean doInputAdjustment;

    private final static double FUDGE_FACTOR_ESTIMATION = 1.2;

    public HybridHashGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            long inputSizeInRawRecords, long inputSizeInUniqueKeys, int recordSizeInBytes, int tableSize,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFamily[] hashFamilies,
            int hashFuncStartLevel, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor outRecDesc) throws HyracksDataException {
        this(spec, keyFields, framesLimit, inputSizeInRawRecords, inputSizeInUniqueKeys, recordSizeInBytes, tableSize,
                comparatorFactories, hashFamilies, hashFuncStartLevel, firstNormalizerFactory, aggregatorFactory,
                mergerFactory, outRecDesc, true);
    }

    public HybridHashGroupOperatorDescriptor(JobSpecification spec, int[] keyFields, int framesLimit,
            long inputSizeInRawRecords, long inputSizeInUniqueKeys, int recordSizeInBytes, int tableSize,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFamily[] hashFamilies,
            int hashFuncStartLevel, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory mergerFactory,
            RecordDescriptor outRecDesc, boolean doInputAdjustment) throws HyracksDataException {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        this.userProvidedRecordSizeInBytes = recordSizeInBytes;

        this.inputSizeInRawRecords = inputSizeInRawRecords;
        this.inputSizeInUniqueKeys = inputSizeInUniqueKeys;

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

        this.hashFamilies = hashFamilies;

        recordDescriptors[0] = outRecDesc;

        this.doInputAdjustment = doInputAdjustment;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

        final int frameSize = ctx.getFrameSize();

        final double fudgeFactor = HybridHashGroupHashTable.getHashtableOverheadRatio(tableSize, frameSize,
                framesLimit, userProvidedRecordSizeInBytes) * FUDGE_FACTOR_ESTIMATION;

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            HybridHashGroupHashTable topProcessor;

            int observedInputSizeInFrames;

            int userProvidedInputSizeInFrames;

            boolean topLevelFallbackCheck = true;

            ITuplePartitionComputerFamily tpcf = new FieldHashPartitionComputerFamily(keyFields, hashFamilies);

            ITuplePartitionComputerFamily tpcfMerge = new FieldHashPartitionComputerFamily(storedKeyFields,
                    hashFamilies);

            ByteBuffer readAheadBuf;

            /**
             * Compute the partition numbers using hybrid-hash formula.
             * 
             * @param tableSize
             * @param framesLimit
             * @param inputKeySize
             * @param partitionInOperator
             * @param factor
             * @return
             */
            private int getNumberOfPartitions(int tableSize, int framesLimit, int inputKeySize, double factor) {

                int hashtableHeaderPages = HybridHashGroupHashTable.getHeaderPages(tableSize, frameSize);

                int numberOfPartitions = HybridHashUtil.hybridHashPartitionComputer((int) Math.ceil(inputKeySize),
                        framesLimit, factor);

                // if the partition number is more than the available hash table contents, do pure partition.
                if (numberOfPartitions >= framesLimit - hashtableHeaderPages) {
                    numberOfPartitions = framesLimit;
                }

                if (numberOfPartitions <= 0) {
                    numberOfPartitions = 1;
                }

                return numberOfPartitions;
            }

            @Override
            public void open() throws HyracksDataException {

                observedInputSizeInFrames = 0;

                // estimate the number of unique keys for this partition, given the total raw record count and unique record count
                long estimatedNumberOfUniqueKeys = HybridHashUtil.getEstimatedPartitionSizeOfUniqueKeys(
                        inputSizeInRawRecords, inputSizeInUniqueKeys, 1);

                userProvidedInputSizeInFrames = (int) Math.ceil(estimatedNumberOfUniqueKeys
                        * userProvidedRecordSizeInBytes / frameSize);

                int topPartitions = getNumberOfPartitions(tableSize, framesLimit,
                        (int) Math.ceil(userProvidedInputSizeInFrames * ESTIMATOR_MAGNIFIER), fudgeFactor);

                topProcessor = new HybridHashGroupHashTable(ctx, framesLimit, tableSize, topPartitions, keyFields, 0,
                        comparators, tpcf, aggregatorFactory.createAggregator(ctx, inRecDesc, recordDescriptors[0],
                                keyFields, storedKeyFields), inRecDesc, recordDescriptors[0], writer);

                writer.open();
                topProcessor.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                observedInputSizeInFrames++;
                topProcessor.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                topProcessor.finishup();

                List<IFrameReader> runs = topProcessor.getSpilledRuns();
                List<Integer> runsSizeInFrames = topProcessor.getSpilledRunsSizeInPages();

                // get statistics from the hash table
                int hashedKeys = topProcessor.getHashedUniqueKeys();
                int hashedRawRecords = topProcessor.getHashedRawRecords();

                // Get the raw record size of each partition (in records but not in pages)
                List<Integer> partitionRawRecordsCount = topProcessor.getSpilledRunsSizeInTuples();

                topProcessor.close();

                // get a new estimation on the number of keys in the input data set: if the previous level is pure-partition, 
                // then use the size inputed in the previous level; otherwise, compute the key ratio in the data set based on
                // the processed keys.
                int newKeySizeInPages = (doInputAdjustment && hashedRawRecords > 0) ? (int) Math
                        .ceil((double) hashedKeys / hashedRawRecords * observedInputSizeInFrames) : (int) Math
                        .ceil(userProvidedInputSizeInFrames);

                IFrameReader runReader;
                int runSizeInFrames;
                int partitionRawRecords;

                while (!runs.isEmpty()) {

                    runReader = runs.remove(0);
                    runSizeInFrames = runsSizeInFrames.remove(0);
                    partitionRawRecords = partitionRawRecordsCount.remove(0);

                    // compute the estimated key size in frames for the run file
                    int runKeySize;

                    if (doInputAdjustment && hashedRawRecords > 0)
                        runKeySize = (int) Math.ceil((double) newKeySizeInPages * runSizeInFrames
                                / observedInputSizeInFrames);
                    else
                        runKeySize = (int) Math.ceil((double) userProvidedInputSizeInFrames * partitionRawRecords
                                / inputSizeInRawRecords);

                    if (topLevelFallbackCheck && runKeySize > HYBRID_FALLBACK_THRESHOLD * newKeySizeInPages) {
                        fallBack(runReader, runSizeInFrames, runKeySize, 1);
                    } else {
                        processRunFiles(runReader, runKeySize, 1);
                    }
                }

                writer.close();

            }

            private void processRunFiles(IFrameReader runReader, int uniqueKeysOfRunFileInFrames, int runLevel)
                    throws HyracksDataException {

                boolean checkFallback = true;

                int numOfPartitions = getNumberOfPartitions(tableSize, framesLimit, uniqueKeysOfRunFileInFrames,
                        fudgeFactor);

                HybridHashGroupHashTable processor = new HybridHashGroupHashTable(ctx, framesLimit, tableSize,
                        numOfPartitions, keyFields, runLevel, comparators, tpcf, aggregatorFactory.createAggregator(
                                ctx, inRecDesc, recordDescriptors[0], keyFields, storedKeyFields), inRecDesc,
                        recordDescriptors[0], writer);

                processor.open();

                runReader.open();

                int inputRunRawSizeInFrames = 0, inputRunRawSizeInTuples = 0;

                if (readAheadBuf == null) {
                    readAheadBuf = ctx.allocateFrame();
                }
                while (runReader.nextFrame(readAheadBuf)) {
                    inputRunRawSizeInFrames++;
                    inputRunRawSizeInTuples += readAheadBuf.getInt(readAheadBuf.capacity() - 4);
                    processor.nextFrame(readAheadBuf);
                }

                runReader.close();

                processor.finishup();

                List<IFrameReader> runs = processor.getSpilledRuns();
                List<Integer> runSizes = processor.getSpilledRunsSizeInPages();
                List<Integer> partitionRawRecords = processor.getSpilledRunsSizeInTuples();

                int directFlushKeysInTuples = processor.getHashedUniqueKeys();
                int directFlushRawRecordsInTuples = processor.getHashedRawRecords();

                processor.close();

                int newKeySizeInPages = (doInputAdjustment && directFlushRawRecordsInTuples > 0) ? (int) Math
                        .ceil((double) directFlushKeysInTuples / directFlushRawRecordsInTuples
                                * inputRunRawSizeInFrames) : uniqueKeysOfRunFileInFrames;

                IFrameReader recurRunReader;
                int runSizeInPages, subPartitionRawRecords;

                while (!runs.isEmpty()) {
                    recurRunReader = runs.remove(0);
                    runSizeInPages = runSizes.remove(0);
                    subPartitionRawRecords = partitionRawRecords.remove(0);

                    int newRunKeySize;

                    if (doInputAdjustment && directFlushRawRecordsInTuples > 0) {
                        // do adjustment
                        newRunKeySize = (int) Math.ceil((double) newKeySizeInPages * runSizeInPages
                                / inputRunRawSizeInFrames);
                    } else {
                        // no adjustment
                        newRunKeySize = (int) Math.ceil((double) subPartitionRawRecords * uniqueKeysOfRunFileInFrames
                                / inputRunRawSizeInTuples);
                    }

                    if (checkFallback && newRunKeySize > HYBRID_FALLBACK_THRESHOLD * newKeySizeInPages) {
                        fallBack(recurRunReader, runSizeInPages, newRunKeySize, runLevel);
                    } else {
                        processRunFiles(recurRunReader, newRunKeySize, runLevel + 1);
                    }

                }
            }

            private void fallBack(IFrameReader recurRunReader, int runSizeInPages, int runKeySizeInPages, int runLevel)
                    throws HyracksDataException {
                fallbackHashSortAlgorithm(recurRunReader, runLevel + 1);
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
                if (readAheadBuf == null) {
                    readAheadBuf = ctx.allocateFrame();
                }
                while (recurRunReader.nextFrame(readAheadBuf)) {
                    runFrameTupleAccessor.reset(readAheadBuf);
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
                            comparators, recordDescriptors[0], tpcfMerge.createPartitioner(runLevel + 1),
                            mergerFactory.createAggregator(ctx, recordDescriptors[0], recordDescriptors[0],
                                    storedKeyFields, storedKeyFields), framesLimit, tableSize, writer, false);
                    hhsMerger.process();
                }
            }

        };
    }
}
