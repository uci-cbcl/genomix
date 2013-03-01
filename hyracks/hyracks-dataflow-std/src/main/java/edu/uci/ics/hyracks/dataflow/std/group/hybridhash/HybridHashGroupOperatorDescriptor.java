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

            int observedInputSizeInRawTuples;
            int observedInputSizeInFrames, maxRecursiveLevels;

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
            private int getNumberOfPartitions(int tableSize, int framesLimit, long inputKeySize, double factor) {

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
                observedInputSizeInRawTuples += buffer.getInt(buffer.capacity() - 4);
                observedInputSizeInFrames++;
                topProcessor.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                // estimate the maximum recursive levels
                maxRecursiveLevels = (int) Math.max(
                        Math.ceil(Math.log(observedInputSizeInFrames * fudgeFactor) / Math.log(framesLimit)) + 1, 1);

                finishAndRecursion(topProcessor, observedInputSizeInRawTuples, inputSizeInUniqueKeys, 0,
                        topLevelFallbackCheck);

                writer.close();

            }

            private void processRunFiles(IFrameReader runReader, int inputCardinality, int runLevel)
                    throws HyracksDataException {

                boolean checkFallback = true;

                int numOfPartitions = getNumberOfPartitions(tableSize, framesLimit, (long)inputCardinality
                        * userProvidedRecordSizeInBytes / frameSize, fudgeFactor);

                HybridHashGroupHashTable processor = new HybridHashGroupHashTable(ctx, framesLimit, tableSize,
                        numOfPartitions, keyFields, runLevel, comparators, tpcf, aggregatorFactory.createAggregator(
                                ctx, inRecDesc, recordDescriptors[0], keyFields, storedKeyFields), inRecDesc,
                        recordDescriptors[0], writer);

                processor.open();

                runReader.open();

                int inputRunRawSizeInTuples = 0;

                if (readAheadBuf == null) {
                    readAheadBuf = ctx.allocateFrame();
                }
                while (runReader.nextFrame(readAheadBuf)) {
                    inputRunRawSizeInTuples += readAheadBuf.getInt(readAheadBuf.capacity() - 4);
                    processor.nextFrame(readAheadBuf);
                }

                runReader.close();

                finishAndRecursion(processor, inputRunRawSizeInTuples, inputCardinality, runLevel, checkFallback);
            }

            /**
             * Finish the hash table processing and start recursive processing on run files.
             * 
             * @param ht
             * @param inputRawRecordCount
             * @param inputCardinality
             * @param level
             * @param checkFallback
             * @throws HyracksDataException
             */
            private void finishAndRecursion(HybridHashGroupHashTable ht, long inputRawRecordCount,
                    long inputCardinality, int level, boolean checkFallback) throws HyracksDataException {

                ht.finishup();

                List<IFrameReader> generatedRunReaders = ht.getSpilledRuns();
                List<Integer> partitionRawRecords = ht.getSpilledRunsSizeInRawTuples();

                int directFlushKeysInTuples = ht.getHashedUniqueKeys();
                int directFlushRawRecordsInTuples = ht.getHashedRawRecords();

                ht.close();
                ht = null;

                ctx.getCounterContext().getCounter("optional.levels." + level + ".estiInputKeyCardinality", true)
                        .update(inputCardinality);

                // do adjustment
                if (doInputAdjustment && directFlushRawRecordsInTuples > 0) {
                    inputCardinality = (int) Math.ceil((double) directFlushKeysInTuples / directFlushRawRecordsInTuples
                            * inputRawRecordCount);
                }

                ctx.getCounterContext()
                        .getCounter("optional.levels." + level + ".estiInputKeyCardinalityAdjusted", true)
                        .update(inputCardinality);

                IFrameReader recurRunReader;
                int subPartitionRawRecords;

                while (!generatedRunReaders.isEmpty()) {
                    recurRunReader = generatedRunReaders.remove(0);
                    subPartitionRawRecords = partitionRawRecords.remove(0);

                    int runKeyCardinality = (int) Math.ceil((double) inputCardinality * subPartitionRawRecords
                            / inputRawRecordCount);

                    if ((checkFallback && runKeyCardinality > HYBRID_FALLBACK_THRESHOLD * inputCardinality)
                            || level > maxRecursiveLevels) {
                        Logger.getLogger(HybridHashGroupOperatorDescriptor.class.getSimpleName()).warning(
                                "Hybrid-hash falls back to hash-sort algorithm! (" + level + ":" + maxRecursiveLevels
                                        + ")");
                        fallback(recurRunReader, level);
                    } else {
                        processRunFiles(recurRunReader, runKeyCardinality, level + 1);
                    }

                }
            }

            private void fallback(IFrameReader recurRunReader, int runLevel) throws HyracksDataException {
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
