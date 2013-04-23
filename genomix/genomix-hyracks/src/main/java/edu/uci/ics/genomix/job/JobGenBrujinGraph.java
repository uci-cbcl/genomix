/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.genomix.job;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.data.std.accessors.ByteSerializerDeserializer;
import edu.uci.ics.genomix.data.std.accessors.KmerBinaryHashFunctionFamily;
import edu.uci.ics.genomix.data.std.accessors.KmerHashPartitioncomputerFactory;
import edu.uci.ics.genomix.data.std.accessors.KmerNormarlizedComputerFactory;
import edu.uci.ics.genomix.data.std.primitive.KmerPointable;
import edu.uci.ics.genomix.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.genomix.dataflow.KMerSequenceWriterFactory;
import edu.uci.ics.genomix.dataflow.KMerTextWriterFactory;
import edu.uci.ics.genomix.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.dataflow.aggregators.DistributedMergeLmerAggregateFactory;
import edu.uci.ics.genomix.dataflow.aggregators.MergeKmerAggregateFactory;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.hybridhash.HybridHashGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenBrujinGraph extends JobGen {
    public enum GroupbyType {
        EXTERNAL,
        PRECLUSTER,
        HYBRIDHASH,
    }

    public enum OutputFormat {
        TEXT,
        BINARY,
    }

    JobConf job;
    private static final Log LOG = LogFactory.getLog(JobGenBrujinGraph.class);
    private Scheduler scheduler;
    private String[] ncNodeNames;

    private int kmers;
    private int frameLimits;
    private int frameSize;
    private int tableSize;
    private GroupbyType groupbyType;
    private OutputFormat outputFormat;
    private boolean bGenerateReversedKmer;

    private AbstractOperatorDescriptor singleGrouper;
    private IConnectorDescriptor connPartition;
    private AbstractOperatorDescriptor crossGrouper;
    private RecordDescriptor readOutputRec;
    private RecordDescriptor combineOutputRec;

    /** works for hybrid hashing */
    private long inputSizeInRawRecords;
    private long inputSizeInUniqueKeys;
    private int recordSizeInBytes;
    private int hashfuncStartLevel;

    private void logDebug(String status) {
        String names = "";
        for (String str : ncNodeNames) {
            names += str + " ";
        }
        LOG.info(status + " nc nodes:" + ncNodeNames.length + " " + names);
    }

    public JobGenBrujinGraph(GenomixJob job, Scheduler scheduler, final Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) {
        super(job);
        this.scheduler = scheduler;
        String[] nodes = new String[ncMap.size()];
        ncMap.keySet().toArray(nodes);
        ncNodeNames = new String[nodes.length * numPartitionPerMachine];
        for (int i = 0; i < numPartitionPerMachine; i++) {
            System.arraycopy(nodes, 0, ncNodeNames, i * nodes.length, nodes.length);
        }
        logDebug("initialize");
    }

    private ExternalGroupOperatorDescriptor newExternalGroupby(JobSpecification jobSpec, int[] keyFields,
            IAggregatorDescriptorFactory aggeragater) {
        return new ExternalGroupOperatorDescriptor(jobSpec, keyFields, frameLimits,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(KmerPointable.FACTORY) },
                new KmerNormarlizedComputerFactory(), aggeragater, new DistributedMergeLmerAggregateFactory(),
                combineOutputRec, new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(keyFields,
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(KmerPointable.FACTORY) }), tableSize), true);
    }

    private HybridHashGroupOperatorDescriptor newHybridGroupby(JobSpecification jobSpec, int[] keyFields,
            long inputSizeInRawRecords, long inputSizeInUniqueKeys, int recordSizeInBytes, int hashfuncStartLevel,
            IAggregatorDescriptorFactory aggeragater) throws HyracksDataException {
        return new HybridHashGroupOperatorDescriptor(jobSpec, keyFields, frameLimits, inputSizeInRawRecords,
                inputSizeInUniqueKeys, recordSizeInBytes, tableSize,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(KmerPointable.FACTORY) },
                new IBinaryHashFunctionFamily[] { new KmerBinaryHashFunctionFamily() }, hashfuncStartLevel,
                new KmerNormarlizedComputerFactory(), aggeragater, new DistributedMergeLmerAggregateFactory(),
                combineOutputRec, true);
    }

    private void generateDescriptorbyType(JobSpecification jobSpec) throws HyracksDataException {
        int[] keyFields = new int[] { 0 }; // the id of grouped key

        switch (groupbyType) {
            case EXTERNAL:
                singleGrouper = newExternalGroupby(jobSpec, keyFields, new MergeKmerAggregateFactory());
                connPartition = new MToNPartitioningConnectorDescriptor(jobSpec, new KmerHashPartitioncomputerFactory());
                crossGrouper = newExternalGroupby(jobSpec, keyFields, new DistributedMergeLmerAggregateFactory());
                break;
            case PRECLUSTER:
                singleGrouper = newExternalGroupby(jobSpec, keyFields, new MergeKmerAggregateFactory());
                connPartition = new MToNPartitioningMergingConnectorDescriptor(jobSpec,
                        new KmerHashPartitioncomputerFactory(), keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(KmerPointable.FACTORY) });
                crossGrouper = new PreclusteredGroupOperatorDescriptor(jobSpec, keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(KmerPointable.FACTORY) },
                        new DistributedMergeLmerAggregateFactory(), combineOutputRec);
                break;
            case HYBRIDHASH:
            default:
                singleGrouper = newHybridGroupby(jobSpec, keyFields, inputSizeInRawRecords, inputSizeInUniqueKeys,
                        recordSizeInBytes, hashfuncStartLevel, new MergeKmerAggregateFactory());
                connPartition = new MToNPartitioningConnectorDescriptor(jobSpec, new KmerHashPartitioncomputerFactory());

                crossGrouper = newHybridGroupby(jobSpec, keyFields, inputSizeInRawRecords, inputSizeInUniqueKeys,
                        recordSizeInBytes, hashfuncStartLevel, new DistributedMergeLmerAggregateFactory());
                break;
        }
    }

    public HDFSReadOperatorDescriptor createHDFSReader(JobSpecification jobSpec) throws HyracksDataException {
        try {

            InputSplit[] splits = job.getInputFormat().getSplits(job, ncNodeNames.length);

            LOG.info("HDFS read into " + splits.length + " splits");
            String[] readSchedule = scheduler.getLocationConstraints(splits);
            String log = "";
            for (String schedule : readSchedule) {
                log += schedule + " ";
            }
            LOG.info("HDFS read schedule " + log);
            return new HDFSReadOperatorDescriptor(jobSpec, readOutputRec, job, splits, readSchedule,
                    new ReadsKeyValueParserFactory(kmers, bGenerateReversedKmer));
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {

        JobSpecification jobSpec = new JobSpecification();
        readOutputRec = new RecordDescriptor(
                new ISerializerDeserializer[] { null, ByteSerializerDeserializer.INSTANCE });
        combineOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null,
                ByteSerializerDeserializer.INSTANCE, ByteSerializerDeserializer.INSTANCE });
        jobSpec.setFrameSize(frameSize);

        // File input
        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);

        logDebug("Read Operator");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, readOperator, ncNodeNames);

        generateDescriptorbyType(jobSpec);
        logDebug("SingleGroupby Operator");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, singleGrouper, ncNodeNames);

        IConnectorDescriptor readfileConn = new OneToOneConnectorDescriptor(jobSpec);
        jobSpec.connect(readfileConn, readOperator, 0, singleGrouper, 0);

        logDebug("CrossGrouper Operator");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, crossGrouper, ncNodeNames);
        jobSpec.connect(connPartition, singleGrouper, 0, crossGrouper, 0);

        // Output
        ITupleWriterFactory writer = null;
        switch (outputFormat) {
            case TEXT:
                writer = new KMerTextWriterFactory(kmers);
                break;
            case BINARY:
            default:
                writer = new KMerSequenceWriterFactory(job);
                break;
        }
        HDFSWriteOperatorDescriptor writeOperator = new HDFSWriteOperatorDescriptor(jobSpec, job, writer);

        logDebug("WriteOperator");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, writeOperator, ncNodeNames);

        IConnectorDescriptor printConn = new OneToOneConnectorDescriptor(jobSpec);
        jobSpec.connect(printConn, crossGrouper, 0, writeOperator, 0);
        jobSpec.addRoot(writeOperator);

        if (groupbyType == GroupbyType.PRECLUSTER) {
            jobSpec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        }
        return jobSpec;
    }

    @Override
    protected void initJobConfiguration() {

        kmers = conf.getInt(GenomixJob.KMER_LENGTH, GenomixJob.DEFAULT_KMER);
        if (kmers % 2 == 0) {
            kmers--;
            conf.setInt(GenomixJob.KMER_LENGTH, kmers);
        }
        frameLimits = conf.getInt(GenomixJob.FRAME_LIMIT, GenomixJob.DEFAULT_FRAME_LIMIT);
        tableSize = conf.getInt(GenomixJob.TABLE_SIZE, GenomixJob.DEFAULT_TABLE_SIZE);
        frameSize = conf.getInt(GenomixJob.FRAME_SIZE, GenomixJob.DEFAULT_FRAME_SIZE);
        inputSizeInRawRecords = conf.getLong(GenomixJob.GROUPBY_HYBRID_INPUTSIZE,
                GenomixJob.DEFAULT_GROUPBY_HYBRID_INPUTSIZE);
        inputSizeInUniqueKeys = conf.getLong(GenomixJob.GROUPBY_HYBRID_INPUTKEYS,
                GenomixJob.DEFAULT_GROUPBY_HYBRID_INPUTKEYS);
        recordSizeInBytes = conf.getInt(GenomixJob.GROUPBY_HYBRID_RECORDSIZE_SINGLE,
                GenomixJob.DEFAULT_GROUPBY_HYBRID_RECORDSIZE_SINGLE);
        hashfuncStartLevel = conf.getInt(GenomixJob.GROUPBY_HYBRID_HASHLEVEL,
                GenomixJob.DEFAULT_GROUPBY_HYBRID_HASHLEVEL);
        /** here read the different recordSize why ? */
        recordSizeInBytes = conf.getInt(GenomixJob.GROUPBY_HYBRID_RECORDSIZE_CROSS,
                GenomixJob.DEFAULT_GROUPBY_HYBRID_RECORDSIZE_CROSS);

        bGenerateReversedKmer = conf.getBoolean(GenomixJob.REVERSED_KMER, GenomixJob.DEFAULT_REVERSED);

        String type = conf.get(GenomixJob.GROUPBY_TYPE, GenomixJob.DEFAULT_GROUPBY_TYPE);
        if (type.equalsIgnoreCase("external")) {
            groupbyType = GroupbyType.EXTERNAL;
        } else if (type.equalsIgnoreCase("precluster")) {
            groupbyType = GroupbyType.PRECLUSTER;
        } else {
            groupbyType = GroupbyType.HYBRIDHASH;
        }

        String output = conf.get(GenomixJob.OUTPUT_FORMAT, GenomixJob.DEFAULT_OUTPUT_FORMAT);
        if (output.equalsIgnoreCase("text")) {
            outputFormat = OutputFormat.TEXT;
        } else {
            outputFormat = OutputFormat.BINARY;
        }
        job = new JobConf(conf);
        LOG.info("Genomix Graph Build Configuration");
        LOG.info("Kmer:" + kmers);
        LOG.info("Groupby type:" + type);
        LOG.info("Output format:" + output);
        LOG.info("Frame limit" + frameLimits);
        LOG.info("Frame size" + frameSize);
    }

}