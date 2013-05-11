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

package edu.uci.ics.genomix.hyracks.job;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.hyracks.data.accessors.KmerHashPartitioncomputerFactory;
import edu.uci.ics.genomix.hyracks.data.accessors.KmerNormarlizedComputerFactory;
import edu.uci.ics.genomix.hyracks.data.accessors.ReadIDNormarlizedComputeFactory;
import edu.uci.ics.genomix.hyracks.data.accessors.ReadIDPartitionComputerFactory;
import edu.uci.ics.genomix.hyracks.data.primitive.KmerPointable;
import edu.uci.ics.genomix.hyracks.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.genomix.hyracks.dataflow.KMerSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.dataflow.KMerTextWriterFactory;
import edu.uci.ics.genomix.hyracks.dataflow.MapKmerPositionToReadOperator;
import edu.uci.ics.genomix.hyracks.dataflow.NodeSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.dataflow.NodeTextWriterFactory;
import edu.uci.ics.genomix.hyracks.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.AggregateKmerAggregateFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.AggregateReadIDAggregateFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.MergeKmerAggregateFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.MergeReadIDAggregateFactory;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
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

    private int kmerSize;
    private int frameLimits;
    private int frameSize;
    private int tableSize;
    private GroupbyType groupbyType;
    private OutputFormat outputFormat;
    private boolean bGenerateReversedKmer;

    /** works for hybrid hashing */
    private long inputSizeInRawRecords;
    private long inputSizeInUniqueKeys;
    private int recordSizeInBytes;
    private int hashfuncStartLevel;
    private ExternalGroupOperatorDescriptor readLocalAggregator;
    private MToNPartitioningConnectorDescriptor readConnPartition;
    private ExternalGroupOperatorDescriptor readCrossAggregator;

    private void logDebug(String status) {
        LOG.debug(status + " nc nodes:" + ncNodeNames.length);
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
            IAggregatorDescriptorFactory aggeragater, IAggregatorDescriptorFactory merger,
            ITuplePartitionComputerFactory partition, INormalizedKeyComputerFactory normalizer,
            IPointableFactory pointable, RecordDescriptor outRed) {
        return new ExternalGroupOperatorDescriptor(jobSpec, keyFields, frameLimits,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) }, normalizer,
                aggeragater, merger, outRed, new HashSpillableTableFactory(new FieldHashPartitionComputerFactory(
                        keyFields,
                        new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(pointable) }),
                        tableSize), true);
    }

    private Object[] generateAggeragateDescriptorbyType(JobSpecification jobSpec,
            IAggregatorDescriptorFactory aggregator, IAggregatorDescriptorFactory merger,
            ITuplePartitionComputerFactory partition, INormalizedKeyComputerFactory normalizer,
            IPointableFactory pointable, RecordDescriptor outRed) throws HyracksDataException {
        int[] keyFields = new int[] { 0 }; // the id of grouped key
        Object[] obj = new Object[3];

        switch (groupbyType) {
            case EXTERNAL:
                obj[0] = newExternalGroupby(jobSpec, keyFields, aggregator, merger, partition, normalizer, pointable,
                        outRed);
                obj[1] = new MToNPartitioningConnectorDescriptor(jobSpec, partition);
                obj[2] = newExternalGroupby(jobSpec, keyFields, merger, merger, partition, normalizer, pointable,
                        outRed);
                break;
            case PRECLUSTER:
            default:
                obj[0] = newExternalGroupby(jobSpec, keyFields, aggregator, merger, partition, normalizer, pointable,
                        outRed);
                obj[1] = new MToNPartitioningMergingConnectorDescriptor(jobSpec, partition, keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) });
                obj[2] = new PreclusteredGroupOperatorDescriptor(jobSpec, keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) }, merger,
                        outRed);
                break;
        }
        return obj;
    }

    public HDFSReadOperatorDescriptor createHDFSReader(JobSpecification jobSpec, RecordDescriptor outRec)
            throws HyracksDataException {
        try {

            InputSplit[] splits = job.getInputFormat().getSplits(job, ncNodeNames.length);

            LOG.info("HDFS read into " + splits.length + " splits");
            String[] readSchedule = scheduler.getLocationConstraints(splits);
            return new HDFSReadOperatorDescriptor(jobSpec, outRec, job, splits, readSchedule,
                    new ReadsKeyValueParserFactory(kmerSize, bGenerateReversedKmer));
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private void connectOperators(JobSpecification jobSpec, IOperatorDescriptor preOp, String[] preNodes,
            IOperatorDescriptor nextOp, String[] nextNodes, IConnectorDescriptor conn) {
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, preOp, preNodes);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, nextOp, nextNodes);
        jobSpec.connect(conn, preOp, 0, nextOp, 0);
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {

        JobSpecification jobSpec = new JobSpecification();
        RecordDescriptor readKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null, null, null });
        RecordDescriptor combineKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null, null });
        jobSpec.setFrameSize(frameSize);

        // File input
        logDebug("ReadKmer Operator");
        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec, readKmerOutputRec);

        Object[] objs = generateAggeragateDescriptorbyType(jobSpec, new AggregateKmerAggregateFactory(),
                new MergeKmerAggregateFactory(), new KmerHashPartitioncomputerFactory(),
                new KmerNormarlizedComputerFactory(), KmerPointable.FACTORY, combineKmerOutputRec);
        AbstractOperatorDescriptor kmerLocalAggregator = (AbstractOperatorDescriptor) objs[0];
        logDebug("LocalKmerGroupby Operator");
        connectOperators(jobSpec, readOperator, ncNodeNames, kmerLocalAggregator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));

        logDebug("CrossKmerGroupby Operator");
        IConnectorDescriptor kmerConnPartition = (IConnectorDescriptor) objs[1];
        AbstractOperatorDescriptor kmerCrossAggregator = (AbstractOperatorDescriptor) objs[2];
        connectOperators(jobSpec, kmerLocalAggregator, ncNodeNames, kmerCrossAggregator, ncNodeNames, kmerConnPartition);

        logDebug("Map Kmer to Read Operator");
        //Map (Kmer, {(ReadID,PosInRead),...}) into (ReadID,PosInRead,{OtherPosition,...},Kmer) 
        RecordDescriptor readIDOutputRec = new RecordDescriptor(
                new ISerializerDeserializer[] { null, null, null, null });
        AbstractOperatorDescriptor mapKmerToRead = new MapKmerPositionToReadOperator(jobSpec, readIDOutputRec);
        connectOperators(jobSpec, kmerCrossAggregator, ncNodeNames, mapKmerToRead, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));

        logDebug("Group by Read Operator");
        // (ReadID, {(PosInRead,{OtherPositoin..},Kmer) ...} 
        RecordDescriptor nodeCombineRec = new RecordDescriptor(new ISerializerDeserializer[] { null, null });
        objs = generateAggeragateDescriptorbyType(jobSpec, new AggregateReadIDAggregateFactory(),
                new MergeReadIDAggregateFactory(), new ReadIDPartitionComputerFactory(),
                new ReadIDNormarlizedComputeFactory(), IntegerPointable.FACTORY, nodeCombineRec);
        AbstractOperatorDescriptor readLocalAggregator = (AbstractOperatorDescriptor) objs[0];
        connectOperators(jobSpec, mapKmerToRead, ncNodeNames, readLocalAggregator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));

        IConnectorDescriptor readconn = (IConnectorDescriptor) objs[1];
        AbstractOperatorDescriptor readCrossAggregator = (AbstractOperatorDescriptor) objs[2];
        connectOperators(jobSpec, readLocalAggregator, ncNodeNames, readCrossAggregator, ncNodeNames, readconn);

        // Output Kmer
        ITupleWriterFactory kmerWriter = null;
        ITupleWriterFactory nodeWriter = null;
        switch (outputFormat) {
            case TEXT:
                kmerWriter = new KMerTextWriterFactory(kmerSize);
                nodeWriter = new NodeTextWriterFactory();
                break;
            case BINARY:
            default:
                kmerWriter = new KMerSequenceWriterFactory(job);
                nodeWriter = new NodeSequenceWriterFactory(job);
                break;
        }
        logDebug("WriteOperator");
        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec, job, kmerWriter);
        connectOperators(jobSpec, kmerCrossAggregator, ncNodeNames, writeKmerOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        jobSpec.addRoot(writeKmerOperator);

        // Output Node
        HDFSWriteOperatorDescriptor writeNodeOperator = new HDFSWriteOperatorDescriptor(jobSpec, job, nodeWriter);
        connectOperators(jobSpec, readCrossAggregator, ncNodeNames, writeNodeOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        jobSpec.addRoot(writeNodeOperator);
        
        if (groupbyType == GroupbyType.PRECLUSTER) {
            jobSpec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
        }
        return jobSpec;
    }

    @Override
    protected void initJobConfiguration() {

        kmerSize = conf.getInt(GenomixJob.KMER_LENGTH, GenomixJob.DEFAULT_KMER);
        if (kmerSize % 2 == 0) {
            kmerSize--;
            conf.setInt(GenomixJob.KMER_LENGTH, kmerSize);
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
        LOG.info("Kmer:" + kmerSize);
        LOG.info("Groupby type:" + type);
        LOG.info("Output format:" + output);
        LOG.info("Frame limit" + frameLimits);
        LOG.info("Frame size" + frameSize);
    }

}
