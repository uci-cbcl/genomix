/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.genomix.hyracks.newgraph.job;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.genomix.hyracks.data.accessors.KmerHashPartitioncomputerFactory;
import edu.uci.ics.genomix.hyracks.data.accessors.KmerNormarlizedComputerFactory;
import edu.uci.ics.genomix.hyracks.data.accessors.ReadIDPartitionComputerFactory;
import edu.uci.ics.genomix.hyracks.data.primitive.KmerPointable;
import edu.uci.ics.genomix.hyracks.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.genomix.hyracks.dataflow.MapKmerPositionToReadOperator;
import edu.uci.ics.genomix.hyracks.dataflow.MapReadToNodeOperator;
import edu.uci.ics.genomix.hyracks.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.AggregateKmerAggregateFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.AggregateReadIDAggregateFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.MergeKmerAggregateFactory;
import edu.uci.ics.genomix.hyracks.dataflow.aggregators.MergeReadIDAggregateFactory;
import edu.uci.ics.genomix.hyracks.dataflow.io.KMerSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.dataflow.io.KMerTextWriterFactory;
import edu.uci.ics.genomix.hyracks.dataflow.io.NodeSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.dataflow.io.NodeTextWriterFactory;
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
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

@SuppressWarnings("deprecation")
public class JobGenBrujinGraph extends JobGen {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public enum GroupbyType {
        EXTERNAL,
        PRECLUSTER,
        HYBRIDHASH,
    }

    public enum OutputFormat {
        TEXT,
        BINARY,
    }

    protected ConfFactory hadoopJobConfFactory;
    protected static final Log LOG = LogFactory.getLog(JobGenBrujinGraph.class);
    protected String[] ncNodeNames;
    protected String[] readSchedule;

    protected int readLength;
    protected int kmerSize;
    protected int frameLimits;
    protected int frameSize;
    protected int tableSize;
    protected GroupbyType groupbyType;
    protected OutputFormat outputFormat;
    protected boolean bGenerateReversedKmer;

    protected void logDebug(String status) {
        LOG.debug(status + " nc nodes:" + ncNodeNames.length);
    }

    public JobGenBrujinGraph(GenomixJobConf job, Scheduler scheduler, final Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job);
        String[] nodes = new String[ncMap.size()];
        ncMap.keySet().toArray(nodes);
        ncNodeNames = new String[nodes.length * numPartitionPerMachine];
        for (int i = 0; i < numPartitionPerMachine; i++) {
            System.arraycopy(nodes, 0, ncNodeNames, i * nodes.length, nodes.length);
        }
        initJobConfiguration(scheduler);
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

    private Object[] generateAggeragateDescriptorbyType(JobSpecification jobSpec, int[] keyFields,
            IAggregatorDescriptorFactory aggregator, IAggregatorDescriptorFactory merger,
            ITuplePartitionComputerFactory partition, INormalizedKeyComputerFactory normalizer,
            IPointableFactory pointable, RecordDescriptor combineRed, RecordDescriptor finalRec)
            throws HyracksDataException {

        Object[] obj = new Object[3];

        switch (groupbyType) {
            case EXTERNAL:
                obj[0] = newExternalGroupby(jobSpec, keyFields, aggregator, merger, partition, normalizer, pointable,
                        combineRed);
                obj[1] = new MToNPartitioningConnectorDescriptor(jobSpec, partition);
                obj[2] = newExternalGroupby(jobSpec, keyFields, merger, merger, partition, normalizer, pointable,
                        finalRec);
                break;
            case PRECLUSTER:
            default:

                obj[0] = new PreclusteredGroupOperatorDescriptor(jobSpec, keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) }, aggregator,
                        combineRed);
                obj[1] = new MToNPartitioningMergingConnectorDescriptor(jobSpec, partition, keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) });
                obj[2] = new PreclusteredGroupOperatorDescriptor(jobSpec, keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) }, merger,
                        finalRec);
                jobSpec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
                break;
        }
        return obj;
    }

    public HDFSReadOperatorDescriptor createHDFSReader(JobSpecification jobSpec) throws HyracksDataException {
        try {
            InputSplit[] splits = hadoopJobConfFactory.getConf().getInputFormat()
                    .getSplits(hadoopJobConfFactory.getConf(), ncNodeNames.length);

            return new HDFSReadOperatorDescriptor(jobSpec, ReadsKeyValueParserFactory.readKmerOutputRec,
                    hadoopJobConfFactory.getConf(), splits, readSchedule, new ReadsKeyValueParserFactory(readLength,
                            kmerSize, bGenerateReversedKmer));
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    public static void connectOperators(JobSpecification jobSpec, IOperatorDescriptor preOp, String[] preNodes,
            IOperatorDescriptor nextOp, String[] nextNodes, IConnectorDescriptor conn) {
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, preOp, preNodes);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, nextOp, nextNodes);
        jobSpec.connect(conn, preOp, 0, nextOp, 0);
    }

    public AbstractOperatorDescriptor generateGroupbyKmerJob(JobSpecification jobSpec,
            AbstractOperatorDescriptor readOperator) throws HyracksDataException {
        int[] keyFields = new int[] { 0 }; // the id of grouped key

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(jobSpec, frameLimits, keyFields,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(KmerPointable.FACTORY) },
                ReadsKeyValueParserFactory.readKmerOutputRec);
        connectOperators(jobSpec, readOperator, ncNodeNames, sorter, ncNodeNames, new OneToOneConnectorDescriptor(
                jobSpec));

        RecordDescriptor combineKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { null, null });
        jobSpec.setFrameSize(frameSize);

        Object[] objs = generateAggeragateDescriptorbyType(jobSpec, keyFields, new AggregateKmerAggregateFactory(),
                new MergeKmerAggregateFactory(), new KmerHashPartitioncomputerFactory(),
                new KmerNormarlizedComputerFactory(), KmerPointable.FACTORY, combineKmerOutputRec, combineKmerOutputRec);
        AbstractOperatorDescriptor kmerLocalAggregator = (AbstractOperatorDescriptor) objs[0];
        logDebug("LocalKmerGroupby Operator");
        connectOperators(jobSpec, sorter, ncNodeNames, kmerLocalAggregator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));

        logDebug("CrossKmerGroupby Operator");
        IConnectorDescriptor kmerConnPartition = (IConnectorDescriptor) objs[1];
        AbstractOperatorDescriptor kmerCrossAggregator = (AbstractOperatorDescriptor) objs[2];
        connectOperators(jobSpec, kmerLocalAggregator, ncNodeNames, kmerCrossAggregator, ncNodeNames, kmerConnPartition);
        return kmerCrossAggregator;
    }


    public AbstractOperatorDescriptor generateKmerWritorOperator(JobSpecification jobSpec,
            AbstractOperatorDescriptor kmerCrossAggregator) throws HyracksException {
        // Output Kmer
        ITupleWriterFactory kmerWriter = null;
        switch (outputFormat) {
            case TEXT:
                kmerWriter = new KMerTextWriterFactory(kmerSize);
                break;
            case BINARY:
            default:
                kmerWriter = new KMerSequenceWriterFactory(hadoopJobConfFactory.getConf());
                break;
        }
        logDebug("WriteOperator");
        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec,
                hadoopJobConfFactory.getConf(), kmerWriter);
        connectOperators(jobSpec, kmerCrossAggregator, ncNodeNames, writeKmerOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        return writeKmerOperator;
    }

    public AbstractOperatorDescriptor generateNodeWriterOpertator(JobSpecification jobSpec,
            AbstractOperatorDescriptor mapEachReadToNode) throws HyracksException {
        ITupleWriterFactory nodeWriter = null;
        switch (outputFormat) {
            case TEXT:
                nodeWriter = new NodeTextWriterFactory(kmerSize);
                break;
            case BINARY:
            default:
                nodeWriter = new NodeSequenceWriterFactory(hadoopJobConfFactory.getConf());
                break;
        }
        logDebug("WriteOperator");
        // Output Node
        HDFSWriteOperatorDescriptor writeNodeOperator = new HDFSWriteOperatorDescriptor(jobSpec,
                hadoopJobConfFactory.getConf(), nodeWriter);
        connectOperators(jobSpec, mapEachReadToNode, ncNodeNames, writeNodeOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        return writeNodeOperator;
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {

        JobSpecification jobSpec = new JobSpecification();
        logDebug("ReadKmer Operator");

        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);

        logDebug("Group by Kmer");
        AbstractOperatorDescriptor lastOperator = generateGroupbyKmerJob(jobSpec, readOperator);

        // logDebug("Write kmer to result");
        // generateRootByWriteKmerGroupbyResult(jobSpec, lastOperator);

        logDebug("Write node to result");
        lastOperator = generateNodeWriterOpertator(jobSpec, lastOperator);

        jobSpec.addRoot(lastOperator);
        return jobSpec;
    }

    protected void initJobConfiguration(Scheduler scheduler) throws HyracksDataException {
        Configuration conf = confFactory.getConf();
        readLength = conf.getInt(GenomixJobConf.READ_LENGTH, GenomixJobConf.DEFAULT_READLEN);
        kmerSize = conf.getInt(GenomixJobConf.KMER_LENGTH, GenomixJobConf.DEFAULT_KMERLEN);
        if (kmerSize % 2 == 0) {
            kmerSize--;
            conf.setInt(GenomixJobConf.KMER_LENGTH, kmerSize);
        }
        frameLimits = conf.getInt(GenomixJobConf.FRAME_LIMIT, GenomixJobConf.DEFAULT_FRAME_LIMIT);
        tableSize = conf.getInt(GenomixJobConf.TABLE_SIZE, GenomixJobConf.DEFAULT_TABLE_SIZE);
        frameSize = conf.getInt(GenomixJobConf.FRAME_SIZE, GenomixJobConf.DEFAULT_FRAME_SIZE);

        bGenerateReversedKmer = conf.getBoolean(GenomixJobConf.REVERSED_KMER, GenomixJobConf.DEFAULT_REVERSED);

        String type = conf.get(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        if (type.equalsIgnoreCase(GenomixJobConf.GROUPBY_TYPE_EXTERNAL)) {
            groupbyType = GroupbyType.EXTERNAL;
        } else if (type.equalsIgnoreCase(GenomixJobConf.GROUPBY_TYPE_PRECLUSTER)) {
            groupbyType = GroupbyType.PRECLUSTER;
        } else {
            groupbyType = GroupbyType.HYBRIDHASH;
        }

        String output = conf.get(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        if (output.equalsIgnoreCase("text")) {
            outputFormat = OutputFormat.TEXT;
        } else {
            outputFormat = OutputFormat.BINARY;
        }
        try {
            hadoopJobConfFactory = new ConfFactory(new JobConf(conf));
            InputSplit[] splits = hadoopJobConfFactory.getConf().getInputFormat()
                    .getSplits(hadoopJobConfFactory.getConf(), ncNodeNames.length);
            readSchedule = scheduler.getLocationConstraints(splits);
        } catch (IOException ex) {
            throw new HyracksDataException(ex);
        }

        LOG.info("Genomix Graph Build Configuration");
        LOG.info("Kmer:" + kmerSize);
        LOG.info("Groupby type:" + type);
        LOG.info("Output format:" + output);
        LOG.info("Frame limit" + frameLimits);
        LOG.info("Frame kmerByteSize" + frameSize);
    }

}
