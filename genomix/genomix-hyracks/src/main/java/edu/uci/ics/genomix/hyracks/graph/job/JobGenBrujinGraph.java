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

package edu.uci.ics.genomix.hyracks.graph.job;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.data.accessors.KmerHashPartitioncomputerFactory;
import edu.uci.ics.genomix.hyracks.data.accessors.KmerNormarlizedComputerFactory;
import edu.uci.ics.genomix.hyracks.data.primitive.KmerPointable;
import edu.uci.ics.genomix.hyracks.graph.dataflow.AssembleKeyIntoNodeOperator;
import edu.uci.ics.genomix.hyracks.graph.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.genomix.hyracks.graph.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.hyracks.graph.dataflow.aggregators.AggregateKmerAggregateFactory;
import edu.uci.ics.genomix.hyracks.graph.dataflow.aggregators.MergeKmerAggregateFactory;
import edu.uci.ics.genomix.hyracks.graph.io.NodeSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.graph.io.NodeTextWriterFactory;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.api.IPointableFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.preclustered.PreclusteredGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.ConfFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

@SuppressWarnings("deprecation")
public class JobGenBrujinGraph extends JobGen {

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
    private static final Logger LOG = Logger.getLogger(JobGenBrujinGraph.class.getName());
    public static final int DEFAULT_FRAME_LIMIT = 4096;
    public static final int DEFAULT_FRAME_SIZE = 65535;
    protected String[] ncNodeNames;
    protected String[] readSchedule;

    protected int kmerSize;
    protected int frameLimits;
    protected int frameSize;
    protected int tableSize;
    protected GroupbyType groupbyType;
    protected OutputFormat outputFormat;

    protected void logDebug(String status) {
        LOG.fine(status + " nc nodes:" + ncNodeNames.length);
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

    private Object[] generateAggeragateDescriptorbyType(JobSpecification jobSpec, int[] keyFields,
            IAggregatorDescriptorFactory aggregator, IAggregatorDescriptorFactory merger,
            ITuplePartitionComputerFactory partition, INormalizedKeyComputerFactory normalizer,
            IPointableFactory pointable, RecordDescriptor combineRed, RecordDescriptor finalRec)
            throws HyracksDataException {

        Object[] obj = new Object[3];

        switch (groupbyType) {
            case PRECLUSTER:
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
            default:
                throw new IllegalArgumentException("Unrecognized groupbyType: " + groupbyType);
        }
        return obj;
    }

    public HDFSReadOperatorDescriptor createHDFSReader(JobSpecification jobSpec) throws HyracksDataException {
        try {
            InputSplit[] splits = hadoopJobConfFactory.getConf().getInputFormat()
                    .getSplits(hadoopJobConfFactory.getConf(), ncNodeNames.length);

            return new HDFSReadOperatorDescriptor(jobSpec, ReadsKeyValueParserFactory.readKmerOutputRec,
                    hadoopJobConfFactory.getConf(), splits, readSchedule, new ReadsKeyValueParserFactory(kmerSize, hadoopJobConfFactory));
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

        Object[] objs = generateAggeragateDescriptorbyType(jobSpec, keyFields, new AggregateKmerAggregateFactory(
                kmerSize), new MergeKmerAggregateFactory(kmerSize), new KmerHashPartitioncomputerFactory(),
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

    public AbstractOperatorDescriptor generateKmerToFinalNode(JobSpecification jobSpec,
            AbstractOperatorDescriptor kmerCrossAggregator) {

        AbstractOperatorDescriptor mapToFinalNode = new AssembleKeyIntoNodeOperator(jobSpec,
                AssembleKeyIntoNodeOperator.nodeOutputRec, kmerSize);
        connectOperators(jobSpec, kmerCrossAggregator, ncNodeNames, mapToFinalNode, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        return mapToFinalNode;
    }

    public AbstractOperatorDescriptor generateNodeWriterOpertator(JobSpecification jobSpec,
            AbstractOperatorDescriptor mapEachReadToNode) throws HyracksException {
        ITupleWriterFactory nodeWriter = null;
        switch (outputFormat) {
            case TEXT:
                nodeWriter = new NodeTextWriterFactory(kmerSize);
                break;
            case BINARY:
                nodeWriter = new NodeSequenceWriterFactory(hadoopJobConfFactory.getConf());
                break;
            default:
                throw new IllegalArgumentException("Invalid outputFormat: " + outputFormat);
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

        logDebug("Generate final node");
        lastOperator = generateKmerToFinalNode(jobSpec, lastOperator);

        logDebug("Write node to result");
        lastOperator = generateNodeWriterOpertator(jobSpec, lastOperator);

        jobSpec.addRoot(lastOperator);
        return jobSpec;
    }

    protected void initJobConfiguration(Scheduler scheduler) throws HyracksDataException {
        Configuration conf = confFactory.getConf();
        kmerSize = Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH));
        frameLimits = Integer.parseInt(conf.get(GenomixJobConf.FRAME_LIMIT));
//        tableSize = conf.getInt(GenomixJobConf.TABLE_SIZE, GenomixJobConf.DEFAULT_TABLE_SIZE);
        frameSize = Integer.parseInt(conf.get(GenomixJobConf.FRAME_SIZE));
        System.out.println(DEFAULT_FRAME_SIZE);
        System.out.println(frameSize);
        String type = conf.get(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        groupbyType = GroupbyType.PRECLUSTER;

        String output = conf.get(GenomixJobConf.OUTPUT_FORMAT);

        if (output.equalsIgnoreCase(GenomixJobConf.OUTPUT_FORMAT_TEXT)) {
            outputFormat = OutputFormat.TEXT;
        } else if(output.equalsIgnoreCase(GenomixJobConf.OUTPUT_FORMAT_BINARY)){
            outputFormat = OutputFormat.BINARY;
        } else {
            throw new IllegalArgumentException("Unrecognized outputFormat: " + output);
        }
        
        try {
            
            JobConf jobconf = new JobConf(conf);
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
