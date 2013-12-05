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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.data.primitive.KmerNormarlizedComputerFactory;
import edu.uci.ics.genomix.hyracks.data.primitive.KmerPartitionComputerFactory;
import edu.uci.ics.genomix.hyracks.data.primitive.KmerPointable;
import edu.uci.ics.genomix.hyracks.graph.dataflow.AggregateKmerAggregateFactory;
import edu.uci.ics.genomix.hyracks.graph.dataflow.ConnectorPolicyAssignmentPolicy;
import edu.uci.ics.genomix.hyracks.graph.dataflow.KmerNodePairSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.graph.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
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
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenBuildBrujinGraph extends JobGen {

    public enum GroupbyType {
        EXTERNAL,
        PRECLUSTER,
        HYBRIDHASH,
    }

    private static final long serialVersionUID = 1L;

    private GroupbyType groupbyType;

    public JobGenBuildBrujinGraph(GenomixJobConf job, Scheduler scheduler, final Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
    }

    @Override
    protected void initGenomixConfiguration() throws HyracksDataException {
        super.initGenomixConfiguration();
        Configuration conf = hadoopJobConfFactory.getConf();
        groupbyType = GroupbyType.valueOf(conf.get(GenomixJobConf.HYRACKS_GROUPBY_TYPE,
                GroupbyType.PRECLUSTER.toString()));
    }

    @Override
    public JobSpecification assignJob(JobSpecification jobSpec) throws HyracksException {

        HDFSReadOperatorDescriptor readOperator = JobGenReadLetterParser.createHDFSReader(jobSpec,
                super.hadoopJobConfFactory, super.getInputSplit(), super.readSchedule);

        AbstractOperatorDescriptor lastOperator = generateGroupbyKmerJob(jobSpec, readOperator);

        lastOperator = generateKmerNodeWriterOpertator(jobSpec, lastOperator);

        jobSpec.addRoot(lastOperator);
        return jobSpec;
    }

    public AbstractOperatorDescriptor generateGroupbyKmerJob(JobSpecification jobSpec,
            AbstractOperatorDescriptor parserOperator) throws HyracksDataException {
        int[] keyFields = new int[] { 0 }; // the id of grouped key

        ExternalSortOperatorDescriptor sorter = new ExternalSortOperatorDescriptor(jobSpec, frameLimits, keyFields,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(KmerPointable.FACTORY) },
                ReadsKeyValueParserFactory.readKmerOutputRec);

        connectOperators(jobSpec, parserOperator, ncNodeNames, sorter, ncNodeNames, new OneToOneConnectorDescriptor(
                jobSpec));

        RecordDescriptor combineKmerOutputRec = new RecordDescriptor(new ISerializerDeserializer[2]);

        Object[] objs = generateAggeragateDescriptorbyType(jobSpec, keyFields,
                new AggregateKmerAggregateFactory(hadoopJobConfFactory.getConf()),
                new AggregateKmerAggregateFactory(hadoopJobConfFactory.getConf()), new KmerPartitionComputerFactory(),
                new KmerNormarlizedComputerFactory(), KmerPointable.FACTORY, combineKmerOutputRec, combineKmerOutputRec);
        AbstractOperatorDescriptor kmerLocalAggregator = (AbstractOperatorDescriptor) objs[0];
        connectOperators(jobSpec, sorter, ncNodeNames, kmerLocalAggregator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));

        IConnectorDescriptor kmerConnPartition = (IConnectorDescriptor) objs[1];
        AbstractOperatorDescriptor kmerCrossAggregator = (AbstractOperatorDescriptor) objs[2];
        connectOperators(jobSpec, kmerLocalAggregator, ncNodeNames, kmerCrossAggregator, ncNodeNames, kmerConnPartition);
        return kmerCrossAggregator;
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
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) }, normalizer);
                obj[2] = new PreclusteredGroupOperatorDescriptor(jobSpec, keyFields,
                        new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(pointable) }, merger,
                        finalRec);
                jobSpec.setConnectorPolicyAssignmentPolicy(new ConnectorPolicyAssignmentPolicy());
                break;
            default:
                throw new IllegalArgumentException("Unsupport groupbyType: " + groupbyType);
        }
        return obj;
    }

    public AbstractOperatorDescriptor generateKmerNodeWriterOpertator(JobSpecification jobSpec,
            AbstractOperatorDescriptor kmerCrossAggregator) throws HyracksException {
        ITupleWriterFactory writer = new KmerNodePairSequenceWriterFactory(hadoopJobConfFactory.getConf());
        // Output Node
        HDFSWriteOperatorDescriptor writeNodeOperator = new HDFSWriteOperatorDescriptor(jobSpec,
                hadoopJobConfFactory.getConf(), writer);
        connectOperators(jobSpec, kmerCrossAggregator, ncNodeNames, writeNodeOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        return writeNodeOperator;
    }

}
