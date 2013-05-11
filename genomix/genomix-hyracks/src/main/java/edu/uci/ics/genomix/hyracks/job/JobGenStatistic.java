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

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import edu.uci.ics.genomix.hyracks.data.accessors.ByteSerializerDeserializer;
import edu.uci.ics.genomix.hyracks.util.ByteComparatorFactory;
import edu.uci.ics.genomix.hyracks.util.StatCountAggregateFactory;
import edu.uci.ics.genomix.hyracks.util.StatReadsKeyValueParserFactory;
import edu.uci.ics.genomix.hyracks.util.StatSumAggregateFactory;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.AbstractFileWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.LineFileWriteOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.external.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenStatistic extends JobGen {
    private int kmers;
    private JobConf hadoopjob;
    private RecordDescriptor readOutputRec;
    private String[] ncNodeNames;
    private Scheduler scheduler;
    private RecordDescriptor combineOutputRec;

    public JobGenStatistic(GenomixJob job) {
        super(job);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void initJobConfiguration() {
        // TODO Auto-generated method stub
        kmers = conf.getInt(GenomixJob.KMER_LENGTH, GenomixJob.DEFAULT_KMER);
        hadoopjob = new JobConf(conf);
        hadoopjob.setInputFormat(SequenceFileInputFormat.class);
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {
        int[] degreeFields = { 0, 1 }; // indegree, outdegree
        int[] countFields = { 2 };
        JobSpecification jobSpec = new JobSpecification();
        /** specify the record fields after read */
        readOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { ByteSerializerDeserializer.INSTANCE,
                ByteSerializerDeserializer.INSTANCE, ByteSerializerDeserializer.INSTANCE });
        combineOutputRec = new RecordDescriptor(new ISerializerDeserializer[] { ByteSerializerDeserializer.INSTANCE,
                ByteSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        /** the reader */
        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, readOperator, ncNodeNames);

        /** the combiner aggregator */
        AbstractOperatorDescriptor degreeLocal = connectLocalAggregateByField(jobSpec, degreeFields, readOperator);
        AbstractOperatorDescriptor countLocal = connectLocalAggregateByField(jobSpec, countFields, readOperator);

        /** the final aggregator */
        AbstractOperatorDescriptor degreeMerger = connectFinalAggregateByField(jobSpec, degreeFields, degreeLocal);
        AbstractOperatorDescriptor countMerger = connectFinalAggregateByField(jobSpec, countFields, countLocal);

        /** writer */
        AbstractFileWriteOperatorDescriptor writeDegree = connectWriter(jobSpec, degreeFields, degreeMerger);
        AbstractFileWriteOperatorDescriptor writeCount = connectWriter(jobSpec, countFields, countMerger);
        jobSpec.addRoot(writeDegree);
        jobSpec.addRoot(writeCount);
        return jobSpec;
    }

    private HDFSReadOperatorDescriptor createHDFSReader(JobSpecification jobSpec) throws HyracksDataException {
        try {

            InputSplit[] splits = hadoopjob.getInputFormat().getSplits(hadoopjob, ncNodeNames.length);

            String[] readSchedule = scheduler.getLocationConstraints(splits);
            return new HDFSReadOperatorDescriptor(jobSpec, readOutputRec, hadoopjob, splits, readSchedule,
                    new StatReadsKeyValueParserFactory());
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
    }

    private ExternalGroupOperatorDescriptor newExternalGroupby(JobSpecification jobSpec, int[] keyFields,
            IAggregatorDescriptorFactory aggeragater) {
        return new ExternalGroupOperatorDescriptor(jobSpec, keyFields, GenomixJob.DEFAULT_FRAME_LIMIT,
                new IBinaryComparatorFactory[] { new ByteComparatorFactory(), new ByteComparatorFactory() }, null,
                aggeragater, new StatSumAggregateFactory(), combineOutputRec, new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(keyFields, new IBinaryHashFunctionFactory[] {
                                new ByteComparatorFactory(), new ByteComparatorFactory() }),
                        GenomixJob.DEFAULT_TABLE_SIZE), true);
    }

    private AbstractOperatorDescriptor connectLocalAggregateByField(JobSpecification jobSpec, int[] fields,
            HDFSReadOperatorDescriptor readOperator) {
        AbstractOperatorDescriptor localAggregator = newExternalGroupby(jobSpec, fields,
                new StatCountAggregateFactory());
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, localAggregator, ncNodeNames);
        IConnectorDescriptor readfileConn = new OneToOneConnectorDescriptor(jobSpec);
        jobSpec.connect(readfileConn, readOperator, 0, localAggregator, 0);
        return localAggregator;
    }

    private AbstractOperatorDescriptor connectFinalAggregateByField(JobSpecification jobSpec, int[] fields,
            AbstractOperatorDescriptor localAggregator) {
        AbstractOperatorDescriptor finalAggregator = newExternalGroupby(jobSpec, fields, new StatSumAggregateFactory());
        // only need one reducer
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, finalAggregator, ncNodeNames[fields[0]
                % ncNodeNames.length]);
        IConnectorDescriptor mergeConn = new MToNPartitioningMergingConnectorDescriptor(jobSpec,
                new ITuplePartitionComputerFactory() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ITuplePartitionComputer createPartitioner() {
                        return new ITuplePartitionComputer() {
                            @Override
                            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts)
                                    throws HyracksDataException {
                                return 0;
                            }
                        };
                    }
                }, fields, new IBinaryComparatorFactory[] { new ByteComparatorFactory() });
        jobSpec.connect(mergeConn, localAggregator, 0, finalAggregator, 0);
        return finalAggregator;
    }

    private AbstractFileWriteOperatorDescriptor connectWriter(JobSpecification jobSpec, int[] fields,
            AbstractOperatorDescriptor finalAggregator) {
        LineFileWriteOperatorDescriptor writeOperator = new LineFileWriteOperatorDescriptor(jobSpec, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec, writeOperator, ncNodeNames[fields[0]
                % ncNodeNames.length]);

        IConnectorDescriptor printConn = new OneToOneConnectorDescriptor(jobSpec);
        jobSpec.connect(printConn, finalAggregator, 0, writeOperator, 0);
        return writeOperator;
    }
}
