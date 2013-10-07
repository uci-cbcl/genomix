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

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.graph.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

//TODO move it to test-code
public class JobGenCheckReader extends JobGenOldBrujinGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public JobGenCheckReader(GenomixJobConf job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {

        JobSpecification jobSpec = new JobSpecification();
        logDebug("ReadKmer Operator");
        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);

        logDebug("Write kmer to result");
        generateRootByWriteKmerReader(jobSpec, readOperator);

        return jobSpec;
    }

    public AbstractSingleActivityOperatorDescriptor generateRootByWriteKmerReader(JobSpecification jobSpec,
            HDFSReadOperatorDescriptor readOperator) throws HyracksException {

        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec,
                hadoopJobConfFactory.getConf(), new ITupleWriterFactory() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx, int partition, int nPartition)
                            throws HyracksDataException {
                        Kmer.setGlobalKmerLength(kmerSize); // TODO is this the right place for this?
                        return new ITupleWriter() {

                            private Node outputNode = new Node();
                            private Kmer outputKmer = new Kmer();

                            @Override
                            public void open(DataOutput output) throws HyracksDataException {
                            }

                            @Override
                            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                                try {
                                    if (outputKmer.getLength() > tuple
                                            .getFieldLength(ReadsKeyValueParserFactory.OutputKmerField)) {
                                        throw new IllegalArgumentException("Not enough kmer bytes");
                                    }
                                    outputKmer.setAsReference(
                                            tuple.getFieldData(ReadsKeyValueParserFactory.OutputKmerField),
                                            tuple.getFieldStart(ReadsKeyValueParserFactory.OutputKmerField));
                                    outputNode.setAsReference(
                                            tuple.getFieldData(ReadsKeyValueParserFactory.OutputNodeField),
                                            tuple.getFieldStart(ReadsKeyValueParserFactory.OutputNodeField));
                                    output.write(outputKmer.toString().getBytes());
                                    output.writeByte('\t');
                                    output.write(outputNode.toString().getBytes());
                                    output.writeByte('\n');
                                } catch (IOException e) {
                                    throw new HyracksDataException(e);
                                }
                            }

                            @Override
                            public void close(DataOutput output) throws HyracksDataException {

                            }

                        };
                    }

                });
        connectOperators(jobSpec, readOperator, ncNodeNames, writeKmerOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        jobSpec.addRoot(writeKmerOperator);
        return writeKmerOperator;
    }

}
