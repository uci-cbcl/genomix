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
package edu.uci.ics.genomix.hyracks.job;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.hyracks.dataflow.MapKmerPositionToReadOperator;
import edu.uci.ics.genomix.oldtype.PositionListWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenMapKmerToRead extends JobGenBrujinGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public JobGenMapKmerToRead(GenomixJobConf job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
    }

    public AbstractOperatorDescriptor generateRootByWriteMapperFromKmerToReadID(JobSpecification jobSpec,
            AbstractOperatorDescriptor mapper) throws HyracksException {
        // Output Kmer
        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec,
                hadoopJobConfFactory.getConf(), new ITupleWriterFactory() {

                    /**
                     * 
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
                        return new ITupleWriter() {

                            private KmerBytesWritable kmer = new KmerBytesWritable(kmerSize);
                            private PositionListWritable plist = new PositionListWritable();

                            @Override
                            public void open(DataOutput output) throws HyracksDataException {

                            }

                            @Override
                            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                                try {
                                    int readID = Marshal.getInt(
                                            tuple.getFieldData(MapKmerPositionToReadOperator.OutputReadIDField),
                                            tuple.getFieldStart(MapKmerPositionToReadOperator.OutputReadIDField));
                                    byte posInRead = tuple
                                            .getFieldData(MapKmerPositionToReadOperator.OutputPosInReadField)[tuple
                                            .getFieldStart(MapKmerPositionToReadOperator.OutputPosInReadField)];
                                    int posCount = PositionListWritable.getCountByDataLength(tuple
                                            .getFieldLength(MapKmerPositionToReadOperator.OutputOtherReadIDListField));
                                    plist.setNewReference(
                                            posCount,
                                            tuple.getFieldData(MapKmerPositionToReadOperator.OutputOtherReadIDListField),
                                            tuple.getFieldStart(MapKmerPositionToReadOperator.OutputOtherReadIDListField));

                                    String kmerString = "";
                                    if (posInRead > 0) {
                                        if (kmer.getLength() > tuple
                                                .getFieldLength(MapKmerPositionToReadOperator.OutputKmerField)) {
                                            throw new IllegalArgumentException("Not enough kmer bytes");
                                        }
                                        kmer.setAsReference(
                                                tuple.getFieldData(MapKmerPositionToReadOperator.OutputKmerField),
                                                tuple.getFieldStart(MapKmerPositionToReadOperator.OutputKmerField));
                                        kmerString = kmer.toString();
                                    }

                                    output.write(Integer.toString(readID).getBytes());
                                    output.writeByte('\t');
                                    output.write(Integer.toString(posInRead).getBytes());
                                    output.writeByte('\t');
                                    output.write(plist.toString().getBytes());
                                    output.writeByte('\t');
                                    output.write(kmerString.getBytes());
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
        connectOperators(jobSpec, mapper, ncNodeNames, writeKmerOperator, ncNodeNames, new OneToOneConnectorDescriptor(
                jobSpec));
        jobSpec.addRoot(writeKmerOperator);
        return writeKmerOperator;
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {

        JobSpecification jobSpec = new JobSpecification();
        logDebug("ReadKmer Operator");
        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);

        logDebug("Group by Kmer");
        AbstractOperatorDescriptor lastOperator = generateGroupbyKmerJob(jobSpec, readOperator);

        logDebug("Map Kmer to Read Operator");
        lastOperator = generateMapperFromKmerToRead(jobSpec, lastOperator);

        generateRootByWriteMapperFromKmerToReadID(jobSpec, lastOperator);

        return jobSpec;
    }
}
