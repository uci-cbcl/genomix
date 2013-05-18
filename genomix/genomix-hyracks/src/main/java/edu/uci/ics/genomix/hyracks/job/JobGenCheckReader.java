package edu.uci.ics.genomix.hyracks.job;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import edu.uci.ics.genomix.hyracks.dataflow.ReadsKeyValueParserFactory;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionWritable;
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

public class JobGenCheckReader extends JobGenBrujinGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public JobGenCheckReader(GenomixJob job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
        // TODO Auto-generated constructor stub
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
        // Output Kmer
        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec, hadoopJobConfFactory.getConf(), new ITupleWriterFactory(){

            /**
             * 
             */
            private static final long serialVersionUID = 1L;
            private KmerBytesWritable kmer = new KmerBytesWritable(kmerSize);
            private PositionWritable pos = new PositionWritable();

            @Override
            public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
                return new ITupleWriter(){

                    @Override
                    public void open(DataOutput output) throws HyracksDataException {
                        // TODO Auto-generated method stub
                        
                    }

                    @Override
                    public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                        try {
                            if (kmer.getLength() > tuple.getFieldLength(ReadsKeyValueParserFactory.OutputKmerField)) {
                                throw new IllegalArgumentException("Not enough kmer bytes");
                            }
                            kmer.setNewReference(tuple.getFieldData(ReadsKeyValueParserFactory.OutputKmerField), tuple.getFieldStart(ReadsKeyValueParserFactory.OutputKmerField));
                            pos.setNewReference(tuple.getFieldData(ReadsKeyValueParserFactory.OutputPosition),
                                    tuple.getFieldStart(ReadsKeyValueParserFactory.OutputPosition));

                            output.write(kmer.toString().getBytes());
                            output.writeByte('\t');
                            output.write(pos.toString().getBytes());
                            output.writeByte('\n');
                        } catch (IOException e) {
                            throw new HyracksDataException(e);
                        }
                    }

                    @Override
                    public void close(DataOutput output) throws HyracksDataException {
                        // TODO Auto-generated method stub
                        
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
