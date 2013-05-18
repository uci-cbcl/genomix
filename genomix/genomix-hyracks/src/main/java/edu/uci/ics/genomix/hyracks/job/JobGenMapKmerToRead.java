package edu.uci.ics.genomix.hyracks.job;

import java.util.Map;

import edu.uci.ics.genomix.hyracks.dataflow.io.MapperKmerToReadIDTextWriterFactory;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenMapKmerToRead extends JobGenBrujinGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public JobGenMapKmerToRead(GenomixJob job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
        // TODO Auto-generated constructor stub
    }
    
    public AbstractOperatorDescriptor generateRootByWriteMapperFromKmerToReadID(JobSpecification jobSpec, AbstractOperatorDescriptor mapper) throws HyracksException{
        // Output Kmer
        ITupleWriterFactory kmerWriter = new MapperKmerToReadIDTextWriterFactory(kmerSize);
        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec, hadoopJobConfFactory.getConf(), kmerWriter);
        connectOperators(jobSpec, mapper, ncNodeNames, writeKmerOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
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
