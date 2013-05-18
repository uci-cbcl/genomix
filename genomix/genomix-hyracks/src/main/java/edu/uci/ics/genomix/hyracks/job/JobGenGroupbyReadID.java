package edu.uci.ics.genomix.hyracks.job;

import java.util.Map;

import edu.uci.ics.genomix.hyracks.dataflow.io.ReadIDAggregationTextWriterFactory;
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

public class JobGenGroupbyReadID extends JobGenBrujinGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public JobGenGroupbyReadID(GenomixJob job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
        // TODO Auto-generated constructor stub
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {

        JobSpecification jobSpec = new JobSpecification();
        logDebug("ReadKmer Operator");
        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);

        logDebug("Group by Kmer");
        AbstractOperatorDescriptor lastOperator = generateGroupbyKmerJob(jobSpec, readOperator);

        logDebug("Write kmer to result");
        generateRootByWriteKmerGroupbyResult(jobSpec, lastOperator);

        logDebug("Map Kmer to Read Operator");
        lastOperator = generateMapperFromKmerToRead(jobSpec, lastOperator);

        logDebug("Group by Read Operator");
        lastOperator = generateGroupbyReadJob(jobSpec, lastOperator);

        logDebug("Map ReadInfo to Node");
        lastOperator = generateMapperFromReadToNode(jobSpec, lastOperator);

        logDebug("Write node to result");
        generateRootByWriteReadIDAggregationResult(jobSpec, lastOperator);

        return jobSpec;
    }

    public AbstractOperatorDescriptor generateRootByWriteReadIDAggregationResult(JobSpecification jobSpec,
            AbstractOperatorDescriptor readCrossAggregator) throws HyracksException {
        ITupleWriterFactory readWriter = new ReadIDAggregationTextWriterFactory(kmerSize);
        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec, hadoopJobConfFactory.getConf(), readWriter);
        connectOperators(jobSpec, readCrossAggregator, ncNodeNames, writeKmerOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        jobSpec.addRoot(writeKmerOperator);
        return writeKmerOperator;
    }

}
