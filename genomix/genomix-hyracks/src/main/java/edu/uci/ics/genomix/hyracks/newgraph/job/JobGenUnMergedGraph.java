package edu.uci.ics.genomix.hyracks.newgraph.job;

import java.util.Map;

import edu.uci.ics.genomix.hyracks.newgraph.io.KeyValueSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.newgraph.io.KeyValueTextWriterFactory;
import edu.uci.ics.genomix.hyracks.newgraph.io.NodeSequenceWriterFactory;
import edu.uci.ics.genomix.hyracks.newgraph.io.NodeTextWriterFactory;
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

public class JobGenUnMergedGraph extends JobGenBrujinGraph{

    public JobGenUnMergedGraph(GenomixJobConf job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
    }
    
    public AbstractOperatorDescriptor generateUnmergedunMergedWriterOpertator(JobSpecification jobSpec,
            AbstractOperatorDescriptor kmerCrossAggregator) throws HyracksException {
        ITupleWriterFactory unMergedWriter = null;
        switch (outputFormat) {
            case TEXT:
                unMergedWriter = new KeyValueTextWriterFactory(kmerSize);
                break;
            case BINARY:
            default:
                unMergedWriter = new KeyValueSequenceWriterFactory(hadoopJobConfFactory.getConf());
                break;
        }
        logDebug("WriteOperator");
        // Output Node
        HDFSWriteOperatorDescriptor writeNodeOperator = new HDFSWriteOperatorDescriptor(jobSpec,
                hadoopJobConfFactory.getConf(), unMergedWriter);
        connectOperators(jobSpec, kmerCrossAggregator, ncNodeNames, writeNodeOperator, ncNodeNames,
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

        logDebug("Write node to result");
        lastOperator = generateUnmergedunMergedWriterOpertator(jobSpec, lastOperator);

        jobSpec.addRoot(lastOperator);
        return jobSpec;
    }
    
}
