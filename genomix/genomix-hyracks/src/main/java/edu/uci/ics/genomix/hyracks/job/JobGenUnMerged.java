package edu.uci.ics.genomix.hyracks.job;

import java.util.Map;

import edu.uci.ics.genomix.hyracks.dataflow.MapReadToNodeOperator;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenUnMerged extends JobGenBrujinGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public JobGenUnMerged(GenomixJobConf job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
    }

    @Override
    public AbstractOperatorDescriptor generateMapperFromReadToNode(JobSpecification jobSpec,
            AbstractOperatorDescriptor readCrossAggregator) {
        AbstractOperatorDescriptor mapEachReadToNode = new MapReadToNodeOperator(jobSpec,
                MapReadToNodeOperator.nodeOutputRec, kmerSize, false);
        connectOperators(jobSpec, readCrossAggregator, ncNodeNames, mapEachReadToNode, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));
        return mapEachReadToNode;
    }
}
