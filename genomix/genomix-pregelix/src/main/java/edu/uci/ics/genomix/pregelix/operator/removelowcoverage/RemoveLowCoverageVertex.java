package edu.uci.ics.genomix.pregelix.operator.removelowcoverage;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * Graph clean pattern: Remove Lowcoverage
 * @author anbangx
 *
 */
public class RemoveLowCoverageVertex extends
    BasicGraphCleanVertex<VertexValueWritable,MessageWritable> {
    private static float minAverageCoverage = -1;
    
    private static Set<VKmerBytesWritable> deadNodeSet = Collections.synchronizedSet(new HashSet<VKmerBytesWritable>());
    
    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex(); 
        if (minAverageCoverage < 0)
            minAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.REMOVE_LOW_COVERAGE_MAX_COVERAGE));
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    public void detectLowCoverageVertex(){
        if(getVertexValue().getAvgCoverage() <= minAverageCoverage){
            //broadcase kill self
            broadcastKillself();
            deadNodeSet.add(new VKmerBytesWritable(getVertexId()));
        }
    }
    
    public void cleanupDeadVertex(){
        deleteVertex(getVertexId());
        //set statistics counter: Num_RemovedLowCoverageNodes
        incrementCounter(StatisticsCounter.Num_RemovedLowCoverageNodes);
        getVertexValue().setCounters(counters);
    }
    
    public void responseToDeadVertex(Iterator<MessageWritable> msgIterator){
        MessageWritable incomingMsg;
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            //response to dead node
            EDGETYPE deadToMeEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            getVertexValue().getEdgeList(deadToMeEdgetype).remove(incomingMsg.getSourceVertexId());
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1)
            detectLowCoverageVertex();
        else if(getSuperstep() == 2){
            if(deadNodeSet.contains(getVertexId()))
                cleanupDeadVertex();
            else
                responseToDeadVertex(msgIterator);
        } 
        voteToHalt();
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, RemoveLowCoverageVertex.class));
    }
}
