package edu.uci.ics.genomix.pregelix.operator.removelowcoverage;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * Graph clean pattern: Remove Lowcoverage
 * @author anbangx
 *
 */
public class RemoveLowCoverageVertex extends
    BasicGraphCleanVertex<MessageWritable> {
    private static float minAverageCoverage = -1;
    
    private static Set<VKmerBytesWritable> deadNodeSet = Collections.synchronizedSet(new HashSet<VKmerBytesWritable>());
    
    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(incomingMsg == null)
            incomingMsg = new MessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1){
            if(getVertexValue().getAvgCoverage() <= minAverageCoverage){
                broadcaseReallyKillself();
                deadNodeSet.add(new VKmerBytesWritable(getVertexId()));
            }
            else
                voteToHalt();
        } else if(getSuperstep() == 2){
            if(deadNodeSet.contains(getVertexId())){
                deleteVertex(getVertexId());
                //set statistics counter: Num_RemovedLowCoverageNodes
                updateStatisticsCounter(StatisticsCounter.Num_RemovedLowCoverageNodes);
                getVertexValue().setCounters(counters);
            }
            else{
                while(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    if(isResponseKillMsg())
                        responseToDeadVertex();
                }
            }
            voteToHalt();
        } 
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, RemoveLowCoverageVertex.class));
    }
}
