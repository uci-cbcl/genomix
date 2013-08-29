package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * Remove tip or single node when l > constant
 * @author anbangx
 *
 */
public class TipRemoveVertex extends
        BasicGraphCleanVertex<MessageWritable> {
    private int length = -1;
    
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
            if(VertexUtil.isIncomingTipVertex(getVertexValue())){
            	if(getVertexValue().getKmerLength() <= length){
            	    sendSettledMsgToNextNode();
            		deleteVertex(getVertexId());
                    //set statistics counter: Num_RemovedTips
                    updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
                    getVertexValue().setCounters(counters);
            	}
            }
            else if(VertexUtil.isOutgoingTipVertex(getVertexValue())){
                if(getVertexValue().getKmerLength() <= length){
                    sendSettledMsgToPrevNode();
                    deleteVertex(getVertexId());
                    //set statistics counter: Num_RemovedTips
                    updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
                    getVertexValue().setCounters(counters);
                }
            }
            else if(VertexUtil.isSingleVertex(getVertexValue())){
                if(getVertexValue().getKmerLength() <= length){
                    deleteVertex(getVertexId());
                    //set statistics counter: Num_RemovedTips
                    updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
                    getVertexValue().setCounters(counters);
                }
            }
        }
        else if(getSuperstep() == 2){
        	while(msgIterator.hasNext()){
        		incomingMsg = msgIterator.next();
        		responseToDeadVertex();
        	}
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, TipRemoveVertex.class));
    }
}
