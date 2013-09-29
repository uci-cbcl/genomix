package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.util.Iterator;
import java.util.logging.Logger;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;

/**
 * Remove tip or single node when kmerLength < MIN_LENGTH_TO_KEEP
 * 
 * Details: Sequencing errors at the ends of the reads form "tips": short, low coverage nodes 
 * with in-degree + out-degree = 1 (they either have a single edge in or a single edge out). 
 * The algorithm identifies these nodes and prunes them from the graph. This is then followed 
 * by recompressing the graph.
 * @author anbangx
 *
 */
public class TipRemoveVertex extends
        BasicGraphCleanVertex<VertexValueWritable, MessageWritable> {
    
    private static final Logger LOG = Logger.getLogger(TipRemoveVertex.class.getName());
    
    private int MIN_LENGTH_TO_KEEP = -1;
    
    /**
     * initiate kmerSize, length
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(MIN_LENGTH_TO_KEEP == -1){
            MIN_LENGTH_TO_KEEP = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.TIP_REMOVE_MAX_LENGTH));
        }
        if(outgoingMsg == null){
            outgoingMsg = new MessageWritable();
        }
        else{
            outgoingMsg.reset();
        }
        StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    /**
     * detect the tip and figure out what edgeType neighborToTip is
     */
    public EDGETYPE getTipToNeighbor(){
        VertexValueWritable vertex = getVertexValue();
        for (DIR d : DIR.values()) {
        	if (vertex.getDegree(d) == 1 && vertex.getDegree(d.mirror()) == 0) {
        		return vertex.getNeighborEdgeType(d);
        	}
        }
        return null;
    }
    
    /**
     * step 1
     */
    public void updateTipNeighbor(){
        EDGETYPE tipToNeighborEdgetype = getTipToNeighbor();
        //I'm tip and my length is less than the minimum
        if(tipToNeighborEdgetype != null && getVertexValue().getKmerLength() <= MIN_LENGTH_TO_KEEP){
        	outgoingMsg.reset();
            outgoingMsg.setFlag(tipToNeighborEdgetype.mirror().get());
            outgoingMsg.setSourceVertexId(getVertexId());
            EdgeListWritable edgeList = getVertexValue().getEdgeList(tipToNeighborEdgetype);
            if(edgeList.size() != 1)
                throw new IllegalArgumentException("In this edgeType, the size of edges has to be 1!");
            VKmerBytesWritable destVertexId = edgeList.get(0).getKey();
            sendMsg(destVertexId, outgoingMsg);
            deleteVertex(getVertexId());
            
            if(verbose){
                LOG.fine("I'm tip! " + "\r\n"
                		+ "My vertexId is " + getVertexId() + "\r\n"
                        + "My vertexValue is " + getVertexValue() + "\r\n"
                        + "Kill self and broadcast kill self to " + destVertexId + "\r\n"
                        + "The message is: " + outgoingMsg + "\r\n\n");
            }
            //set statistics counter: Num_RemovedTips 
            incrementCounter(StatisticsCounter.Num_RemovedTips);
            getVertexValue().setCounters(counters);  // TODO take a long hard look at how we run the logic of counters...
        } 
    }
    
    /**
     * step 2
     */
    public void processUpdates(Iterator<MessageWritable> msgIterator){
        responseToDeadNode(msgIterator);
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        if(getSuperstep() == 1){
            initVertex(); 
            updateTipNeighbor();
        }
        else if(getSuperstep() == 2){
            processUpdates(msgIterator);
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, TipRemoveVertex.class));
    }
}
