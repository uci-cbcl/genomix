package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;

/**
 * Remove tip or single node when l > constant
 * @author anbangx
 *
 */
public class TipRemoveVertex extends
        BasicGraphCleanVertex<VertexValueWritable, MessageWritable> {
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
    
    /**
     * detect the tip and figure out what edgeType neighborToTip is
     */
    public EDGETYPE getTipEdgetype(){
        VertexValueWritable vertex = getVertexValue();
        if(vertex.getDegree(DIR.PREVIOUS) == 0 && vertex.getDegree(DIR.NEXT) == 1){ //INCOMING TIP
            return vertex.getEdgetypeFromDir(DIR.NEXT);
        } else if(vertex.getDegree(DIR.PREVIOUS) == 1 && vertex.getDegree(DIR.NEXT) == 0){ //OUTGOING TIP
            return vertex.getEdgetypeFromDir(DIR.PREVIOUS);
        } else
            return null;
    }
    
    /**
     * step1
     */
    public void detectTip(){
        EDGETYPE neighborToTipEdgetype = getTipEdgetype();
        //I'm tip and my length is less than the minimum
        if(neighborToTipEdgetype != null && getVertexValue().getKmerLength() <= length){ 
            EDGETYPE tipToNeighborEdgetype = neighborToTipEdgetype.mirror();
            outgoingMsg.setFlag(tipToNeighborEdgetype.get());
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId = getDestVertexId(neighborToTipEdgetype.dir());
            sendMsg(destVertexId, outgoingMsg);
            deleteVertex(getVertexId());
            
            //set statistics counter: Num_RemovedTips
            updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
            getVertexValue().setCounters(counters);
        }
    }
    
    /**
     * step2
     */
    public void responseToDeadTip(Iterator<MessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            EDGETYPE tipToMeEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
            getVertexValue().getEdgeList(tipToMeEdgetype).remove(incomingMsg.getSourceVertexId());
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1)
            detectTip();
        else if(getSuperstep() == 2)
            responseToDeadTip(msgIterator);
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, TipRemoveVertex.class));
    }
}
