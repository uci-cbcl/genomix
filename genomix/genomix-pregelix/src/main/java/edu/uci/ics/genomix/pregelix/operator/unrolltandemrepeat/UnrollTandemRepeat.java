package edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

/**
 * Graph clean pattern: Unroll TandemRepeat
 * @author anbangx
 *
 */
public class UnrollTandemRepeat extends
    BasicGraphCleanVertex<VertexValueWritable, MessageWritable>{
    private EdgeWritable tmpEdge = new EdgeWritable();
    
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
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    /**
     * check if this tandem repeat can be solved
     */
    public boolean repeatCanBeMerged(){
        tmpValue.setAsCopy(getVertexValue());
        tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
        boolean hasFlip = false;
        /** pick one edge and flip **/
        for(byte d : DirectionFlag.values){
            for(EdgeWritable edge : tmpValue.getEdgeList(d)){
                byte flipDir = flipDir(d);
                tmpValue.getEdgeList(flipDir).add(edge);
                tmpValue.getEdgeList(d).remove(edge);
                /** setup hasFlip to go out of the loop **/
                hasFlip = true;
                break;
            }
            if(hasFlip)
                break;
        }
        
        if(VertexUtil.isPathVertex(tmpValue) || VertexUtil.isTipVertex(tmpValue)
                || VertexUtil.isSingleVertex(tmpValue))
            return true;
        else
            return false;
    }
    
    /**
     * merge tandem repeat
     */
    public void mergeTandemRepeat(){
        getVertexValue().getInternalKmer().mergeWithKmerInDir(repeatDir, kmerSize, getVertexId());
        getVertexValue().getEdgeList(repeatDir).remove(getVertexId());
        boolean hasFlip = false;
        /** pick one edge and flip **/
        for(byte d : DirectionFlag.values){
            for(EdgeWritable edge : getVertexValue().getEdgeList(d)){
                byte flipDir = flipDir(d);
                getVertexValue().getEdgeList(flipDir).add(edge);
                getVertexValue().getEdgeList(d).remove(edge);
                /** send flip message to node for updating edgeDir **/
                outgoingMsg.setFlag(flipDir);
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(edge.getKey(), outgoingMsg);
                /** setup hasFlip to go out of the loop **/
                hasFlip = true;
                break;
            }
            if(hasFlip)
                break;
        }
    }
    
    /**
     * update edges
     */
    public void updateEdges(){
        byte flipDir = flipDir((byte)(incomingMsg.getFlag() & MessageFlag.DEAD_MASK));
        byte prevNeighborToMe = mirrorDirection(flipDir);
        byte curNeighborToMe = mirrorDirection((byte)(incomingMsg.getFlag() & MessageFlag.DEAD_MASK));
        tmpEdge.setAsCopy(getVertexValue().getEdgeList(prevNeighborToMe).getEdge(incomingMsg.getSourceVertexId()));
        getVertexValue().getEdgeList(prevNeighborToMe).remove(incomingMsg.getSourceVertexId());
        getVertexValue().getEdgeList(curNeighborToMe).add(tmpEdge);
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
        initVertex();
        if(getSuperstep() == 1){
            if(isTandemRepeat(getVertexValue()) && repeatCanBeMerged()){
                mergeTandemRepeat();
                //set statistics counter: Num_RemovedTips
                updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
                getVertexValue().setCounters(counters);
            }
            voteToHalt();
        } else if(getSuperstep() == 2){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                /** update edge **/
                updateEdges();
            }
            voteToHalt();
        }
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, UnrollTandemRepeat.class));
    }
}
