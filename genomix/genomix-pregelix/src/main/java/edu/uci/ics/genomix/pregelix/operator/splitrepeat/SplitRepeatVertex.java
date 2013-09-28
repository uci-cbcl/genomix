package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.SplitRepeatMessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.NodeWritable.NeighborInfo;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * Graph clean pattern: Split Repeat
 * @author anbangx
 *
 */
public class SplitRepeatVertex extends 
    BasicGraphCleanVertex<VertexValueWritable, SplitRepeatMessageWritable>{
    
    public static final int NUM_LETTERS_TO_APPEND = 3;
    
//    private NeighborInfo newReverseNeighborInfo;
//    private NeighborInfo newForwardNeighborInfo;
    private EdgeWritable newReverseEdge = new EdgeWritable();
    private EdgeWritable newForwardEdge = new EdgeWritable();
    
    private static Set<String> existKmerString = Collections.synchronizedSet(new HashSet<String>());

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        //TODO initialize (using new) when you declare these variables that don't depend on the conf
        if(outgoingMsg == null)
            outgoingMsg = new SplitRepeatMessageWritable();
        else
            outgoingMsg.reset();         //TODO don't reset here; rather reset right before you use the outgoingMsg
        StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }
    
    /**
     * Generate random string from [ACGT]
     */
    public String generaterRandomString(int n){
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random(); // TODO use the seed given from cmd line... but only set this once at the beginning of the algorithm (don't reset the seed here)
        synchronized(existKmerString){ // make local(not static) and remove synchronized
            while(true){ // TODO what if the len(existing) > num_letters added ? (infinite loop) 
                for (int i = 0; i < n; i++) {
                    char c = chars[random.nextInt(chars.length)];
                    sb.append(c);
                }
                if(!existKmerString.contains(sb.toString()))
                    break;
            }
            existKmerString.add(sb.toString());
        }
        return sb.toString();
    }
    
    public VKmerBytesWritable randomGenerateVertexId(int numOfSuffix){
        String newVertexId = getVertexId().toString() + generaterRandomString(numOfSuffix);
        VKmerBytesWritable createdVertexId = new VKmerBytesWritable(); 
        createdVertexId.setByRead(kmerSize + numOfSuffix, newVertexId.getBytes(), 0);
        return createdVertexId;
    }
   
    // TODO LATER implement EdgeListWritbale's array of long to TreeMap(sorted)
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void createNewVertex(VKmerBytesWritable createdVertexId, NeighborInfo reverseNeighborInfo,
    		NeighborInfo forwardNeighborInfo){
        Vertex newVertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        VKmerBytesWritable vertexId = new VKmerBytesWritable();
        VertexValueWritable vertexValue = new VertexValueWritable();
        //add the corresponding edge to new vertex
        vertexValue.getEdgeList(reverseNeighborInfo.et).add(new EdgeWritable(reverseNeighborInfo.edge));
        vertexValue.getEdgeList(forwardNeighborInfo.et).add(new EdgeWritable(forwardNeighborInfo.edge));
        
        vertexValue.setInternalKmer(getVertexId());
        
        vertexId.setAsCopy(createdVertexId);
        newVertex.setVertexId(vertexId);
        newVertex.setVertexValue(vertexValue);
        
        addVertex(vertexId, newVertex);
    }
    
    public void updateNeighbors(VKmerBytesWritable createdVertexId, Set<Long> edgeIntersection,
            NeighborInfo newReverseNeighborInfo, NeighborInfo newForwardNeighborInfo){
    	// TODO simplify this to use generic update.  operation is "SPLIT_EDGE?" and should use the Node inside the message directly
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(getVertexId());
        EdgeWritable createdEdge = outgoingMsg.getCreatedEdge();
        createdEdge.setKey(createdVertexId);
        createdEdge.setReadIDs(edgeIntersection);
        
        EDGETYPE neighborToRepeat = newReverseNeighborInfo.et.mirror();
        outgoingMsg.setFlag(neighborToRepeat.get());
        sendMsg(newReverseNeighborInfo.edge.getKey(), outgoingMsg);
        
        neighborToRepeat = newForwardNeighborInfo.et.mirror();
        outgoingMsg.setFlag(neighborToRepeat.get());
        sendMsg(newForwardNeighborInfo.edge.getKey(), outgoingMsg);
    }        
    
    public void deleteEdgeFromOldVertex(NeighborInfo newReverseNeighborInfo, NeighborInfo newForwardNeighborInfo){
        getVertexValue().getEdgeList(newReverseNeighborInfo.et).removeSubEdge(newReverseNeighborInfo.edge);
        getVertexValue().getEdgeList(newReverseNeighborInfo.et).removeSubEdge(newReverseNeighborInfo.edge);
    }
    
    public void updateEdgeListPointToNewVertex(SplitRepeatMessageWritable incomingMsg){
        EDGETYPE meToNeighbor = EDGETYPE.fromByte(incomingMsg.getFlag());
        EdgeWritable createdEdge = incomingMsg.getCreatedEdge();
        EdgeWritable deletedEdge = createdEdge;
        deletedEdge.setKey(incomingMsg.getSourceVertexId());
        
        getVertexValue().getEdgeList(meToNeighbor).add(new EdgeWritable(createdEdge));
        getVertexValue().getEdgeList(meToNeighbor).removeSubEdge(deletedEdge);
    }
    
    public void detectRepeatAndSplit(){
        if(getVertexValue().getDegree() > 2){
            VertexValueWritable vertex = getVertexValue();
            // process connectedTable
            for(int i = 0; i < connectedTable.length; i++){
                // set edgeType and the corresponding edgeList based on connectedTable
                EDGETYPE reverseEdgeType = connectedTable[i][0];
                EDGETYPE forwardEdgeType = connectedTable[i][1];
                EdgeListWritable reverseEdgeList = vertex.getEdgeList(reverseEdgeType);
                EdgeListWritable forwardEdgeList = vertex.getEdgeList(forwardEdgeType);
                
                for(EdgeWritable reverseEdge : reverseEdgeList){
                    for(EdgeWritable forwardEdge : forwardEdgeList){
                        // set neighborEdge readId intersection
                        Set<Long> edgeIntersection = EdgeWritable.getEdgeIntersection(reverseEdge, forwardEdge);
                        
                        if(!edgeIntersection.isEmpty()){
                            // random generate vertexId of new vertex // TODO create new vertex when add letters, the #letter depends on the time, which can't cause collision
                            VKmerBytesWritable createdVertexId = randomGenerateVertexId(NUM_LETTERS_TO_APPEND);
                            
                            // change new incomingEdge/outgoingEdge's edgeList to commondReadIdSet
                            newReverseEdge.reset();
                            newReverseEdge.setKey(reverseEdge.getKey());
                            newReverseEdge.setReadIDs(edgeIntersection);
                            newForwardEdge.reset();
                            newForwardEdge.setKey(forwardEdge.getKey());
                            newForwardEdge.setReadIDs(edgeIntersection);
                            
                            NeighborInfo newReverseNeighborInfo = new NeighborInfo(reverseEdgeType, newReverseEdge); 
                            NeighborInfo newForwardNeighborInfo = new NeighborInfo(forwardEdgeType, newForwardEdge);
                            
                            // create new/created vertex which has new incomingEdge/outgoingEdge
                            createNewVertex(createdVertexId, newReverseNeighborInfo, newForwardNeighborInfo);
                            
                            //set statistics counter: Num_SplitRepeats
                            incrementCounter(StatisticsCounter.Num_SplitRepeats);
                            getVertexValue().setCounters(counters);
                            
                            // send msg to neighbors to update their edges to new vertex 
                            updateNeighbors(createdVertexId, edgeIntersection, 
                                    newReverseNeighborInfo, newForwardNeighborInfo);
                            
                            // store deleteSet, move this out of loop
                            // delete extra edges from old vertex
                            deleteEdgeFromOldVertex(newReverseNeighborInfo, newForwardNeighborInfo);
                        }
                    }
                }                
            }
            
            // Old vertex delete or voteToHalt 
            if(getVertexValue().getDegree() == 0)//if no any edge, delete
                deleteVertex(getVertexId());
            else
                voteToHalt();
        }
    }
    
    public void responseToRepeat(Iterator<SplitRepeatMessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            SplitRepeatMessageWritable incomingMsg = msgIterator.next();
            // update edgelist to new/created vertex
            updateEdgeListPointToNewVertex(incomingMsg);
        }
    }
    
    @Override
    public void compute(Iterator<SplitRepeatMessageWritable> msgIterator) {
        if(getSuperstep() == 1){
            initVertex();
            detectRepeatAndSplit();
        } else if(getSuperstep() == 2){
            responseToRepeat(msgIterator);
            voteToHalt();
        }
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, SplitRepeatVertex.class));
    }
    
}
