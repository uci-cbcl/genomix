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
    
    
    private static Set<String> existKmerString = Collections.synchronizedSet(new HashSet<String>());
    private VKmerBytesWritable createdVertexId = null;  

    private EdgeWritable deletedEdge = new EdgeWritable();
    
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(outgoingMsg == null)
            outgoingMsg = new SplitRepeatMessageWritable();
        else
            outgoingMsg.reset();
        if(createdVertexId == null)
            createdVertexId = new VKmerBytesWritable();
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
        Random random = new Random();
        synchronized(existKmerString){
            while(true){
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
    
    public void randomGenerateVertexId(int numOfSuffix){
        String newVertexId = getVertexId().toString() + generaterRandomString(numOfSuffix);;
        createdVertexId.setByRead(kmerSize + numOfSuffix, newVertexId.getBytes(), 0);
    }
   
    public Set<Long> getEdgeIntersection(EdgeWritable incomingEdge, EdgeWritable outgoingEdge){
        Set<Long> incomingReadIds = incomingEdge.getSetOfReadIds();
        Set<Long> outgoingReadIds = outgoingEdge.getSetOfReadIds();
        Set<Long> edgeIntersection = new HashSet<Long>();
        edgeIntersection.addAll(incomingReadIds);
        edgeIntersection.retainAll(outgoingReadIds);
        
        return edgeIntersection;
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void createNewVertex(int i, EdgeWritable incomingEdge, EdgeWritable outgoingEdge){
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        VKmerBytesWritable vertexId = new VKmerBytesWritable();
        VertexValueWritable vertexValue = new VertexValueWritable();
        //add the corresponding edge to new vertex
        vertexValue.getEdgeList(connectedTable[i][0]).add(new EdgeWritable(incomingEdge));
        
        vertexValue.getEdgeList(connectedTable[i][1]).add(new EdgeWritable(outgoingEdge));
        
        vertexValue.setInternalKmer(getVertexId());
        
        vertexId.setAsCopy(createdVertexId);
        vertex.setVertexId(vertexId);
        vertex.setVertexValue(vertexValue);
        
        addVertex(vertexId, vertex);
    }
    
    public void sendMsgToUpdateEdge(EdgeWritable incomingEdge, EDGETYPE incomingEdgeType, 
            EdgeWritable outgoingEdge, EDGETYPE outgoingEdgeType, 
            Set<Long> edgeIntersection){
        EdgeWritable createdEdge = new EdgeWritable();
        createdEdge.setKey(createdVertexId);
        for(Long readId: edgeIntersection)
            createdEdge.appendReadID(readId);
        outgoingMsg.setCreatedEdge(createdEdge);
//        outgoingMsg.setSourceVertexId(getVertexId());
        deletedEdge.reset();
        deletedEdge.setKey(getVertexId());
        deletedEdge.setReadIDs(edgeIntersection);
        outgoingMsg.setDeletedEdge(deletedEdge);
        
        outgoingMsg.setFlag(incomingEdgeType.get());
        VKmerBytesWritable destVertexId = null;
        destVertexId = incomingEdge.getKey();
        sendMsg(destVertexId, outgoingMsg);
        
        outgoingMsg.setFlag(outgoingEdgeType.get());
        destVertexId = outgoingEdge.getKey();
        sendMsg(destVertexId, outgoingMsg);
    }
    
    public void deleteEdgeFromOldVertex(int i, EdgeWritable incomingEdge, EDGETYPE incomingEdgeType,
            EdgeWritable outgoingEdge, EDGETYPE outgoingEdgeType){
        getVertexValue().getEdgeList(incomingEdgeType).removeSubEdge(incomingEdge);
        getVertexValue().getEdgeList(outgoingEdgeType).removeSubEdge(outgoingEdge);
    }
    
    public void updateEdgeListPointToNewVertex(SplitRepeatMessageWritable incomingMsg){
        EDGETYPE meToNeighborDir = EDGETYPE.fromByte(incomingMsg.getFlag());//(byte) (incomingMsg.getFlag() & MessageFlag.VERTEX_MASK);
        EDGETYPE neighborToMeDir = meToNeighborDir.mirror();
        
        getVertexValue().getEdgeList(neighborToMeDir).removeSubEdge(incomingMsg.getDeletedEdge());
        getVertexValue().getEdgeList(neighborToMeDir).add(new EdgeWritable(incomingMsg.getCreatedEdge()));
    }
    
    public void detectRepeatAndSplit(){
        if(getVertexValue().getDegree() > 2){
            VertexValueWritable vertex = getVertexValue();
            // process connectedTable
            for(int i = 0; i < 4; i++){
                // set edgeType and the corresponding edgeList based on connectedTable
                EDGETYPE incomingEdgeType = connectedTable[i][0];
                EDGETYPE outgoingEdgeType = connectedTable[i][1];
                EdgeListWritable incomingEdgeList = vertex.getEdgeList(incomingEdgeType);
                EdgeListWritable outgoingEdgeList = vertex.getEdgeList(outgoingEdgeType);
                
                for(EdgeWritable incomingEdge : incomingEdgeList){
                    for(EdgeWritable outgoingEdge : outgoingEdgeList){
                        // set neighborEdge readId intersection
                        Set<Long> edgeIntersection = getEdgeIntersection(incomingEdge, outgoingEdge);
                        
                        if(!edgeIntersection.isEmpty()){
                            // random generate vertexId of new vertex
                            randomGenerateVertexId(NUM_LETTERS_TO_APPEND);
                            
                            // change new incomingEdge/outgoingEdge's edgeList to commondReadIdSet
                            EdgeWritable newIncomingEdge = new EdgeWritable();
                            EdgeWritable newOutgoingEdge = new EdgeWritable();
                            newIncomingEdge.setKey(incomingEdge.getKey());
                            newIncomingEdge.setReadIDs(edgeIntersection);
                            newOutgoingEdge.setKey(outgoingEdge.getKey());
                            newOutgoingEdge.setReadIDs(edgeIntersection);
                            
                            // create new/created vertex which has new incomingEdge/outgoingEdge
                            createNewVertex(i, newIncomingEdge, newOutgoingEdge);
                            
                            //set statistics counter: Num_SplitRepeats
                            incrementCounter(StatisticsCounter.Num_SplitRepeats);
                            getVertexValue().setCounters(counters);
                            
                            // send msg to neighbors to update their edges to new vertex 
                            sendMsgToUpdateEdge(newIncomingEdge, incomingEdgeType, 
                                    newOutgoingEdge, outgoingEdgeType, 
                                    edgeIntersection);
                            
                            // delete extra edges from old vertex
                            deleteEdgeFromOldVertex(i, newIncomingEdge, incomingEdgeType,
                                    newOutgoingEdge, outgoingEdgeType);
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
