package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.genomix.pregelix.io.SplitRepeatMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class SplitRepeatVertex extends 
    BasicGraphCleanVertex<SplitRepeatMessageWritable>{
    
    public class DeletedEdge{
        private byte dir;
        private EdgeWritable edge;
        
        public DeletedEdge(){
            dir = 0;
            edge = new EdgeWritable();
        }

        public byte getDir() {
            return dir;
        }

        public void setDir(byte dir) {
            this.dir = dir;
        }

        public EdgeWritable getEdge() {
            return edge;
        }

        public void setEdge(EdgeWritable edge) {
            this.edge.setAsCopy(edge);
        }
    }
    
    private byte[][] connectedTable = new byte[][]{
            {MessageFlag.DIR_RF, MessageFlag.DIR_FF},
            {MessageFlag.DIR_RF, MessageFlag.DIR_FR},
            {MessageFlag.DIR_RR, MessageFlag.DIR_FF},
            {MessageFlag.DIR_RR, MessageFlag.DIR_FR}
    };
    
    public static Set<String> existKmerString = new HashSet<String>();
    protected VKmerBytesWritable createdVertexId = null;  
    private Set<Long> incomingReadIdSet = new HashSet<Long>();
    private Set<Long> outgoingReadIdSet = new HashSet<Long>();
    private Set<Long> neighborEdgeIntersection = new HashSet<Long>();
    private EdgeListWritable incomingEdgeList = null; 
    private EdgeListWritable outgoingEdgeList = null; 
    private byte incomingEdgeDir = 0;
    private byte outgoingEdgeDir = 0;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(incomingMsg == null)
            incomingMsg = new SplitRepeatMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new SplitRepeatMessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
        if(incomingEdgeList == null)
            incomingEdgeList = new EdgeListWritable();
        if(outgoingEdgeList == null)
            outgoingEdgeList = new EdgeListWritable();
        if(createdVertexId == null)
            createdVertexId = new VKmerBytesWritable();
    }
    
    /**
     * Generate random string from [ACGT]
     */
    public String generaterRandomString(int n){
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        while(true){
            for (int i = 0; i < n; i++) {
                char c = chars[random.nextInt(chars.length)];
                sb.append(c);
            }
            if(!existKmerString.contains(sb.toString()))
                break;
        }
        existKmerString.add(sb.toString());
        return sb.toString();
    }
    
    public void randomGenerateVertexId(int numOfSuffix){
        String newVertexId = getVertexId().toString() + generaterRandomString(numOfSuffix);;
        createdVertexId.setByRead(kmerSize + numOfSuffix, newVertexId.getBytes(), 0);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void createNewVertex(int i, EdgeWritable incomingEdge, EdgeWritable outgoingEdge){
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        VKmerBytesWritable vertexId = new VKmerBytesWritable();
        VertexValueWritable vertexValue = new VertexValueWritable();
        //add the corresponding edge to new vertex
        vertexValue.getEdgeList(connectedTable[i][0]).add(incomingEdge);
        
        vertexValue.getEdgeList(connectedTable[i][1]).add(outgoingEdge);
        
        vertexId.setAsCopy(createdVertexId);
        vertex.setVertexId(vertexId);
        vertex.setVertexValue(vertexValue);
        
        addVertex(vertexId, vertex);
    }
    
    public void sendMsgToUpdateEdge(EdgeWritable incomingEdge, EdgeWritable outgoingEdge){
        EdgeWritable createdEdge = new EdgeWritable();
        createdEdge.setKey(createdVertexId);
        for(Long readId: neighborEdgeIntersection)
            createdEdge.appendReadID(readId);
        outgoingMsg.setCreatedEdge(createdEdge);
        outgoingMsg.setSourceVertexId(getVertexId());
        
        outgoingMsg.setFlag(incomingEdgeDir);
        destVertexId.setAsCopy(incomingEdge.getKey());
        sendMsg(destVertexId, outgoingMsg);
        
        outgoingMsg.setFlag(outgoingEdgeDir);
        destVertexId.setAsCopy(outgoingEdge.getKey());
        sendMsg(destVertexId, outgoingMsg);
    }
    
    public void storeDeletedEdge(Set<DeletedEdge> deletedEdges, int i, EdgeWritable incomingEdge, EdgeWritable outgoingEdge){
        DeletedEdge deletedIncomingEdge = new DeletedEdge();
        DeletedEdge deletedOutgoingEdge = new DeletedEdge();
        
        deletedIncomingEdge.setDir(connectedTable[i][0]);
        deletedIncomingEdge.setEdge(incomingEdge);
        
        deletedOutgoingEdge.setDir(connectedTable[i][1]);
        deletedOutgoingEdge.setEdge(outgoingEdge);
        
        deletedEdges.add(deletedIncomingEdge);
        deletedEdges.add(deletedOutgoingEdge);
    }
    
    public void deleteEdgeFromOldVertex(DeletedEdge deleteEdge){
        getVertexValue().getEdgeList(deleteEdge.dir).remove(deleteEdge.getEdge());
    }
    
    public void setEdgeListAndEdgeDir(int i){
        incomingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][0]));
        incomingEdgeDir = connectedTable[i][0];
        
        outgoingEdgeList.setAsCopy(getVertexValue().getEdgeList(connectedTable[i][1]));
        outgoingEdgeDir = connectedTable[i][1];
    }
    
    public void setNeighborEdgeIntersection(EdgeWritable incomingEdge, EdgeWritable outgoingEdge){
        incomingReadIdSet.clear();
        long[] incomingReadIds = incomingEdge.getReadIDs().toReadIDArray();
        for(long readId : incomingReadIds){
            incomingReadIdSet.add(readId);
        }
        outgoingReadIdSet.clear();
        long[] outgoingReadIds = outgoingEdge.getReadIDs().toReadIDArray();
        for(long readId : outgoingReadIds){
            outgoingReadIdSet.add(readId);
        }
        neighborEdgeIntersection.clear();
        neighborEdgeIntersection.addAll(incomingReadIdSet);
        neighborEdgeIntersection.retainAll(outgoingReadIdSet);
    }
    
    public void updateEdgeListPointToNewVertex(){
        byte meToNeighborDir = incomingMsg.getFlag();
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        getVertexValue().getEdgeList(neighborToMeDir).remove(incomingMsg.getSourceVertexId());
        getVertexValue().getEdgeList(neighborToMeDir).add(incomingMsg.getCreatedEdge());
    }
    
    @Override
    public void compute(Iterator<SplitRepeatMessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            if(getVertexValue().getDegree() > 2){
                //A set storing deleted edges
                Set<DeletedEdge> deletedEdges = new HashSet<DeletedEdge>();
                /** process connectedTable **/
                for(int i = 0; i < 4; i++){
                    /** set edgeList and edgeDir based on connectedTable **/
                    setEdgeListAndEdgeDir(i);
                    
                    for(EdgeWritable incomingEdge : incomingEdgeList){
                        for(EdgeWritable outgoingEdge : outgoingEdgeList){
                            /** set neighborEdge readId intersection **/
                            setNeighborEdgeIntersection(incomingEdge, outgoingEdge);
                            
                            if(!neighborEdgeIntersection.isEmpty()){
                                /** random generate vertexId of new vertex **/
                                randomGenerateVertexId(3);
                                
                                /** create new/created vertex **/
                                createNewVertex(i, incomingEdge, outgoingEdge);
                                
                                /** send msg to neighbors to update their edges to new vertex **/
                                sendMsgToUpdateEdge(incomingEdge, outgoingEdge);
                                
                                /** store deleted edge **/
                                storeDeletedEdge(deletedEdges, i, incomingEdge, outgoingEdge);
                            }
                        }
                    }                
                }
                /** delete extra edges from old vertex **/
                for(DeletedEdge deletedEdge : deletedEdges){
                    deleteEdgeFromOldVertex(deletedEdge);
                }
                
                /** Old vertex delete or voteToHalt **/
                if(getVertexValue().getDegree() == 0)//if no any edge, delete
                    deleteVertex(getVertexId());
                else
                    voteToHalt();
            }
        } else if(getSuperstep() == 2){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                /** update edgelist to new/created vertex **/
                updateEdgeListPointToNewVertex();
            }
            voteToHalt();
        }
    } 
}
