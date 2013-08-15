package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class SimpleSplitRepeatVertex extends 
    BasicGraphCleanVertex{
    
    public class EdgeDir{
        public static final byte DIR_FF = 0 << 0;
        public static final byte DIR_FR = 1 << 0;
        public static final byte DIR_RF = 2 << 0;
        public static final byte DIR_RR = 3 << 0;
    }
    
    public class DeletedEdge{
        private byte dir;
        private VKmerBytesWritable edge;
        
        public DeletedEdge(){
            dir = 0;
            edge = new VKmerBytesWritable(kmerSize);
        }

        public byte getDir() {
            return dir;
        }

        public void setDir(byte dir) {
            this.dir = dir;
        }

        public VKmerBytesWritable getEdge() {
            return edge;
        }

        public void setEdge(VKmerBytesWritable edge) {
            this.edge.setAsCopy(edge);
        }
    }
    
    private byte[][] connectedTable = new byte[][]{
            {EdgeDir.DIR_RF, EdgeDir.DIR_FF},
            {EdgeDir.DIR_RF, EdgeDir.DIR_FR},
            {EdgeDir.DIR_RR, EdgeDir.DIR_FF},
            {EdgeDir.DIR_RR, EdgeDir.DIR_FR}
    };
    
    public static Set<String> existKmerString = new HashSet<String>();
    protected VKmerBytesWritable createdVertexId = null;  
    private Set<Long> incomingReadIdSet = new HashSet<Long>();
    private Set<Long> outgoingReadIdSet = new HashSet<Long>();
    private Set<Long> neighborEdgeIntersection = new HashSet<Long>();
    private VKmerListWritable incomingEdgeList = null; 
    private VKmerListWritable outgoingEdgeList = null; 
    private byte incomingEdgeDir = 0;
    private byte outgoingEdgeDir = 0;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
        if(incomingMsg == null)
            incomingMsg = new MessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset(kmerSize);
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
        if(incomingEdgeList == null)
            incomingEdgeList = new VKmerListWritable();
        if(outgoingEdgeList == null)
            outgoingEdgeList = new VKmerListWritable();
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
    public void createNewVertex(int i, VKmerBytesWritable incomingEdge, VKmerBytesWritable outgoingEdge){
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        VKmerBytesWritable vertexId = new VKmerBytesWritable(kmerSize);
        VertexValueWritable vertexValue = new VertexValueWritable(); //kmerSize
        //add the corresponding edge to new vertex
        switch(connectedTable[i][0]){
            case EdgeDir.DIR_RF:
                vertexValue.getRFList().append(incomingEdge);
                break;
            case EdgeDir.DIR_RR:
                vertexValue.getRRList().append(incomingEdge);
                break;
        }
        switch(connectedTable[i][1]){
            case EdgeDir.DIR_FF:
                vertexValue.getFFList().append(outgoingEdge);
                break;
            case EdgeDir.DIR_FR:
                vertexValue.getFRList().append(outgoingEdge);
                break;
        }
        vertexId.setAsCopy(createdVertexId);
        vertex.setVertexId(vertexId);
        vertex.setVertexValue(vertexValue);
        
        addVertex(vertexId, vertex);
    }
    
    public void sendMsgToUpdateEdge(VKmerBytesWritable incomingEdge, VKmerBytesWritable outgoingEdge){
        outgoingMsg.setCreatedVertexId(createdVertexId);
        outgoingMsg.setSourceVertexId(getVertexId());
        
        outgoingMsg.setFlag(incomingEdgeDir);
        destVertexId.setAsCopy(incomingEdge);
        sendMsg(destVertexId, outgoingMsg);
        
        outgoingMsg.setFlag(outgoingEdgeDir);
        destVertexId.setAsCopy(outgoingEdge);
        sendMsg(destVertexId, outgoingMsg);
    }
    
    public void storeDeletedEdge(Set<DeletedEdge> deletedEdges, int i, VKmerBytesWritable incomingEdge, VKmerBytesWritable outgoingEdge){
        DeletedEdge deletedIncomingEdge = new DeletedEdge();
        DeletedEdge deletedOutgoingEdge = new DeletedEdge();
        switch(connectedTable[i][0]){
            case EdgeDir.DIR_RF:
                deletedIncomingEdge.setDir(EdgeDir.DIR_RF);
                deletedIncomingEdge.setEdge(incomingEdge);
                break;
            case EdgeDir.DIR_RR:
                deletedIncomingEdge.setDir(EdgeDir.DIR_RR);
                deletedIncomingEdge.setEdge(incomingEdge);
                break;
        }
        switch(connectedTable[i][1]){
            case EdgeDir.DIR_FF:
                deletedOutgoingEdge.setDir(EdgeDir.DIR_FF);
                deletedOutgoingEdge.setEdge(outgoingEdge);
                break;
            case EdgeDir.DIR_FR:
                deletedOutgoingEdge.setDir(EdgeDir.DIR_FR);
                deletedOutgoingEdge.setEdge(outgoingEdge);
                break;
        }
        deletedEdges.add(deletedIncomingEdge);
        deletedEdges.add(deletedOutgoingEdge);
    }
    public void deleteEdgeFromOldVertex(DeletedEdge deleteEdge){
        switch(deleteEdge.dir){
            case EdgeDir.DIR_RF:
                getVertexValue().getRFList().remove(deleteEdge.getEdge());
                break;
            case EdgeDir.DIR_RR:
                getVertexValue().getRRList().remove(deleteEdge.getEdge());
                break;
            case EdgeDir.DIR_FF:
                getVertexValue().getFFList().remove(deleteEdge.getEdge());
                break;
            case EdgeDir.DIR_FR:
                getVertexValue().getFRList().remove(deleteEdge.getEdge());
                break;
        }
    }
    
    public void setEdgeListAndEdgeDir(int i){
        switch(connectedTable[i][0]){
            case EdgeDir.DIR_RF:
                incomingEdgeList.setCopy(getVertexValue().getRFList());
                incomingEdgeDir = MessageFlag.DIR_RF;
                break;
            case EdgeDir.DIR_RR:
                incomingEdgeList.setCopy(getVertexValue().getRRList());
                incomingEdgeDir = MessageFlag.DIR_RR;
                break;
        }
        switch(connectedTable[i][1]){
            case EdgeDir.DIR_FF:
                outgoingEdgeList.setCopy(getVertexValue().getFFList());
                outgoingEdgeDir = MessageFlag.DIR_FF;
                break;
            case EdgeDir.DIR_FR:
                outgoingEdgeList.setCopy(getVertexValue().getFRList());
                outgoingEdgeDir = MessageFlag.DIR_FR;
                break;
        }
    }
    
    public void setNeighborEdgeIntersection(VKmerBytesWritable incomingEdge, VKmerBytesWritable outgoingEdge){
//        incomingReadIdSet.clear();
//        outgoingReadIdSet.clear(); 
//        tmpKmer.setAsCopy(incomingEdge);
//        incomingReadIdSet.addAll(kmerMap.get(tmpKmer));
//        tmpKmer.setAsCopy(outgoingEdge);
//        outgoingReadIdSet.addAll(kmerMap.get(tmpKmer));
//        
//        //set all neighberEdge readId intersection
//        neighborEdgeIntersection.addAll(selfReadIdSet);
        neighborEdgeIntersection.retainAll(incomingReadIdSet);
        neighborEdgeIntersection.retainAll(outgoingReadIdSet);
    }
    
    public void updateEdgeListPointToNewVertex(){
        byte meToNeighborDir = incomingMsg.getFlag();
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
                getVertexValue().getFFList().remove(incomingMsg.getSourceVertexId());
                getVertexValue().getFFList().append(incomingMsg.getCreatedVertexId());
                break;
            case MessageFlag.DIR_FR:
                getVertexValue().getFRList().remove(incomingMsg.getSourceVertexId());
                getVertexValue().getFRList().append(incomingMsg.getCreatedVertexId());
                break;
            case MessageFlag.DIR_RF:
                getVertexValue().getRFList().remove(incomingMsg.getSourceVertexId());
                getVertexValue().getRFList().append(incomingMsg.getCreatedVertexId());
                break;
            case MessageFlag.DIR_RR:
                getVertexValue().getRRList().remove(incomingMsg.getSourceVertexId());
                getVertexValue().getRRList().append(incomingMsg.getCreatedVertexId());
                break;
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            if(getVertexValue().getDegree() > 2){
                //A set storing deleted edges
                Set<DeletedEdge> deletedEdges = new HashSet<DeletedEdge>();
                /** process connectedTable **/
                for(int i = 0; i < 4; i++){
                    /** set edgeList and edgeDir based on connectedTable **/
                    setEdgeListAndEdgeDir(i);
                    
                    for(VKmerBytesWritable incomingEdge : incomingEdgeList){
                        for(VKmerBytesWritable outgoingEdge : outgoingEdgeList){
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
        }
    }
}
