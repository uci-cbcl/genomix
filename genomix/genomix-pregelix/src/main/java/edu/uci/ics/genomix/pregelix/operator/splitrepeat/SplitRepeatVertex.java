package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.SplitRepeatMessageWritable;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class SplitRepeatVertex extends 
    BasicGraphCleanVertex<SplitRepeatMessageWritable>{
    
    public class EdgeAndDir{
        private byte dir;
        private EdgeWritable edge;
        
        public EdgeAndDir(){
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
    
    public static Set<String> existKmerString = new HashSet<String>();
    protected VKmerBytesWritable createdVertexId = null;  
    private Set<Long> incomingReadIdSet = new HashSet<Long>();
    private Set<Long> outgoingReadIdSet = new HashSet<Long>();
    private Set<Long> neighborEdgeIntersection = new HashSet<Long>();
    private EdgeWritable tmpIncomingEdge = null;
    private EdgeWritable tmpOutgoingEdge = null;

    private EdgeWritable deletedEdge = new EdgeWritable();
    private Set<EdgeAndDir> deletedEdges = new HashSet<EdgeAndDir>();//A set storing deleted edges
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
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
        if(tmpIncomingEdge == null)
            tmpIncomingEdge = new EdgeWritable();
        if(tmpOutgoingEdge == null)
            tmpOutgoingEdge = new EdgeWritable();
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
        
        vertexValue.setInternalKmer(getVertexId());
        
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
//        outgoingMsg.setSourceVertexId(getVertexId());
        deletedEdge.reset();
        deletedEdge.setKey(getVertexId());
        deletedEdge.setReadIDs(neighborEdgeIntersection);
        outgoingMsg.setDeletedEdge(deletedEdge);
        
        outgoingMsg.setFlag(incomingEdgeDir);
        destVertexId.setAsCopy(incomingEdge.getKey());
        sendMsg(destVertexId, outgoingMsg);
        
        outgoingMsg.setFlag(outgoingEdgeDir);
        destVertexId.setAsCopy(outgoingEdge.getKey());
        sendMsg(destVertexId, outgoingMsg);
    }
    
    public void storeDeletedEdge(int i, EdgeWritable incomingEdge, EdgeWritable outgoingEdge,
            Set<Long> commonReadIdSet){
        EdgeAndDir deletedIncomingEdge = new EdgeAndDir();
        EdgeAndDir deletedOutgoingEdge = new EdgeAndDir();
        
        deletedIncomingEdge.setDir(connectedTable[i][0]);
        deletedIncomingEdge.setEdge(incomingEdge);
        
        deletedOutgoingEdge.setDir(connectedTable[i][1]);
        deletedOutgoingEdge.setEdge(outgoingEdge);
        
        deletedEdges.add(deletedIncomingEdge);
        deletedEdges.add(deletedOutgoingEdge);
    }
    
    public void deleteEdgeFromOldVertex(EdgeAndDir deleteEdge){
        getVertexValue().getEdgeList(deleteEdge.dir).removeSubEdge(deleteEdge.getEdge());
    }
    
    public void updateEdgeListPointToNewVertex(){
        byte meToNeighborDir = incomingMsg.getFlag();
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        
        getVertexValue().getEdgeList(neighborToMeDir).removeSubEdge(incomingMsg.getDeletedEdge());
        getVertexValue().getEdgeList(neighborToMeDir).add(incomingMsg.getCreatedEdge());
    }
    
    @Override
    public void compute(Iterator<SplitRepeatMessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            if(getVertexValue().getDegree() > 2){
                deletedEdges.clear();
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
                                
                                /** change incomingEdge/outgoingEdge's edgeList to commondReadIdSet **/
                                tmpIncomingEdge.setAsCopy(incomingEdge);
                                tmpOutgoingEdge.setAsCopy(outgoingEdge);
                                tmpIncomingEdge.setReadIDs(neighborEdgeIntersection);
                                tmpOutgoingEdge.setReadIDs(neighborEdgeIntersection);
                                
                                /** create new/created vertex **/
                                createNewVertex(i, tmpIncomingEdge, tmpOutgoingEdge);
                                
                                /** send msg to neighbors to update their edges to new vertex **/
                                sendMsgToUpdateEdge(tmpIncomingEdge, tmpOutgoingEdge);
                                
                                /** store deleted edge **/
                                storeDeletedEdge(i, tmpIncomingEdge, tmpOutgoingEdge, neighborEdgeIntersection);
                            }
                        }
                    }                
                }
                /** delete extra edges from old vertex **/
                for(EdgeAndDir deletedEdge : deletedEdges){
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
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf) throws IOException {
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(SplitRepeatVertex.class.getSimpleName());
        else
            job = new PregelixJob(conf, SplitRepeatVertex.class.getSimpleName());
        job.setVertexClass(SplitRepeatVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
