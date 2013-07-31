package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class SplitRepeatVertex extends 
    BasicGraphCleanVertex{
    
    public class EdgeDir{
        public static final byte DIR_FF = 0 << 0;
        public static final byte DIR_FR = 1 << 0;
        public static final byte DIR_RF = 2 << 0;
        public static final byte DIR_RR = 3 << 0;
    }
    
    public class DeletedEdge{
        private byte dir;
        private KmerBytesWritable edge;
        
        public DeletedEdge(){
            dir = 0;
            edge = new KmerBytesWritable(kmerSize);
        }

        public byte getDir() {
            return dir;
        }

        public void setDir(byte dir) {
            this.dir = dir;
        }

        public KmerBytesWritable getEdge() {
            return edge;
        }

        public void setEdge(KmerBytesWritable edge) {
            this.edge.set(edge);
        }
    }
    
    private byte[][] connectedTable = new byte[][]{
            {EdgeDir.DIR_RF, EdgeDir.DIR_FF},
            {EdgeDir.DIR_RF, EdgeDir.DIR_FR},
            {EdgeDir.DIR_RR, EdgeDir.DIR_FF},
            {EdgeDir.DIR_RR, EdgeDir.DIR_FR}
    };
    public static Set<String> existKmerString = new HashSet<String>();
    private Set<Long> readIdSet;
    private Set<Long> incomingReadIdSet = new HashSet<Long>();
    private Set<Long> outgoingReadIdSet = new HashSet<Long>();
    private Set<Long> selfReadIdSet = new HashSet<Long>();
    private Set<Long> neighborEdgeIntersection = new HashSet<Long>();
    private Map<KmerBytesWritable, Set<Long>> kmerMap = new HashMap<KmerBytesWritable, Set<Long>>();
    private KmerListWritable incomingEdgeList = null; 
    private KmerListWritable outgoingEdgeList = null; 
    private byte incomingEdgeDir = 0;
    private byte outgoingEdgeDir = 0;
    
    protected KmerBytesWritable createdVertexId = null;  
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(incomingMsg == null)
            incomingMsg = new MessageWritable(kmerSize);
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable(kmerSize);
        else
            outgoingMsg.reset(kmerSize);
        if(incomingEdgeList == null)
            incomingEdgeList = new KmerListWritable(kmerSize);
        if(outgoingEdgeList == null)
            outgoingEdgeList = new KmerListWritable(kmerSize);
        if(createdVertexId == null)
            createdVertexId = new KmerBytesWritable(kmerSize);//kmerSize + 1
        if(destVertexId == null)
            destVertexId = new KmerBytesWritable(kmerSize);
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
    
    /**
     * GenerateString only for test
     */
    public String generateString(){
        if(existKmerString.isEmpty()){
            existKmerString.add("AAA");
            return "AAA";
        }
        else
            return "GGG";
    }
    
    public void generateKmerMap(Iterator<MessageWritable> msgIterator){
        kmerMap.clear();
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            readIdSet = new HashSet<Long>();
            for(PositionWritable nodeId : incomingMsg.getNodeIdList()){
                readIdSet.add(nodeId.getReadId());
            }
            kmerMap.put(incomingMsg.getSourceVertexId(), readIdSet);
        }
    }
    
    public void setSelfReadIdSet(){
        selfReadIdSet.clear();
        for(PositionWritable nodeId : getVertexValue().getNodeIdList()){
            selfReadIdSet.add(nodeId.getReadId());
        }    
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void createNewVertex(int i, KmerBytesWritable incomingEdge, KmerBytesWritable outgoingEdge){
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        KmerBytesWritable vertexId = new KmerBytesWritable(kmerSize);
        VertexValueWritable vertexValue = new VertexValueWritable(kmerSize);
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
        vertexId.set(createdVertexId);
        vertex.setVertexId(vertexId);
        vertex.setVertexValue(vertexValue);
        
        addVertex(vertexId, vertex);
    }
    
    public void sendMsgToUpdateEdge(KmerBytesWritable incomingEdge, KmerBytesWritable outgoingEdge){
        outgoingMsg.setCreatedVertexId(createdVertexId);
        outgoingMsg.setSourceVertexId(getVertexId());
        
        outgoingMsg.setFlag(incomingEdgeDir);
        destVertexId.set(incomingEdge);
        sendMsg(destVertexId, outgoingMsg);
        
        outgoingMsg.setFlag(outgoingEdgeDir);
        destVertexId.set(outgoingEdge);
        sendMsg(destVertexId, outgoingMsg);
    }
    
    public void storeDeletedEdge(Set<DeletedEdge> deletedEdges, int i, KmerBytesWritable incomingEdge, KmerBytesWritable outgoingEdge){
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
                incomingEdgeList.set(getVertexValue().getRFList());
                incomingEdgeDir = MessageFlag.DIR_RF;
                break;
            case EdgeDir.DIR_RR:
                incomingEdgeList.set(getVertexValue().getRRList());
                incomingEdgeDir = MessageFlag.DIR_RR;
                break;
        }
        switch(connectedTable[i][1]){
            case EdgeDir.DIR_FF:
                outgoingEdgeList.set(getVertexValue().getFFList());
                outgoingEdgeDir = MessageFlag.DIR_FF;
                break;
            case EdgeDir.DIR_FR:
                outgoingEdgeList.set(getVertexValue().getFRList());
                outgoingEdgeDir = MessageFlag.DIR_FR;
                break;
        }
    }
    
    public void setNeighborEdgeIntersection(KmerBytesWritable incomingEdge, KmerBytesWritable outgoingEdge){
        outgoingReadIdSet.clear(); 
        incomingReadIdSet.clear();
        tmpKmer.set(incomingEdge);
        incomingReadIdSet.addAll(kmerMap.get(tmpKmer));
        tmpKmer.set(outgoingEdge);
        outgoingReadIdSet.addAll(kmerMap.get(tmpKmer));
        
        //set all neighberEdge readId intersection
        neighborEdgeIntersection.addAll(selfReadIdSet);
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
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsgToAllNeighborNodes(getVertexValue());
            }
            voteToHalt();
        } else if(getSuperstep() == 2){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                outgoingMsg.setNodeIdList(getVertexValue().getNodeIdList());
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
            }
            voteToHalt();
        } else if(getSuperstep() == 3){
            /** generate KmerMap map kmer(key) to readIdSet(value) **/
            generateKmerMap(msgIterator);
            
            /** set self readId set **/
            setSelfReadIdSet();
            
            int count = 0;
            //A set storing deleted edges
            Set<DeletedEdge> deletedEdges = new HashSet<DeletedEdge>();
            /** process connectedTable **/
            for(int i = 0; i < 4; i++){
                /** set edgeList and edgeDir based on connectedTable **/
                setEdgeListAndEdgeDir(i);
                
                KmerBytesWritable incomingEdge = new KmerBytesWritable(kmerSize);
                KmerBytesWritable outgoingEdge = new KmerBytesWritable(kmerSize);
                for(int x = 0; x < incomingEdgeList.getCountOfPosition(); x++){
                    for(int y = 0; y < outgoingEdgeList.getCountOfPosition(); y++){
                        incomingEdge.set(incomingEdgeList.getPosition(x));
                        outgoingEdge.set(outgoingEdgeList.getPosition(y));
                        /** set neighborEdge readId intersection **/
                        setNeighborEdgeIntersection(incomingEdge, outgoingEdge);
                        
                        if(!neighborEdgeIntersection.isEmpty()){
                            if(count == 0)
                                createdVertexId.setByRead("AAA".getBytes(), 0);//kmerSize + 1 generaterRandomString(kmerSize).getBytes()
                            else
                                createdVertexId.setByRead("GGG".getBytes(), 0);
                            count++;
                            
                            /** create new/created vertex **/
                            createNewVertex(i, incomingEdge, outgoingEdge);
                            
                            /** send msg to neighbors to update their edges to new vertex **/
                            sendMsgToUpdateEdge(incomingEdge, outgoingEdge);
                            
                            /** store deleted edge **/
                            storeDeletedEdge(deletedEdges, i, incomingEdge, outgoingEdge);
                        }
                    }
                }
                
//                for(KmerBytesWritable incomingEdge : incomingEdgeList){
//                    for(KmerBytesWritable outgoingEdge : outgoingEdgeList){
//                        /** set neighborEdge readId intersection **/
//                        setNeighborEdgeIntersection(incomingEdge, outgoingEdge);
//                        
//                        if(!neighborEdgeIntersection.isEmpty()){
//                            if(count == 0)
//                                createdVertexId.setByRead("AAA".getBytes(), 0);//kmerSize + 1 generaterRandomString(kmerSize).getBytes()
//                            else
//                                createdVertexId.setByRead("GGG".getBytes(), 0);
//                            count++;
//                            
//                            /** create new/created vertex **/
//                            createNewVertex(i, incomingEdge, outgoingEdge);
//                            
//                            /** send msg to neighbors to update their edges to new vertex **/
//                            sendMsgToUpdateEdge(incomingEdge, outgoingEdge);
//                            
//                            /** store deleted edge **/
//                            storeDeletedEdge(deletedEdges, i, incomingEdge, outgoingEdge);
//                        }
//                    }
//                }
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
        } else if(getSuperstep() == 4){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                /** update edgelist to new/created vertex **/
                updateEdgeListPointToNewVertex();
            }
            voteToHalt();
        }
    }
    
    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(SplitRepeatVertex.class.getSimpleName());
        job.setVertexClass(SplitRepeatVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
