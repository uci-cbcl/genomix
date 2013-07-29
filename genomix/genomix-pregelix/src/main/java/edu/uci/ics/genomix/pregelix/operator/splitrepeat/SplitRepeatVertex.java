package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class SplitRepeatVertex extends 
    BasicGraphCleanVertex{
    
    public class CreatedVertex{
        VKmerBytesWritable createdVertexId;
        String incomingDir;
        String outgoingDir;
        VKmerBytesWritable incomingEdge;
        VKmerBytesWritable outgoingEdge;
        
        public CreatedVertex(){
            createdVertexId = new VKmerBytesWritable(kmerSize);
            incomingDir = "";
            outgoingDir = "";
            incomingEdge = new VKmerBytesWritable(kmerSize);
            outgoingEdge = new VKmerBytesWritable(kmerSize);
        }
        
        public void clear(){
            createdVertexId.reset(kmerSize);
            incomingDir = "";
            outgoingDir = "";
            incomingEdge.reset(kmerSize);
            outgoingEdge.reset(kmerSize);
        }
        
        public VKmerBytesWritable getCreatedVertexId() {
            return createdVertexId;
        }

        public void setCreatedVertexId(KmerBytesWritable createdVertexId) {
            this.createdVertexId = createdVertexId;
        }

        public String getIncomingDir() {
            return incomingDir;
        }

        public void setIncomingDir(String incomingDir) {
            this.incomingDir = incomingDir;
        }

        public String getOutgoingDir() {
            return outgoingDir;
        }

        public void setOutgoingDir(String outgoingDir) {
            this.outgoingDir = outgoingDir;
        }

        public VKmerBytesWritable getIncomingEdge() {
            return incomingEdge;
        }

        public void setIncomingEdge(KmerBytesWritable incomingEdge) {
            this.incomingEdge.set(incomingEdge);
        }

        public VKmerBytesWritable getOutgoingEdge() {
            return outgoingEdge;
        }

        public void setOutgoingEdge(KmerBytesWritable outgoingEdge) {
            this.outgoingEdge.set(outgoingEdge);
        }
    }
    
    private String[][] connectedTable = new String[][]{
            {"FF", "RF"},
            {"FF", "RR"},
            {"FR", "RF"},
            {"FR", "RR"}
    };
    public static Set<String> existKmerString = new HashSet<String>();
    private Set<Long> readIdSet;
    private Set<Long> incomingReadIdSet = new HashSet<Long>();
    private Set<Long> outgoingReadIdSet = new HashSet<Long>();
    private Set<Long> selfReadIdSet = new HashSet<Long>();
    private Set<Long> incomingEdgeIntersection = new HashSet<Long>();
    private Set<Long> outgoingEdgeIntersection = new HashSet<Long>();
    private Set<Long> neighborEdgeIntersection = new HashSet<Long>();
    private Map<KmerBytesWritable, Set<Long>> kmerMap = new HashMap<KmerBytesWritable, Set<Long>>();
    private VKmerListWritable incomingEdgeList = null; 
    private VKmerListWritable outgoingEdgeList = null; 
    private byte incomingEdgeDir = 0;
    private byte outgoingEdgeDir = 0;
    
    protected KmerBytesWritable createdVertexId = null;  
    private CreatedVertex createdVertex = new CreatedVertex();
    public static Set<CreatedVertex> createdVertexSet = new HashSet<CreatedVertex>();
    
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
            incomingEdgeList = new VKmerListWritable(kmerSize);
        if(outgoingEdgeList == null)
            outgoingEdgeList = new VKmerListWritable(kmerSize);
        if(createdVertexId == null)
            createdVertexId = new VKmerBytesWritable(kmerSize + 1);
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
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
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
            kmerMap.clear();
            createdVertexSet.clear();
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                readIdSet = new HashSet<Long>();
                for(PositionWritable nodeId : incomingMsg.getNodeIdList()){
                    readIdSet.add(nodeId.getReadId());
                }
                kmerMap.put(incomingMsg.getSourceVertexId(), readIdSet);
            }
            /** process connectedTable **/
            for(int i = 0; i < 4; i++){
                switch(connectedTable[i][0]){
                    case "FF":
                        outgoingEdgeList.set(getVertexValue().getFFList());
                        outgoingEdgeDir = MessageFlag.DIR_FF;
                        break;
                    case "FR":
                        outgoingEdgeList.set(getVertexValue().getFRList());
                        outgoingEdgeDir = MessageFlag.DIR_FR;
                        break;
                }
                switch(connectedTable[i][1]){
                    case "RF":
                        incomingEdgeList.set(getVertexValue().getRFList());
                        incomingEdgeDir = MessageFlag.DIR_RF;
                        break;
                    case "RR":
                        incomingEdgeList.set(getVertexValue().getRRList());
                        incomingEdgeDir = MessageFlag.DIR_RR;
                        break;
                }
                selfReadIdSet.clear();
                for(PositionWritable nodeId : getVertexValue().getNodeIdList()){
                    selfReadIdSet.add(nodeId.getReadId());
                }
                for(KmerBytesWritable outgoingEdge : outgoingEdgeList){
                    for(KmerBytesWritable incomingEdge : incomingEdgeList){
                        outgoingReadIdSet.clear();
                        incomingReadIdSet.clear();
                        outgoingReadIdSet.addAll(kmerMap.get(outgoingEdge));
                        incomingReadIdSet.addAll(kmerMap.get(incomingEdge));
                        
                        //set all neighberEdge readId intersection
                        neighborEdgeIntersection.addAll(selfReadIdSet);
                        neighborEdgeIntersection.retainAll(outgoingReadIdSet);
                        neighborEdgeIntersection.retainAll(incomingReadIdSet);
                        //set outgoingEdge readId intersection
                        outgoingEdgeIntersection.addAll(selfReadIdSet);
                        outgoingEdgeIntersection.retainAll(outgoingReadIdSet);
                        outgoingEdgeIntersection.removeAll(neighborEdgeIntersection); 
                        //set incomingEdge readId intersection
                        incomingEdgeIntersection.addAll(selfReadIdSet);
                        incomingEdgeIntersection.retainAll(incomingReadIdSet);
                        incomingEdgeIntersection.removeAll(neighborEdgeIntersection);
                        
                        if(!neighborEdgeIntersection.isEmpty()){
                            createdVertex.clear();
                            createdVertexId.setByRead(generaterRandomString(kmerSize + 1).getBytes(), 0);
                            createdVertex.setCreatedVertexId(createdVertexId);
                            createdVertex.setIncomingDir(connectedTable[i][1]);
                            createdVertex.setOutgoingDir(connectedTable[i][0]);
                            createdVertex.setIncomingEdge(incomingEdge);
                            createdVertex.setOutgoingEdge(outgoingEdge);
                            createdVertexSet.add(createdVertex);
                            
                            outgoingMsg.setCreatedVertexId(createdVertex.getCreatedVertexId());
                            outgoingMsg.setSourceVertexId(getVertexId());
                            outgoingMsg.setFlag(incomingEdgeDir);
                            sendMsg(incomingEdge, outgoingMsg);
                            outgoingMsg.setFlag(outgoingEdgeDir);
                            sendMsg(outgoingEdge, outgoingMsg);
                        }
                        
                        if(!incomingEdgeIntersection.isEmpty()){
                            createdVertex.clear();
                            createdVertexId.setByRead(generaterRandomString(kmerSize + 1).getBytes(), 0);
                            createdVertex.setCreatedVertexId(createdVertexId);
                            createdVertex.setIncomingDir(connectedTable[i][1]);
                            createdVertex.setIncomingEdge(incomingEdge);
                            createdVertexSet.add(createdVertex);
                            
                            outgoingMsg.setCreatedVertexId(createdVertex.getCreatedVertexId());
                            outgoingMsg.setSourceVertexId(getVertexId());
                            outgoingMsg.setFlag(incomingEdgeDir);
                            sendMsg(incomingEdge, outgoingMsg);
                        }
                        
                        if(!outgoingEdgeIntersection.isEmpty()){
                            createdVertex.clear();
                            createdVertexId.setByRead(generaterRandomString(kmerSize + 1).getBytes(), 0);
                            createdVertex.setCreatedVertexId(createdVertexId);
                            createdVertex.setOutgoingDir(connectedTable[i][0]);
                            createdVertex.setOutgoingEdge(outgoingEdge);
                            createdVertexSet.add(createdVertex);
                            
                            outgoingMsg.setCreatedVertexId(createdVertex.getCreatedVertexId());
                            outgoingMsg.setSourceVertexId(getVertexId());
                            outgoingMsg.setFlag(outgoingEdgeDir);
                            sendMsg(outgoingEdge, outgoingMsg);
                        }
                    }
                }
            }
        } else if(getSuperstep() == 4){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                /** update edgelist to new/created vertex **/
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
                /** add new/created vertex **/
                for(CreatedVertex v : createdVertexSet){
                    Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
                    vertex.getMsgList().clear();
                    vertex.getEdges().clear();
                    VertexValueWritable vertexValue = new VertexValueWritable();
                    switch(v.incomingDir){
                        case "RF":
                            vertexValue.getRFList().append(v.incomingEdge);
                            break;
                        case "RR":
                            vertexValue.getRRList().append(v.incomingEdge);
                            break;
                    }
                    switch(v.outgoingDir){
                        case "FF":
                            vertexValue.getFFList().append(v.outgoingEdge);
                            break;
                        case "FR":
                            vertexValue.getFRList().append(v.outgoingEdge);
                            break;
                    }
                    vertex.setVertexId(v.getCreatedVertexId());
                    vertex.setVertexValue(vertexValue);
                }
                createdVertexSet.clear();
            }
        }
    }
}
