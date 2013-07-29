package edu.uci.ics.genomix.pregelix.operator.splitrepeat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class SplitRepeatVertex extends 
    BasicGraphCleanVertex{
    
    public class CreatedVertex{
        KmerBytesWritable createdVertexId;
        String incomingDir;
        String outgoingDir;
        KmerBytesWritable incomingEdge;
        KmerBytesWritable outgoingEdge;
        
        public CreatedVertex(){
            createdVertexId = new KmerBytesWritable(kmerSize);
            incomingDir = "";
            outgoingDir = "";
            incomingEdge = new KmerBytesWritable(kmerSize);
            outgoingEdge = new KmerBytesWritable(kmerSize);
        }
        
        public void clear(){
            createdVertexId.reset(kmerSize);
            incomingDir = "";
            outgoingDir = "";
            incomingEdge.reset(kmerSize);
            outgoingEdge.reset(kmerSize);
        }
        
        public KmerBytesWritable getCreatedVertexId() {
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

        public KmerBytesWritable getIncomingEdge() {
            return incomingEdge;
        }

        public void setIncomingEdge(KmerBytesWritable incomingEdge) {
            this.incomingEdge.set(incomingEdge);
        }

        public KmerBytesWritable getOutgoingEdge() {
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
    private Set<Long> readIdSet = new HashSet<Long>();
    private Set<Long> incomingReadIdSet = new HashSet<Long>();
    private Set<Long> outgoingReadIdSet = new HashSet<Long>();
    private Set<Long> selfReadIdSet = new HashSet<Long>();
    private Set<Long> incomingEdgeIntersection = new HashSet<Long>();
    private Set<Long> outgoingEdgeIntersection = new HashSet<Long>();
    private Set<Long> neighborEdgeIntersection = new HashSet<Long>();
    private Map<KmerBytesWritable, Set<Long>> kmerMap = new HashMap<KmerBytesWritable, Set<Long>>();
    private KmerListWritable incomingEdgeList = new KmerListWritable(kmerSize);
    private KmerListWritable outgoingEdgeList = new KmerListWritable(kmerSize);
    private byte incomingEdgeDir = 0;
    private byte outgoingEdgeDir = 0;
    
    private CreatedVertex createdVertex = new CreatedVertex();
    public static Set<CreatedVertex> createdVertexSet = new HashSet<CreatedVertex>();
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
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
                readIdSet.clear();
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
                        outgoingReadIdSet = kmerMap.get(outgoingEdge);
                        incomingReadIdSet = kmerMap.get(incomingEdge);
                        
                        //set all neighberEdge readId intersection
                        neighborEdgeIntersection = selfReadIdSet;
                        neighborEdgeIntersection.retainAll(outgoingReadIdSet);
                        neighborEdgeIntersection.retainAll(incomingReadIdSet);
                        //set outgoingEdge readId intersection
                        outgoingEdgeIntersection = selfReadIdSet;
                        outgoingEdgeIntersection.retainAll(outgoingReadIdSet);
                        outgoingEdgeIntersection.removeAll(neighborEdgeIntersection);
                        //set incomingEdge readId intersection
                        incomingEdgeIntersection = selfReadIdSet;
                        incomingEdgeIntersection.retainAll(incomingReadIdSet);
                        incomingEdgeIntersection.removeAll(neighborEdgeIntersection);
                        
                        if(!neighborEdgeIntersection.isEmpty()){
                            createdVertex.clear();
                            createdVertex.setCreatedVertexId(getVertexId());
                            createdVertex.setIncomingDir(connectedTable[i][1]);
                            createdVertex.setOutgoingDir(connectedTable[i][0]);
                            createdVertex.setIncomingEdge(incomingEdge);
                            createdVertex.setOutgoingEdge(outgoingEdge);
                            createdVertexSet.add(createdVertex);
                            
                            outgoingMsg.setSourceVertexId(getVertexId());
                            outgoingMsg.setFlag(incomingEdgeDir);
                            sendMsg(incomingEdge, outgoingMsg);
                            outgoingMsg.setFlag(outgoingEdgeDir);
                            sendMsg(outgoingEdge, outgoingMsg);
                        }
                        
                        if(!incomingEdgeIntersection.isEmpty()){
                            createdVertex.clear();
                            createdVertex.setCreatedVertexId(getVertexId());
                            createdVertex.setIncomingDir(connectedTable[i][1]);
                            createdVertex.setIncomingEdge(incomingEdge);
                            createdVertexSet.add(createdVertex);
                            
                            outgoingMsg.setSourceVertexId(getVertexId());
                            outgoingMsg.setFlag(incomingEdgeDir);
                            sendMsg(incomingEdge, outgoingMsg);
                        }
                        
                        if(!outgoingEdgeIntersection.isEmpty()){
                            createdVertex.clear();
                            createdVertex.setCreatedVertexId(getVertexId());
                            createdVertex.setOutgoingDir(connectedTable[i][0]);
                            createdVertex.setOutgoingEdge(outgoingEdge);
                            createdVertexSet.add(createdVertex);
                            
                            outgoingMsg.setSourceVertexId(getVertexId());
                            outgoingMsg.setFlag(outgoingEdgeDir);
                            sendMsg(outgoingEdge, outgoingMsg);
                        }
                    }
                }
            }
        } else if(getSuperstep() == 4){
            
        }
    }
}
