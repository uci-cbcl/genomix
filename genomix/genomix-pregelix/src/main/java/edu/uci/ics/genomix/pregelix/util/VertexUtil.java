package edu.uci.ics.genomix.pregelix.util;

import edu.uci.ics.genomix.pregelix.io.AdjacencyListWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class VertexUtil {
    /**
     * Single Vertex: in-degree = out-degree = 1
     * 
     * @param vertexValue
     */
    public static boolean isPathVertex(VertexValueWritable value) {
        return value.inDegree() == 1 && value.outDegree() == 1;
    }

    /**
     * Head Vertex: out-degree > 0
     */
    public static boolean isHead(VertexValueWritable value){
        return value.outDegree() > 0 && !isPathVertex(value);
    }
    
    public static boolean isHeadOrRearVertexWithDegree(VertexValueWritable value){
        return isHeadVertexWithIndegree(value) || isRearVertexWithOutdegree(value);
    }
    /**
     * Head Vertex: out-degree > 0, and has indegress
     * 
     * @param vertexValue
     */
    public static boolean isHeadVertexWithIndegree(VertexValueWritable value) {
        return isHead(value) && !isHeadWithoutIndegree(value);
    }

    /**
     * Head Vertex without indegree: indegree = 0, outdegree = 1
     */
    public static boolean isHeadWithoutIndegree(VertexValueWritable value){
        return value.inDegree() == 0 && value.outDegree() == 1;
    }
    
    /**
     * Head Vertex: out-degree > 0
     */
    public static boolean isRear(VertexValueWritable value){
        return value.inDegree() > 0 && !isPathVertex(value);
    }
    
    /**
     * Rear Vertex: in-degree > 0, and has outdegree
     * 
     * @param vertexValue
     */
    public static boolean isRearVertexWithOutdegree(VertexValueWritable value) {
        return isRear(value) && !isRearWithoutOutdegree(value);
    }

    
    /**
     * Rear Vertex without outdegree: indegree = 1, outdegree = 0
     */
    public static boolean isRearWithoutOutdegree(VertexValueWritable value){
        return value.inDegree() == 1 && value.outDegree() == 0;
    }
    
    /**
     * check if mergeChain is cycle
     */
    public static boolean isCycle(KmerBytesWritable kmer, KmerBytesWritable mergeChain, int kmerSize) {
        String chain = mergeChain.toString().substring(1);
        return chain.contains(kmer.toString());

        /*subKmer.set(vertexId);
        for(int istart = 1; istart < mergeChain.getKmerLength() - kmerSize + 1; istart++){
        	byte nextgene = mergeChain.getGeneCodeAtPosition(istart+kmerSize);
        	subKmer.shiftKmerWithNextCode(nextgene);
        	if(subKmer.equals(vertexId))
            	return true;
        }
        return false;*/
    }
    
    /**
     * check if vertex is a tip
     */
    public static boolean isIncomingTipVertex(VertexValueWritable value){
    	return value.inDegree() == 0 && value.outDegree() == 1;
    }
    
    public static boolean isOutgoingTipVertex(VertexValueWritable value){
    	return value.inDegree() == 1 && value.outDegree() == 0;
    }
    
    /**
     * check if vertex is single
     */
    public static boolean isSingleVertex(VertexValueWritable value){
        return value.inDegree() == 0 && value.outDegree() == 0;
    }
    
    /**
     * check if vertex is upbridge
     */
    public static boolean isUpBridgeVertex(VertexValueWritable value){
        return value.inDegree() == 1 && value.outDegree() > 1;
    }
    
    /**
     * check if vertex is downbridge
     */
    public static boolean isDownBridgeVertex(VertexValueWritable value){
        return value.inDegree() > 1 && value.outDegree() == 1;
    }
    
    /**
     * get nodeId from Ad
     */
    public static PositionWritable getNodeIdFromAdjacencyList(AdjacencyListWritable adj){
        if(adj.getForwardList().getCountOfPosition() > 0)
            return adj.getForwardList().getPosition(0);
        else if(adj.getReverseList().getCountOfPosition() > 0)
            return adj.getReverseList().getPosition(0);
        else
            return null;
    }
}
