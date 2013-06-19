package edu.uci.ics.genomix.pregelix.util;

import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class VertexUtil {
    /**
     * Single Vertex: in-degree = out-degree = 1
     * 
     * @param vertexValue
     */
    public static boolean isPathVertex(ValueStateWritable value) {
        return value.inDegree() == 1 && value.outDegree() == 1;
    }

    /**
     * Head Vertex: out-degree > 0,
     * 
     * @param vertexValue
     */
    public static boolean isHeadVertex(ValueStateWritable value) {
        return value.outDegree() > 0 && !isPathVertex(value) && !isHeadWithoutIndegree(value);
    }

    /**
     * Rear Vertex: in-degree > 0,
     * 
     * @param vertexValue
     */
    public static boolean isRearVertex(ValueStateWritable value) {
        return value.inDegree() > 0 && !isPathVertex(value) && !isRearWithoutOutdegree(value);
    }

    /**
     * Head Vertex without indegree: indegree = 0, outdegree = 1
     */
    public static boolean isHeadWithoutIndegree(ValueStateWritable value){
        return value.inDegree() == 0 && value.outDegree() == 1;
    }
    
    /**
     * Rear Vertex without outdegree: indegree = 1, outdegree = 0
     */
    public static boolean isRearWithoutOutdegree(ValueStateWritable value){
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
    public static boolean isIncomingTipVertex(ValueStateWritable value){
    	return value.inDegree() == 0 && value.outDegree() == 1;
    }
    
    public static boolean isOutgoingTipVertex(ValueStateWritable value){
    	return value.inDegree() == 1 && value.outDegree() == 0;
    }
    
    /**
     * check if vertex is single
     */
    public static boolean isSingleVertex(ValueStateWritable value){
        return value.inDegree() == 0 && value.outDegree() == 0;
    }
    
    /**
     * check if vertex is upbridge
     */
    public static boolean isUpBridgeVertex(ValueStateWritable value){
        return value.inDegree() == 1 && value.outDegree() > 1;
    }
    
    /**
     * check if vertex is downbridge
     */
    public static boolean isDownBridgeVertex(ValueStateWritable value){
        return value.inDegree() > 1 && value.outDegree() == 1;
    }
}
