package edu.uci.ics.genomix.pregelix.util;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

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
    
    public static boolean isHeadVertexWithOnlyOneOutgoing(VertexValueWritable value){
        return isHead(value) && value.outDegree() == 1;
    }
    
    public static boolean isHeadVertexWithManyOutgoing(VertexValueWritable value){
        return isHead(value) && value.outDegree() > 1;
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
    

    public static boolean isRearVertexWithOnlyOneIncoming(VertexValueWritable value){
        return isRear(value) && value.inDegree() == 1;
    }
    
    public static boolean isRearVertexWithManyIncoming(VertexValueWritable value){
        return isRear(value) && value.inDegree() > 1;
    }
    
    /**
     * check if mergeChain is cycle
     */
    public static boolean isCycle(VKmerBytesWritable kmer, VKmerBytesWritable mergeChain, int kmerSize) {
        String chain = mergeChain.toString().substring(1);
        return chain.contains(kmer.toString());
    }
    
    /**
     * check if vertex is a tip
     */
    public static boolean isTipVertex(VertexValueWritable value){
        return isIncomingTipVertex(value) || isOutgoingTipVertex(value); 
    }
    
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
     * check if vertex is a valid head
     * valid head = 1. path node || 2. only one outgoing + no path node
     */
    public static boolean isValidHead(VertexValueWritable value){
        return isPathVertex(value) || (value.outDegree() == 1 && !isPathVertex(value));
    }
    
    /**
     * check if vertex is a valid Rear
     * valid head = 1. path node || 2. only one incoming + no path node
     */
    public static boolean isValidRear(VertexValueWritable value){
        return isPathVertex(value) || (value.inDegree() == 1 && !isPathVertex(value));
    }
    
    /** 
     * check condition for starting to send msg 
     * @param value
     * @return
     */
    public static boolean isVertexWithOnlyOneIncoming(VertexValueWritable value){
        return value.inDegree() == 1 && !isPathVertex(value);
    }
    
    public static boolean isVertexWithOnlyOneOutgoing(VertexValueWritable value){
        return value.outDegree() == 1 && !isPathVertex(value);
    }
    
    public static boolean isVertexWithManyIncoming(VertexValueWritable value){
        return value.inDegree() > 1;
    }
    
    public static boolean isVertexWithManyOutgoing(VertexValueWritable value){
        return value.outDegree() > 1;
    }
    
    // head or path
    public static boolean isCanMergeVertex(VertexValueWritable value){
        return value.inDegree() == 1 || value.outDegree() == 1;
    }
    
    // non-head and non-path
    public static boolean isUnMergeVertex(VertexValueWritable value){
        return !isCanMergeVertex(value);
    }
    
    /**
     * check if the vertex is bubble
     */
    public static boolean isBubbleVertex(VertexValueWritable value){
        return value.inDegree() > 0 && value.outDegree() > 0;
    }
    
    /**
     * check if the vertex is major or minor
     */

}
