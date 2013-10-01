package edu.uci.ics.genomix.hadoop.tp.graphclean.type;

import edu.uci.ics.genomix.type.NodeWritable;


public class VertexUtil {
    /**
     * Single Vertex: in-degree = out-degree = 1
     * 
     * @param vertexValue
     */
    public static boolean isPathVertex(NodeWritable node) {
        return node.inDegree() == 1 && node.outDegree() == 1;
    }

    /**
     * Head Vertex: out-degree > 0
     */
    public static boolean isHead(NodeWritable node){
        return node.outDegree() > 0 && !isPathVertex(node);
    }
    
//    public static boolean isHeadOrRearVertexWithDegree(VertexValueWritable value){
//        return isHeadVertexWithIndegree(value) || isRearVertexWithOutdegree(value);
//    }
    /**
     * Head Vertex: out-degree > 0, and has indegress
     * 
     * @param vertexValue
     */
    public static boolean isHeadVertexWithIndegree(NodeWritable node) {
        return isHead(node) && !isHeadWithoutIndegree(node);
    }

    /**
     * Head Vertex without indegree: indegree = 0, outdegree = 1
     */
    public static boolean isHeadWithoutIndegree(NodeWritable value){
        return value.inDegree() == 0 && value.outDegree() == 1;
    }
    
    /**
     * Head Vertex: out-degree > 0
     */
//    public static boolean isRear(VertexValueWritable value){
//        return value.inDegree() > 0 && !isPathVertex(value);
//    }
    
    /**
     * Rear Vertex: in-degree > 0, and has outdegree
     * 
     * @param vertexValue
     */
//    public static boolean isRearVertexWithOutdegree(VertexValueWritable value) {
//        return isRear(value) && !isRearWithoutOutdegree(value);
//    }

    
    /**
     * Rear Vertex without outdegree: indegree = 1, outdegree = 0
     */
//    public static boolean isRearWithoutOutdegree(VertexValueWritable value){
//        return value.inDegree() == 1 && value.outDegree() == 0;
//    }
    
    /**
     * check if mergeChain is cycle
     */
//    public static boolean isCycle(VKmerBytesWritable kmer, VKmerBytesWritable mergeChain, int kmerSize) {
//        String chain = mergeChain.toString().substring(1);
//        return chain.contains(kmer.toString());

        /*subKmer.set(vertexId);
        for(int istart = 1; istart < mergeChain.getKmerLength() - kmerSize + 1; istart++){
        	byte nextgene = mergeChain.getGeneCodeAtPosition(istart+kmerSize);
        	subKmer.shiftKmerWithNextCode(nextgene);
        	if(subKmer.equals(vertexId))
            	return true;
        }
        return false;*/
//    }
    
    /**
     * check if vertex is a tip
     */
    public static boolean isIncomingTipVertex(NodeWritable node){
    	return node.inDegree() == 0 && node.outDegree() == 1;
    }
    
    public static boolean isOutgoingTipVertex(NodeWritable node){
    	return node.inDegree() == 1 && node.outDegree() == 0;
    }
    
    /**
     * check if vertex is single
     */
    public static boolean isSingleVertex(NodeWritable node){
        return node.inDegree() == 0 && node.outDegree() == 0;
    }
    
    /**
     * check if vertex is upbridge
     */
    public static boolean isUpBridgeVertex(NodeWritable value){
        return value.inDegree() == 1 && value.outDegree() > 1;
    }
    
    /**
     * check if vertex is downbridge
     */
    public static boolean isDownBridgeVertex(NodeWritable value){
        return value.inDegree() > 1 && value.outDegree() == 1;
    }
    
    /**
     * get nodeId from Ad
     */
/*    public static VKmerBytesWritable getNodeIdFromAdjacencyList(AdjacencyListWritable adj){
        if(adj.getForwardList().getCountOfPosition() > 0)
            return adj.getForwardList().getPosition(0);
        else if(adj.getReverseList().getCountOfPosition() > 0)
            return adj.getReverseList().getPosition(0);
        else
            return null;
    }*/
    public static boolean isVertexWithOnlyOneIncoming(NodeWritable value){
        return value.inDegree() == 1 && !isPathVertex(value);
    }
    
    public static boolean isVertexWithOnlyOneOutgoing(NodeWritable value){
        return value.outDegree() == 1 && !isPathVertex(value);
    }
    
    public static boolean isVertexWithManyIncoming(NodeWritable value){
        return value.inDegree() > 1;
    }
    
    public static boolean isVertexWithManyOutgoing(NodeWritable value){
        return value.outDegree() > 1;
    }
}
