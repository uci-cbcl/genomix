package edu.uci.ics.genomix.pregelix.util;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class VertexUtil {
    public static VKmerBytesWritable subKmer = new VKmerBytesWritable(0);

    /**
     * Single Vertex: in-degree = out-degree = 1
     * 
     * @param vertexValue
     */
    public static boolean isPathVertex(byte value) {
        if (GeneCode.inDegree(value) == 1 && GeneCode.outDegree(value) == 1)
            return true;
        return false;
    }

    /**
     * Head Vertex: out-degree > 0,
     * 
     * @param vertexValue
     */
    public static boolean isHeadVertex(byte value) {
        if (GeneCode.outDegree(value) > 0 && !isPathVertex(value))
            return true;
        return false;
    }

    /**
     * Rear Vertex: in-degree > 0,
     * 
     * @param vertexValue
     */
    public static boolean isRearVertex(byte value) {
        if (GeneCode.inDegree(value) > 0 && !isPathVertex(value))
            return true;
        return false;
    }

    /**
     * update right neighber based on next vertexId
     */
    public static byte updateRightNeighberByVertexId(byte oldVertexValue, KmerBytesWritable neighberVertex, int k) {
        byte geneCode = neighberVertex.getGeneCodeAtPosition(k - 1);

        byte newBit = GeneCode.getBitMapFromGeneCode(geneCode); //getAdjBit
        return (byte) ((byte) (oldVertexValue & 0xF0) | (byte) (newBit & 0x0F));
    }

    /**
     * update right neighber
     */
    public static byte updateRightNeighber(byte oldVertexValue, byte newVertexValue) {
        return (byte) ((byte) (oldVertexValue & 0xF0) | (byte) (newVertexValue & 0x0F));
    }

    /**
     * check if mergeChain is cycle
     */
    public static boolean isCycle(KmerBytesWritable vertexId, VKmerBytesWritable mergeChain, int kmerSize) {
        String chain = mergeChain.toString().substring(1);
        if (chain.contains(vertexId.toString()))
            return true;
        return false;

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
     * reverse neighber
     */
    public static byte reverseAdjMap(byte oldAdjMap, byte geneCode) {
        return (byte) ((oldAdjMap & 0xF0) | (GeneCode.getBitMapFromGeneCode(geneCode) & 0x0F));
    }
}
