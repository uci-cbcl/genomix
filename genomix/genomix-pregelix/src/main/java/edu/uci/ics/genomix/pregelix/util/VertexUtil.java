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
        return value.outDegree() > 0 && !isPathVertex(value);
    }

    /**
     * Rear Vertex: in-degree > 0,
     * 
     * @param vertexValue
     */
    public static boolean isRearVertex(ValueStateWritable value) {
        return value.inDegree() > 0 && !isPathVertex(value);
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
}
