package edu.uci.ics.genomix.pregelix;

import org.apache.hadoop.io.BytesWritable;

import edu.uci.ics.genomix.type.old.Kmer;
import edu.uci.ics.genomix.type.old.KmerUtil;

public class GraphVertexOperation {
	
	/**
	 *  generate the valid data(byte[]) from BytesWritable
	 */
	public static byte[] generateValidDataFromBytesWritable(BytesWritable bw){
		byte[] wholeBytes = bw.getBytes();
		int validNum = bw.getLength();
		byte[] validBytes = new byte[validNum];
		for(int i = 0; i < validNum; i++)
			validBytes[i] = wholeBytes[i];
		return validBytes;
	}
	/**
	 * Single Vertex: in-degree = out-degree = 1
	 * @param vertexValue 
	 */
	public static boolean isPathVertex(byte value){
		if(KmerUtil.inDegree(value) == 1 && KmerUtil.outDegree(value) == 1)
			return true;
		return false;
	}
	/** 
	 * Head Vertex:  out-degree > 0, 
	 * @param vertexValue 
	 */
	public static boolean isHeadVertex(byte value){
		if(KmerUtil.outDegree(value) > 0 && !isPathVertex(value))
			return true;
		return false;
	}
	/**
	 * Rear Vertex:  in-degree > 0, 
	 * @param vertexValue 
	 */
	public static boolean isRearVertex(byte value){
		if(KmerUtil.inDegree(value) > 0 && !isPathVertex(value))
			return true;
		return false;
	}
	/**
	 * update right neighber based on next vertexId
	 */
	public static byte updateRightNeighberByVertexId(byte oldVertexValue, byte[] neighberVertexId, int k){
		
		String neighberVertex = Kmer.recoverKmerFrom(k, neighberVertexId, 0, neighberVertexId.length);
		
		byte newBit = Kmer.GENE_CODE.getAdjBit((byte)neighberVertex.charAt(neighberVertex.length() - 1));
		return (byte) ((byte)(oldVertexValue & 0xF0) | (byte) (newBit & 0x0F));
	}
	/**
	 * update right neighber
	 */
	public static byte updateRightNeighber(byte oldVertexValue, byte newVertexValue){
		return (byte) ((byte)(oldVertexValue & 0xF0) | (byte) (newVertexValue & 0x0F));
	}
}
