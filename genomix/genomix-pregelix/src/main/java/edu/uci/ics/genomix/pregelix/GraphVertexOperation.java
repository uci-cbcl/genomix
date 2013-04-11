package edu.uci.ics.genomix.pregelix;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.genomix.pregelix.bitwise.BitwiseOperation;
import edu.uci.ics.genomix.pregelix.io.LogAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.sequencefile.GenerateSequenceFile;

public class GraphVertexOperation {
	public static final int k = 5; //kmer, k: the length of kmer
	public static final int numBytes = (GraphVertexOperation.k-1)/4 + 1;
	static private final Path TMP_DIR = new Path(
			GenerateSequenceFile.class.getSimpleName() + "_INTERIM");
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
	 * Head Vertex:  in-degree != 1, out-degree = 1, 
	 * @param vertexValue 
	 */
	public static boolean isHead(byte value){
		if(KmerUtil.inDegree(value) != 1 && KmerUtil.outDegree(value) == 1)
			return true;
		return false;
	}
	/**
	 * Rear Vertex:  in-degree = 1, out-degree != 1, 
	 * @param vertexValue 
	 */
	public static boolean isRear(byte value){
		if(KmerUtil.inDegree(value) == 1 && KmerUtil.outDegree(value) != 1)
			return true;
		return false;
	}
	/**
	 * write Kmer to Sequence File for test
	 * @param arrayOfKeys
	 * @param arrayOfValues
	 * @param step
	 * @throws IOException
	 */
	public void writeKmerToSequenceFile(ArrayList<BytesWritable> arrayOfKeys, ArrayList<ByteWritable> arrayOfValues, long step) throws IOException{
		
		Configuration conf = new Configuration();
	    Path outDir = new Path(TMP_DIR, "out");
	    Path outFile = new Path(outDir, "B" + Long.toString(step));
	    FileSystem fileSys = FileSystem.get(conf);
	    SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	        outFile, BytesWritable.class, ByteWritable.class, 
	        CompressionType.NONE);
	    
	     //wirte to sequence file
	     for(int i = 0; i < arrayOfKeys.size(); i++)
	    	 writer.append(arrayOfKeys.get(i), arrayOfValues.get(i));
	     writer.close();
	}
	/**
	 * check what kind of succeed node
	 * return 0:A 1:C 2:G 3:T 4:nothing
	 */
	public static int findSucceedNode(byte vertexValue){
		String firstBit = "00000001"; //A
		String secondBit = "00000010"; //C
		String thirdBit = "00000100"; //G
		String fourthBit = "00001000"; //T
		int first = BitwiseOperation.convertBinaryStringToByte(firstBit) & 0xff;
		int second = BitwiseOperation.convertBinaryStringToByte(secondBit) & 0xff;
		int third = BitwiseOperation.convertBinaryStringToByte(thirdBit) & 0xff;
		int fourth = BitwiseOperation.convertBinaryStringToByte(fourthBit) & 0xff;
		int value = vertexValue & 0xff;
		int tmp = value & first;
		if(tmp != 0)
			return Kmer.GENE_CODE.A;
		else{
			tmp = value & second;
			if(tmp != 0)
				return Kmer.GENE_CODE.C;
			else{
				tmp = value & third;
				if(tmp != 0)
					return Kmer.GENE_CODE.G;
				else{
					tmp = value & fourth;
					if(tmp != 0)
						return Kmer.GENE_CODE.T;
					else
						return 4;
				}
			}
		}
	}
	/**
	 * check what kind of precursor node
	 * return 0:A 1:C 2:G 3:T 4:nothing
	 */
	public static int findPrecursorNode(byte vertexValue){
		String firstBit = "00010000"; //A
		String secondBit = "00100000"; //C
		String thirdBit = "01000000"; //G
		String fourthBit = "10000000"; //T
		int first = BitwiseOperation.convertBinaryStringToByte(firstBit) & 0xff;
		int second = BitwiseOperation.convertBinaryStringToByte(secondBit) & 0xff;
		int third = BitwiseOperation.convertBinaryStringToByte(thirdBit) & 0xff;
		int fourth = BitwiseOperation.convertBinaryStringToByte(fourthBit) & 0xff;
		int value = vertexValue & 0xff;
		int tmp = value & first;
		if(tmp != 0)
			return Kmer.GENE_CODE.A;
		else{
			tmp = value & second;
			if(tmp != 0)
				return Kmer.GENE_CODE.C;
			else{
				tmp = value & third;
				if(tmp != 0)
					return Kmer.GENE_CODE.G;
				else{
					tmp = value & fourth;
					if(tmp != 0)
						return Kmer.GENE_CODE.T;
					else
						return 4;
				}
			}
		}
	}
	/**
	 * replace last two bits based on n
	 * Ex. 01 10 00(nothing)	->	01 10 00(A)/01(C)/10(G)/11(T)		
	 */
	public static byte[] replaceLastTwoBits(byte[] vertexId, int n){
		String binaryStringVertexId = BitwiseOperation.convertBytesToBinaryStringKmer(vertexId, k);
		String resultString = "";
		for(int i = 0; i < binaryStringVertexId.length()-2; i++)
			resultString += binaryStringVertexId.charAt(i);
		switch(n){
		case 0:
			resultString += "00";
			break;
		case 1:
			resultString += "01";
			break;
		case 2:
			resultString += "10";
			break;
		case 3:
			resultString += "11";
			break;
		default:
			break;
		}
	
		return BitwiseOperation.convertBinaryStringToBytes(resultString);
	}
	/**
	 * replace first two bits based on n
	 * Ex. 01 10 00(nothing)	->	00(A)/01(C)/10(G)/11(T) 10 00	
	 */
	public static byte[] replaceFirstTwoBits(byte[] vertexId, int n){
		String binaryStringVertexId = BitwiseOperation.convertBytesToBinaryStringKmer(vertexId, k);
		String resultString = "";
		switch(n){
		case 0:
			resultString += "00";
			break;
		case 1:
			resultString += "01";
			break;
		case 2:
			resultString += "10";
			break;
		case 3:
			resultString += "11";
			break;
		default:
			break;
		}
		for(int i = 2; i < binaryStringVertexId.length(); i++)
			resultString += binaryStringVertexId.charAt(i);
		return BitwiseOperation.convertBinaryStringToBytes(resultString);
	}
	/**
	 * find the vertexId of the destination node - left neighber
	 */
	public static byte[] getDestVertexId(byte[] sourceVertexId, byte vertexValue){
		byte[] destVertexId = BitwiseOperation.shiftBitsLeft(sourceVertexId, 2);
		return replaceLastTwoBits(destVertexId, findSucceedNode(vertexValue));
	}
	/**
	 * find the vertexId of the destination node - right neighber
	 */
	public static byte[] getLeftDestVertexId(byte[] sourceVertexId, byte vertexValue){
		byte[] destVertexId = BitwiseOperation.shiftBitsRight(sourceVertexId, 2);
		return replaceFirstTwoBits(destVertexId, findPrecursorNode(vertexValue));
	}
	/**
	 * update the chain vertexId
	 */
	public static byte[] updateChainVertexId(byte[] chainVertexId, int lengthOfChainVertex, byte[] newVertexId){
		return BitwiseOperation.addLastTwoBits(chainVertexId,lengthOfChainVertex,BitwiseOperation.getLastTwoBits(newVertexId,k));
	}
	/**
	 * get the first kmer from chainVertexId
	 */
	public static byte[] getFirstKmer(byte[] chainVertexId){
		String originalVertexId = BitwiseOperation.convertBytesToBinaryString(chainVertexId);
		return BitwiseOperation.convertBinaryStringToBytes(originalVertexId.substring(0,k-1));
	}
	/**
	 * get the last kmer from chainVertexId
	 */
	public static byte[] getLastKmer(byte[] chainVertexId, int lengthOfChainVertex){
		String originalVertexId = BitwiseOperation.convertBytesToBinaryString(chainVertexId);
		return BitwiseOperation.convertBinaryStringToBytes(originalVertexId.substring(2*(lengthOfChainVertex-k),2*lengthOfChainVertex));
	}
	/**
	 * read vertexId from RecordReader
	 */
	public static BytesWritable readVertexIdFromRecordReader(BytesWritable currentKey){
		String finalBinaryString = BitwiseOperation.convertBytesToBinaryStringKmer(currentKey.getBytes(),k);
		return new BytesWritable(BitwiseOperation.convertBinaryStringToBytes(finalBinaryString));
	}
	/**
	 * merge two BytesWritable. Ex. merge two vertexId
	 */
	public static byte[] mergeTwoChainVertex(byte[] b1, int length, byte[] b2, int length2){
		String s2 = BitwiseOperation.convertBytesToBinaryString(b2).substring(2*k-2,2*length2);
		return BitwiseOperation.mergeTwoBytesArray(b1, length, BitwiseOperation.convertBinaryStringToBytes(s2), length2-k+1);
	}
	/**
	 * update right neighber
	 */
	public static byte updateRightNeighber(byte oldVertexValue, byte newVertexValue){
		return (byte) ((byte)(oldVertexValue & 0xF0) | (byte) (newVertexValue & 0x0F));
	}
	/**
	 * update right neighber based on next vertexId
	 */
	public static byte updateRightNeighberByVertexId(byte oldVertexValue, byte[] neighberVertexId){
		
		String neighberVertex = Kmer.recoverKmerFrom(GraphVertexOperation.k, neighberVertexId, 0, neighberVertexId.length);
		
		byte newBit = Kmer.GENE_CODE.getAdjBit((byte)neighberVertex.charAt(neighberVertex.length() - 1));
		return (byte) ((byte)(oldVertexValue & 0xF0) | (byte) (newBit & 0x0F));
		/*String oldVertex = BitwiseOperation.convertByteToBinaryString(oldVertexValue);
		String neighber = BitwiseOperation.convertBytesToBinaryStringKmer(neighberVertexId, k);
		String lastTwoBits = neighber.substring(2*k-2,2*k);
		if(lastTwoBits.compareTo("00") == 0)
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "0001");
		else if(lastTwoBits.compareTo("01") == 0)
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "0010");
		else if(lastTwoBits.compareTo("10") == 0)
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "0100");
		else if(lastTwoBits.compareTo("11") == 0)
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "1000");
		
		return (Byte) null;*/
	}
	/**
	 * get precursor in vertexValue from gene code
	 */
	public static byte getPrecursorFromGeneCode(byte vertexValue, char precursor){
		String oldVertex = BitwiseOperation.convertByteToBinaryString(vertexValue);
		switch(precursor){
		case 'A':
			return BitwiseOperation.convertBinaryStringToByte("0001" + oldVertex.substring(0,4));
		case 'C':
			return BitwiseOperation.convertBinaryStringToByte("0010" + oldVertex.substring(0,4));
		case 'G':
			return BitwiseOperation.convertBinaryStringToByte("0100" + oldVertex.substring(0,4));
		case 'T':
			return BitwiseOperation.convertBinaryStringToByte("1000" + oldVertex.substring(0,4));
			default:
				return (Byte) null;
		}
	}
	/**
	 * get succeed in vertexValue from gene code
	 */
	public static byte getSucceedFromGeneCode(byte vertexValue, char succeed){
		String oldVertex = BitwiseOperation.convertByteToBinaryString(vertexValue);
		switch(succeed){
		case 'A':
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "0001");
		case 'C':
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "0010");
		case 'G':
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "0100");
		case 'T':
			return BitwiseOperation.convertBinaryStringToByte(oldVertex.substring(0,4) + "1000");
			default:
				return (Byte) null;
		}
	}
	/**
	 * convert gene code to binary string
	 */
	public static String convertGeneCodeToBinaryString(String gene){
		String result = "";
		for(int i = 0; i < gene.length(); i++){
			switch(gene.charAt(i)){
			case 'A':
				result += "00";
				break;
			case 'C':
				result += "01";
				break;
			case 'G':
				result += "10";
				break;
			case 'T':
				result += "11";
				break;
				default:
				break;
			}
		}
		return result;
	}
	/**
	 * flush chainVertexId to file -- local file and hdfs file
	 * @throws IOException 
	 */
	public static void flushChainToFile(byte[] chainVertexId, int lengthOfChain, byte[] vertexId) throws IOException{
		 DataOutputStream out = new DataOutputStream(new 
                 FileOutputStream("data/ChainVertex"));
		 out.write(vertexId);
		 out.writeInt(lengthOfChain);
		 out.write(chainVertexId);
		 out.close();
		 //String srcFile = "data/ChainVertex";
		 //String dstFile = "testHDFS/output/ChainVertex";
		 //HDFSOperation.copyFromLocalFile(srcFile, dstFile);
	}
	/**
	 * convert binaryString to geneCode
	 */
	public static String convertBinaryStringToGenecode(String kmer){
		String result = "";
		for(int i = 0; i < kmer.length() ; ){
			String substring = kmer.substring(i,i+2);
			if(substring.compareTo("00") == 0)
				result += "A";
			else if(substring.compareTo("01") == 0)
				result += "C";
			else if(substring.compareTo("10") == 0)
				result += "G";
			else if(substring.compareTo("11") == 0)
				result += "T";
			i = i+2;
		}
		return result;
	}
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
	 *  output test for message communication
	 */
	public static void testMessageCommunication(OutputStreamWriter writer, long step, byte[] tmpSourceVertextId,
			byte[] tmpDestVertexId, MessageWritable tmpMsg){
		//test
    	String kmer = BitwiseOperation.convertBytesToBinaryStringKmer(
    			tmpSourceVertextId,GraphVertexOperation.k);
    	try {
    		writer.write("Step: " + step + "\r\n");
			writer.write("Source Key: " + kmer + "\r\n");
		
        	writer.write("Source Code: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(kmer) + "\r\n");
        	writer.write("Send Message to: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(
		    				BitwiseOperation.convertBytesToBinaryStringKmer(
		    						tmpDestVertexId,GraphVertexOperation.k)) + "\r\n");
        	writer.write("Chain Message: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(
		    						BitwiseOperation.convertBytesToBinaryString(
		    								tmpMsg.getChainVertexId())) + "\r\n");
        	writer.write("Chain Length: " + tmpMsg.getLengthOfChain() + "\r\n"); 
        	writer.write("\r\n");
    	} catch (IOException e) { e.printStackTrace(); }
    	return;
	}
	/**
	 *  output test for message communication
	 */
	public static void testMessageCommunication2(OutputStreamWriter writer, long step, byte[] tmpSourceVertextId,
			byte[] tmpDestVertexId, LogAlgorithmMessageWritable tmpMsg, byte[] myownId){
		//test
    	String kmer = BitwiseOperation.convertBytesToBinaryStringKmer(
    			tmpSourceVertextId,GraphVertexOperation.k);
    	try {
    		writer.write("Step: " + step + "\r\n");
			writer.write("Source Key: " + kmer + "\r\n");
		
        	writer.write("Source Code: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(kmer) + "\r\n");
        	writer.write("Send Message to: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(
		    				BitwiseOperation.convertBytesToBinaryStringKmer(
		    						tmpDestVertexId,GraphVertexOperation.k)) + "\r\n");
        	if(tmpMsg.getLengthOfChain() != 0){
        		writer.write("Chain Message: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(
		    						BitwiseOperation.convertBytesToBinaryString(
		    								tmpMsg.getChainVertexId())) + "\r\n");
        		writer.write("Chain Length: " + tmpMsg.getLengthOfChain() + "\r\n"); 
        	}
        	if(myownId != null)
        		writer.write("My own Id is: " + 
        				GraphVertexOperation.convertBinaryStringToGenecode(
	    						BitwiseOperation.convertBytesToBinaryStringKmer(
	    								myownId,GraphVertexOperation.k)) + "\r\n");
        	if(tmpMsg.getMessage() != 0)
        		writer.write("Message is: " + tmpMsg.getMessage() + "\r\n");
        	writer.write("\r\n");
    	} catch (IOException e) { e.printStackTrace(); }
    	return;
	}
	/**
	 *  output test for last message communication -- flush
	 */
	public static void testLastMessageCommunication(OutputStreamWriter writer, long step, byte[] tmpVertextId,
			byte[] tmpSourceVertextId,  MessageWritable tmpMsg){
		String kmer = BitwiseOperation.convertBytesToBinaryStringKmer(
    			tmpVertextId,GraphVertexOperation.k);
    	try {
    		writer.write("Step: " + step + "\r\n");
    		writer.write("Over!" + "\r\n");
			writer.write("Source Key: " + kmer + "\r\n");
			
        	writer.write("Source Code: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(kmer) + "\r\n");
        
        	writer.write("Flush Chain Message: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(
		    						BitwiseOperation.convertBytesToBinaryString(
		    								tmpMsg.getChainVertexId())) + "\r\n");
        	writer.write("Chain Length: " + tmpMsg.getLengthOfChain() + "\r\n"); 
        	writer.write("\r\n");
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
	 *  output test for log message communication
	 */
	public static void testLogMessageCommunication(OutputStreamWriter writer, long step, byte[] tmpSourceVertextId,
			byte[] tmpDestVertexId, LogAlgorithmMessageWritable tmpMsg){
		//test
    	String kmer = BitwiseOperation.convertBytesToBinaryStringKmer(
    			tmpSourceVertextId,GraphVertexOperation.k);
    	try {
    		writer.write("Step: " + step + "\r\n");
			writer.write("Source Key: " + kmer + "\r\n");
		
        	writer.write("Source Code: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(kmer) + "\r\n");
        	writer.write("Send Message to: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(
		    				BitwiseOperation.convertBytesToBinaryStringKmer(
		    						tmpDestVertexId,GraphVertexOperation.k)) + "\r\n");
        	writer.write("Message is: " +
        			tmpMsg.getMessage() + "\r\n");
        	writer.write("\r\n");
    	} catch (IOException e) { e.printStackTrace(); }
    	return;
	}	
	/**
	 *  test set vertex state
	 */
	public static void testSetVertexState(OutputStreamWriter writer, long step,byte[] tmpSourceVertextId,
			byte[] tmpDestVertexId, LogAlgorithmMessageWritable tmpMsg, ValueStateWritable tmpVal){
		//test
    	String kmer = BitwiseOperation.convertBytesToBinaryStringKmer(
    			tmpSourceVertextId,GraphVertexOperation.k);
    	try {
    		writer.write("Step: " + step + "\r\n");
			writer.write("Source Key: " + kmer + "\r\n");
		
        	writer.write("Source Code: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(kmer) + "\r\n");
        	if(tmpDestVertexId != null && tmpMsg != null){
	        	writer.write("Send Message to: " + 
			    		GraphVertexOperation.convertBinaryStringToGenecode(
			    				BitwiseOperation.convertBytesToBinaryStringKmer(
			    						tmpDestVertexId,GraphVertexOperation.k)) + "\r\n");
	        	writer.write("Message is: " +
	        			tmpMsg.getMessage() + "\r\n");
        	}
        	writer.write("Set vertex state to " +
        			tmpVal.getState() + "\r\n");
        	writer.write("\r\n");

    	} catch (IOException e) { e.printStackTrace(); }
    	return;
	}
	/**
	 *  test delete vertex information
	 */
	public static void testDeleteVertexInfo(OutputStreamWriter writer, long step, byte[] vertexId, String reason){
		try {
    		writer.write("Step: " + step + "\r\n");
			writer.write(reason + "\r\n");
			writer.write("delete " + BitwiseOperation.convertBytesToBinaryStringKmer(vertexId, GraphVertexOperation.k) 
				 + "\t" + GraphVertexOperation.convertBinaryStringToGenecode(
		    				BitwiseOperation.convertBytesToBinaryStringKmer(
		    						vertexId,GraphVertexOperation.k)) + "\r\n");
			writer.write("\r\n");
    	} catch (IOException e) { e.printStackTrace(); }
    	return;
	}
	/**
	 *  test voteToHalt vertex information
	 */
	public static void testVoteVertexInfo(OutputStreamWriter writer, long step, byte[] vertexId, String reason){
		try {
    		writer.write("Step: " + step + "\r\n");
			writer.write(reason + "\r\n");
			writer.write("voteToHalt " + BitwiseOperation.convertBytesToBinaryStringKmer(vertexId, GraphVertexOperation.k) 
				 + "\t" + GraphVertexOperation.convertBinaryStringToGenecode(
		    				BitwiseOperation.convertBytesToBinaryStringKmer(
		    						vertexId,GraphVertexOperation.k)) + "\r\n");
			writer.write("\r\n");
    	} catch (IOException e) { e.printStackTrace(); }
    	return;
	}
	/**
	 *  test merge chain vertex
	 */
	public static void testMergeChainVertex(OutputStreamWriter writer, long step, byte[] mergeChain,
			int lengthOfChain){
		try {
    		writer.write("Step: " + step + "\r\n");
        	writer.write("Merge Chain: " + 
		    		GraphVertexOperation.convertBinaryStringToGenecode(
		    						BitwiseOperation.convertBytesToBinaryString(
		    								mergeChain)) + "\r\n");
        	writer.write("Chain Length: " + lengthOfChain + "\r\n");
			writer.write("\r\n");
    	} catch (IOException e) { e.printStackTrace(); }
    	return;
	}
}
