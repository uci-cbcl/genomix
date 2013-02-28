package edu.uci.ics.pregelix;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.pregelix.SequenceFile.GenerateSequenceFile;
import edu.uci.ics.pregelix.bitwise.BitwiseOperation;

public class GraphVertexOperation {
	public static final int k = 3; //kmer, k: the length of kmer
	static private final Path TMP_DIR = new Path(
			GenerateSequenceFile.class.getSimpleName() + "_INTERIM");
	/**
	 * Single Vertex: in-degree = out-degree = 1
	 * @param vertexValue 
	 */
	public static boolean isPathVertex(ByteWritable vertexValue){
		byte value = vertexValue.get();
		byte[] bit = new byte[8];
		for(int i = 0; i < 8; i++)
			bit[i] = (byte) ((value >> i) & 0x01);
		
		//check out-degree
		if(((bit[0]==1)&&(bit[1]==0)&&(bit[2]==0)&&(bit[3]==0))
				|| ((bit[0]==0)&&(bit[1]==1)&&(bit[2]==0)&&(bit[3]==0))
				|| ((bit[0]==0)&&(bit[1]==0)&&(bit[2]==1)&&(bit[3]==0))
				|| ((bit[0]==0)&&(bit[1]==0)&&(bit[2]==0)&&(bit[3]==1))
				){
			//check in-degree
			if(((bit[4]==1)&&(bit[5]==0)&&(bit[6]==0)&&(bit[7]==0))
					|| ((bit[4]==0)&&(bit[5]==1)&&(bit[6]==0)&&(bit[7]==0))
					|| ((bit[4]==0)&&(bit[5]==0)&&(bit[6]==1)&&(bit[7]==0))
					|| ((bit[4]==0)&&(bit[5]==0)&&(bit[6]==0)&&(bit[7]==1))
					)
				return true;
			else
				return false;
		}
		else
			return false;
	}
	/**
	 * Head Vertex:  out-degree = 1, in-degree != 1
	 * @param vertexValue 
	 */
	public static boolean isHead(ByteWritable vertexValue){
		byte value = vertexValue.get();
		byte[] bit = new byte[8];
		for(int i = 0; i < 8; i++)
			bit[i] = (byte) ((value >> i) & 0x01);
		
		//check out-degree
		if(((bit[0]==1)&&(bit[1]==0)&&(bit[2]==0)&&(bit[3]==0))
				|| ((bit[0]==0)&&(bit[1]==1)&&(bit[2]==0)&&(bit[3]==0))
				|| ((bit[0]==0)&&(bit[1]==0)&&(bit[2]==1)&&(bit[3]==0))
				|| ((bit[0]==0)&&(bit[1]==0)&&(bit[2]==0)&&(bit[3]==1))
				){
			//check in-degree
			if(!((bit[4]==1)&&(bit[5]==0)&&(bit[6]==0)&&(bit[7]==0))
					&& !((bit[4]==0)&&(bit[5]==1)&&(bit[6]==0)&&(bit[7]==0))
					&& !((bit[4]==0)&&(bit[5]==0)&&(bit[6]==1)&&(bit[7]==0))
					&& !((bit[4]==0)&&(bit[5]==0)&&(bit[6]==0)&&(bit[7]==1))
					)
				return true;
			else
				return false;
		}
		else
			return false;
	}
	/**
	 * Rear Vertex:  out-degree != 1, in-degree = 1
	 * @param vertexValue 
	 */
	public static boolean isRear(ByteWritable vertexValue){
		byte value = vertexValue.get();
		byte[] bit = new byte[8];
		for(int i = 0; i < 8; i++)
			bit[i] = (byte) ((value >> i) & 0x01);
		
		//check out-degree
		if(!((bit[0]==1)&&(bit[1]==0)&&(bit[2]==0)&&(bit[3]==0))
				&& !((bit[0]==0)&&(bit[1]==1)&&(bit[2]==0)&&(bit[3]==0))
				&& !((bit[0]==0)&&(bit[1]==0)&&(bit[2]==1)&&(bit[3]==0))
				&& !((bit[0]==0)&&(bit[1]==0)&&(bit[2]==0)&&(bit[3]==1))
				){
			//check in-degree
			if(((bit[4]==1)&&(bit[5]==0)&&(bit[6]==0)&&(bit[7]==0))
					|| ((bit[4]==0)&&(bit[5]==1)&&(bit[6]==0)&&(bit[7]==0))
					|| ((bit[4]==0)&&(bit[5]==0)&&(bit[6]==1)&&(bit[7]==0))
					|| ((bit[4]==0)&&(bit[5]==0)&&(bit[6]==0)&&(bit[7]==1))
					)
				return true;
			else
				return false;
		}
		else
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
			return 0;
		else{
			tmp = value & second;
			if(tmp != 0)
				return 1;
			else{
				tmp = value & third;
				if(tmp != 0)
					return 2;
				else{
					tmp = value & fourth;
					if(tmp != 0)
						return 3;
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
		String binaryStringVertexId = BitwiseOperation.convertBytesToBinaryStringKmer(vertexId, 3);
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
	 * find the vertexId of the destination node
	 */
	public static byte[] getDestVertexId(byte[] sourceVertexId, byte vertexValue){
		byte[] destVertexId = BitwiseOperation.shiftBitsLeft(sourceVertexId, 2);
		return replaceLastTwoBits(destVertexId, findSucceedNode(vertexValue));
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
		return BitwiseOperation.convertBinaryStringToBytes(originalVertexId.substring(lengthOfChainVertex-1-k+1,lengthOfChainVertex-1));
	}
	/**
	 * read vertexId from RecordReader
	 */
	public static BytesWritable readVertexIdFromRecordReader(BytesWritable currentKey){
		String finalBinaryString = BitwiseOperation.convertBytesToBinaryStringKmer(currentKey.getBytes(),k);
		return new BytesWritable(BitwiseOperation.convertBinaryStringToBytes(finalBinaryString));
	}
}
