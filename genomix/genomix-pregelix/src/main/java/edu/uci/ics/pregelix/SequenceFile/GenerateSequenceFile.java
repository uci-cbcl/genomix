package edu.uci.ics.pregelix.SequenceFile;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.pregelix.GraphVertexOperation;
import edu.uci.ics.pregelix.bitwise.BitwiseOperation;
import edu.uci.ics.pregelix.type.KmerCountValue;

public class GenerateSequenceFile {
	
	static private final Path TMP_DIR = new Path(
			GenerateSequenceFile.class.getSimpleName() + "_TMP");
	private static Path outDir = new Path("data/webmap");
	private final static int k = 3;
	
	/**
	 * create test.dat
	 * A - ACG - A		000110	00010001	06	11
	 * C - ACT - C		000111	00100010	07	22
	 * G - CGT - G		011011	01000100	1B	44
	 * T - GTC - T		101101	10001000	2D	88
	 */
	public static void createTestDat() throws IOException{
		 //write output to a file
		 Configuration conf = new Configuration();
	     Path outFile = new Path(outDir, "test-out.dat");
	     FileSystem fileSys = FileSystem.get(conf);
	     SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, BytesWritable.class, ByteWritable.class, 
	         CompressionType.NONE);
	     

		 //Generate <key,value>  <BytesWritable, ByteWritable>
		 byte[] key = hexStringToByteArray("06"); //000110
		 byte[] value = hexStringToByteArray("11"); //00010001
		 System.out.println(Integer.toHexString(key[0]));
		 System.out.println(Integer.toHexString(value[0]));
		 BytesWritable keyWritable = new BytesWritable(key);
		 ByteWritable valueWritable = new ByteWritable(value[0]);
	     
	     ArrayList<BytesWritable> arrayOfKeys = new ArrayList<BytesWritable>();
	     arrayOfKeys.add(keyWritable);
	     ArrayList<ByteWritable> arrayOfValues = new ArrayList<ByteWritable>();
	     arrayOfValues.add(valueWritable);
	     
	     key = hexStringToByteArray("07"); //000111
	     value = hexStringToByteArray("22"); //00100010
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value[0]);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     key = hexStringToByteArray("1B"); //011010
	     value = hexStringToByteArray("44"); //01000100
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value[0]);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     key = hexStringToByteArray("2D"); //100011
	     value = hexStringToByteArray("88"); //10001000
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value[0]);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     //wirte to sequence file
	     for(int i = 0; i < arrayOfKeys.size(); i++)
	    	 writer.append(arrayOfKeys.get(i), arrayOfValues.get(i));
	     writer.close();
	     
	     //read outputs
	     Path inFile = new Path(outDir, "test-out.dat");
	     BytesWritable outKey = new BytesWritable();
	     ByteWritable outValue = new ByteWritable();
	     SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
	     try {
	        reader.next(outKey, outValue);
		     System.out.println(outKey.getBytes());
		     System.out.println(outValue.get());
	     } finally {
	       reader.close();
	     }
	}
	
    /**
     * create a mergeTest SequenceFile
     * CAG - AGC - GCG - CGT - GTA - TAT - ATA 
     * GAG 								   ATC	
     * 
     * CAG	010010	00000010	
     * AGC	001001	01100100	
     * GCG	100110	00011000
     * CGT	011011	01000001
     * GTA	101100	00101000
     * TAT	110011	01000011
     * ATA	001100	10000000
     * GAG	100010	00000010
     * ATC	001101	10000000
     */
	public static void createMergeTest() throws IOException{
		 //write output to a file
		 Configuration conf = new Configuration();
	     Path outFile = new Path(outDir, "sequenceFileMergeTest");
	     FileSystem fileSys = FileSystem.get(conf);
	     SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, BytesWritable.class, KmerCountValue.class, 
	         CompressionType.NONE);
	     

		 //Generate <key,value>  <BytesWritable, ByteWritable>
	     // 1
	     String tmpKey = "010010";
		 byte[] key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
		 String tmpValue = "00000010";
		 byte value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
		 BytesWritable keyWritable = new BytesWritable(key);
		 ByteWritable valueWritable = new ByteWritable(value);
	     
	     ArrayList<BytesWritable> arrayOfKeys = new ArrayList<BytesWritable>();
	     arrayOfKeys.add(keyWritable);
	     ArrayList<ByteWritable> arrayOfValues = new ArrayList<ByteWritable>();
	     arrayOfValues.add(valueWritable);
	     
	     // 2
	     tmpKey = "001001";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "01100100";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // 3
	     tmpKey = "100110";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "00011000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // 4
	     tmpKey = "011011";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "01000001";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // 5
	     tmpKey = "101100";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "00101000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // 6
	     tmpKey = "110011";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "01000011";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);

	     // 7
	     tmpKey = "001100";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "10000000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // 8
	     tmpKey = "100010";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "00000010";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // 9
	     tmpKey = "001101";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "10000000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     KmerCountValue kmerCountValue = null;
	     //wirte to sequence file
	     for(int i = 0; i < arrayOfKeys.size(); i++){
	    	 kmerCountValue = new KmerCountValue();
	    	 kmerCountValue.setAdjBitMap(arrayOfValues.get(i).get());
	    	 writer.append(arrayOfKeys.get(i), kmerCountValue);
	     }
	     writer.close();
	     
	     //read outputs
	     Path inFile = new Path(outDir, "sequenceFileMergeTest");
	     BytesWritable outKey = new BytesWritable();
	     KmerCountValue outValue = new KmerCountValue();
	     SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
	     int iteration = 1;
	     try {
	         while(reader.next(outKey, outValue)){
	        	 System.out.println(iteration);
			     System.out.println("key: " + BitwiseOperation.convertBytesToBinaryStringKmer(outKey.getBytes(),k));
			     System.out.println("value: " + BitwiseOperation.convertByteToBinaryString(outValue.getAdjBitMap()));
			     System.out.println();
			     iteration++;
	         }
	     } finally {
	       reader.close();
	     }
	}
	
	/**
     * create a mergeTest SequenceFile
     * CAG - AGC - GCG - CGT - GTA - TAT - ATA 
     * GAG 								   ATC	
     * 
     */
	public static void createLongMergeTest() throws IOException{
		 //write output to a file
		 Configuration conf = new Configuration();
	     Path outFile = new Path(outDir, "sequenceFileMergeTest");
	     FileSystem fileSys = FileSystem.get(conf);
	     SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, BytesWritable.class, KmerCountValue.class, 
	         CompressionType.NONE);
	     

		 //Generate <key,value>  <BytesWritable, ByteWritable>
	     // CAG
	     String tmpKey = "010010";
		 byte[] key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
		 String tmpValue = "00000010";
		 byte value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
		 BytesWritable keyWritable = new BytesWritable(key);
		 ByteWritable valueWritable = new ByteWritable(value);
	     
	     ArrayList<BytesWritable> arrayOfKeys = new ArrayList<BytesWritable>();
	     arrayOfKeys.add(keyWritable);
	     ArrayList<ByteWritable> arrayOfValues = new ArrayList<ByteWritable>();
	     arrayOfValues.add(valueWritable);
	     
	     // AGC
	     tmpKey = "001001";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "01100001";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // GAG
	     tmpKey = "100010";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "00000010";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // TAT
	     tmpKey = "110011";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "00100011";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);

	     // ATA
	     tmpKey = "001100";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "10000000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // ATC
	     tmpKey = "001101";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "10000000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     KmerCountValue kmerCountValue = null;
	     //wirte to sequence file
	     for(int i = 0; i < arrayOfKeys.size(); i++){
	    	 kmerCountValue = new KmerCountValue();
	    	 kmerCountValue.setAdjBitMap(arrayOfValues.get(i).get());
	    	 writer.append(arrayOfKeys.get(i), kmerCountValue);
	     }
	     writer.close();
	     
	     //read outputs
	     Path inFile = new Path(outDir, "sequenceFileMergeTest");
	     BytesWritable outKey = new BytesWritable();
	     KmerCountValue outValue = new KmerCountValue();
	     SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
	     int iteration = 1;
	     try {
	         while(reader.next(outKey, outValue)){
	        	 System.out.println(iteration);
			     System.out.println("key: " + BitwiseOperation.convertBytesToBinaryStringKmer(outKey.getBytes(),k));
			     System.out.println("value: " + BitwiseOperation.convertByteToBinaryString(outValue.getAdjBitMap()));
			     System.out.println();
			     iteration++;
	         }
	     } finally {
	       reader.close();
	     }
	}
	
	public static void generateNumOfLinesFromBigFile(Path inFile, Path outFile, int numOfLines) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
	    SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
	    SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, BytesWritable.class, KmerCountValue.class, 
	         CompressionType.NONE);
	    BytesWritable outKey = new BytesWritable();
	    KmerCountValue outValue = new KmerCountValue();
	    int i = 0;
	    
	    for(i = 0; i < numOfLines; i++){
	    	 System.out.println(i);
	    	 reader.next(outKey, outValue);
	    	 writer.append(outKey, outValue);
	    }
	    writer.close();
	    reader.close();
	}
	
	 public static void main(String[] argv) throws Exception {
		 //createTestDat();
		 //createMergeTest();
		 //createTestDat();
		/* Path dir = new Path("data/webmap");
		 Path inFile = new Path(dir, "part-1");
		 Path outFile = new Path(dir, "part-1-out");
		 generateNumOfLinesFromBigFile(inFile,outFile,10000);*/
		 /**
		  * AGC - A		C - TAT
		  * "AGCAAACACGAC T TGCC TAT"
		  *  problem "AGCATGGACGTCGATTCTAT"
		  *  "AGCACTTAT"
		  *  "AGCAAACACTTGCTGTACCGTGGCCTAT"
		  */
		 generateSequenceFileFromGeneCode("AGCATGCGGGTCTAT");//GTCGATT  //before T: GGACG
	 }
	 public static void generateSequenceFileFromGeneCode(String s) throws IOException{
		 Configuration conf = new Configuration();
	     Path outFile = new Path(outDir, "sequenceFileMergeTest4");
	     FileSystem fileSys = FileSystem.get(conf);
	     SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, BytesWritable.class, KmerCountValue.class, 
	         CompressionType.NONE);
		 BytesWritable outKey = null;
	     KmerCountValue outValue;
	     byte adjBitMap; 
	     ArrayList<String> lists = new ArrayList<String>();
	     lists.add("010010"); //CAG
	     lists.add("100010"); //GAG
	     lists.add("001001"); //AGC
	     lists.add("110011"); //TAT
	     lists.add("001100"); //ATA
	     lists.add("001101"); //ATC
	     String binaryString = "";
		 for(int i = 1; i < s.length()-k; i++){
			 binaryString = GraphVertexOperation.convertGeneCodeToBinaryString(s.substring(i,i+k));
			 if(lists.contains(binaryString)){
				 System.out.println("error: " + binaryString);
				 return;
			 }
			 lists.add(binaryString);
			 outKey = new BytesWritable(BitwiseOperation.convertBinaryStringToBytes(binaryString));
			 outValue = new KmerCountValue();
			 adjBitMap = GraphVertexOperation.getPrecursorFromGeneCode((byte)0, s.charAt(i-1));
			 adjBitMap = GraphVertexOperation.getSucceedFromGeneCode(adjBitMap, s.charAt(i+k));
			 outValue.setAdjBitMap(adjBitMap);
			 writer.append(outKey, outValue);
		 }
		 /**
		  *  CAG - AGC ------ TAT - ATA
		  *  GAG 					ATC
		  */
		 // CAG
	     String tmpKey = "010010";
		 byte[] key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
		 String tmpValue = "00000010";
		 byte value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
		 BytesWritable keyWritable = new BytesWritable(key);
		 ByteWritable valueWritable = new ByteWritable(value);
	     
	     ArrayList<BytesWritable> arrayOfKeys = new ArrayList<BytesWritable>();
	     arrayOfKeys.add(keyWritable);
	     ArrayList<ByteWritable> arrayOfValues = new ArrayList<ByteWritable>();
	     arrayOfValues.add(valueWritable);
	     
	     // AGC
	     tmpKey = "001001";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "01100001";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // GAG
	     tmpKey = "100010";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "00000010";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     // TAT
	     tmpKey = "110011";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "00100011";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);

	     // ATA
	     tmpKey = "001100";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "10000000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     // ATC
	     tmpKey = "001101";
	     key = BitwiseOperation.convertBinaryStringToBytes(tmpKey);
	     tmpValue = "10000000";
	     value = BitwiseOperation.convertBinaryStringToByte(tmpValue);
	     keyWritable = new BytesWritable(key);
	     valueWritable = new ByteWritable(value);
	     arrayOfKeys.add(keyWritable);
	     arrayOfValues.add(valueWritable);
	     
	     KmerCountValue kmerCountValue = null;
	     //wirte to sequence file
	     for(int i = 0; i < arrayOfKeys.size(); i++){
	    	 kmerCountValue = new KmerCountValue();
	    	 kmerCountValue.setAdjBitMap(arrayOfValues.get(i).get());
	    	 writer.append(arrayOfKeys.get(i), kmerCountValue);
	     }
	     writer.close();
	 }
	 public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
}
