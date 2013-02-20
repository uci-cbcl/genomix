package edu.uci.ics.pregelix.SequenceFile;

import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.JobConf;

public class GenerateSequenceFile {
	
	static private final Path TMP_DIR = new Path(
			GenerateSequenceFile.class.getSimpleName() + "_TMP");
	
	
	
	 public static void main(String[] argv) throws Exception {
		 
	     //write output to a file
		 Configuration conf = new Configuration();
	     Path outDir = new Path(TMP_DIR, "out");
	     Path outFile = new Path(outDir, "reduce-out");
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
	     Path inFile = new Path(outDir, "reduce-out");
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
