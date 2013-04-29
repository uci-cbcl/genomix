package edu.uci.ics.genomix.pregelix.sequencefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

public class GenerateSmallFile {

	public static void generateNumOfLinesFromBigFile(Path inFile, Path outFile, int numOfLines) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);

		SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
	    SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, KmerBytesWritable.class, KmerCountValue.class, 
	         CompressionType.NONE);
	    KmerBytesWritable outKey = new KmerBytesWritable(55);
	    KmerCountValue outValue = new KmerCountValue();
	    int i = 0;
	    
	    for(i = 0; i < numOfLines; i++){
	    	 //System.out.println(i);
	    	 reader.next(outKey, outValue);
	    	 writer.append(outKey, outValue);
	    }
	    writer.close();
	    reader.close();
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Path dir = new Path("data");
		Path inFile = new Path(dir, "part-0");
		Path outFile = new Path(dir, "part-0-out-20000000");
		generateNumOfLinesFromBigFile(inFile,outFile,20000000);
	}

}
