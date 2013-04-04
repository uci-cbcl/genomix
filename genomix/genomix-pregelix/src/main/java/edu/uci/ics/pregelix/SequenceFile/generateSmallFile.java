package edu.uci.ics.pregelix.SequenceFile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.genomix.type.KmerCountValue;

public class generateSmallFile {

	public static void generateNumOfLinesFromBigFile(Path inFile, Path outFile, int numOfLines) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
	    
		//ClassLoader ctxLoader = Thread.currentThread().getContextClassLoader();
		//Thread.currentThread().setContextClassLoader(GenerateSequenceFile.class.getClassLoader());
		SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, inFile, conf);
	    SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, BytesWritable.class, KmerCountValue.class, 
	         CompressionType.NONE);
	    BytesWritable outKey = new BytesWritable();
	    KmerCountValue outValue = new KmerCountValue();
	    int i = 0;
	    
	    for(i = 0; i < numOfLines; i++){
	    	 //System.out.println(i);
	    	 reader.next(outKey, outValue);
	    	 writer.append(outKey, outValue);
	    }
	    writer.close();
	    reader.close();
	    //Thread.currentThread().setContextClassLoader(ctxLoader);
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Path dir = new Path("data/webmap");
		Path inFile = new Path(dir, "part-1");
		Path outFile = new Path(dir, "part-1-out-20000000");
		generateNumOfLinesFromBigFile(inFile,outFile,20000000);
	}

}
