package edu.uci.ics.genomix.pregelix.sequencefile;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import edu.uci.ics.genomix.pregelix.GraphVertexOperation;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerCountValue;


public class CombineSequenceFile {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		
		Path p = new Path("data/ThreeKmer");
		Path p2 = new Path("data/result");
		Path outFile = new Path(p2, "output"); 
		SequenceFile.Reader reader;
	    SequenceFile.Writer writer = SequenceFile.createWriter(fileSys, conf,
	         outFile, BytesWritable.class, KmerCountValue.class, 
	         CompressionType.NONE);
	    BytesWritable key = new BytesWritable();
	    KmerCountValue value = new KmerCountValue();
	    
	    File dir = new File("data/ThreeKmer");
		for(File child : dir.listFiles()){
			String name = child.getAbsolutePath();
			Path inFile = new Path(p, name);
			reader = new SequenceFile.Reader(fileSys, inFile, conf);
			while (reader.next(key, value)) {
				System.out.println(Kmer.recoverKmerFrom(GraphVertexOperation.k, key.getBytes(), 0,
						key.getLength())
						+ "\t" + value.toString());
				writer.append(key, value);
			}
			reader.close();
		}
		writer.close();
		System.out.println();
		
		reader = new SequenceFile.Reader(fileSys, outFile, conf);
		while (reader.next(key, value)) {
			System.err.println(Kmer.recoverKmerFrom(GraphVertexOperation.k, key.getBytes(), 0,
					key.getLength())
					+ "\t" + value.toString());
		}
		reader.close();
	}

}
