package edu.uci.ics.genomix.pregelix.sequencefile;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

public class GenerateTextFile {

	public static void generateFromPathmergeResult() throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter("output2"));
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		for(int i = 0; i < 2; i++){
			Path path = new Path("output/part-" + i);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
			KmerBytesWritable key = new KmerBytesWritable(55);
		    ValueStateWritable value = new ValueStateWritable();
		    
		    while(reader.next(key, value)){
				if (key == null || value == null){
					break;
				}
				bw.write(key.toString()
						+ "\t" + value.toString());
				bw.newLine();
		    }
		    reader.close();
		}
		bw.close();
	}
	public static void generateSpecificLengthChainFromNaivePathmergeResult(int maxLength) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter("naive_text_" + maxLength));
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		for(int i = 0; i < 2; i++){
			Path path = new Path("/home/anbangx/genomix_result/final_naive/part-" + i);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
			KmerBytesWritable key = new KmerBytesWritable(55);
		    ValueStateWritable value = new ValueStateWritable();
		    
		    while(reader.next(key, value)){
				if (key == null || value == null){
					break;
				} 
				if(value.getLengthOfMergeChain() != -1 && value.getLengthOfMergeChain() <= maxLength){
					bw.write(value.toString());
					bw.newLine();
				}
		    }
		    reader.close();
		}
		bw.close();
	}
	
	public static void generateSpecificLengthChainFromLogPathmergeResult(int maxLength) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter("log_text_" + maxLength));
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		for(int i = 0; i < 2; i++){
			Path path = new Path("/home/anbangx/genomix_result/improvelog2/part-" + i);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
			KmerBytesWritable key = new KmerBytesWritable(55);
		    ValueStateWritable value = new ValueStateWritable();
		    
		    while(reader.next(key, value)){
				if (key == null || value == null){
					break;
				} 
				if(value.getLengthOfMergeChain() != -1 && value.getLengthOfMergeChain() <= maxLength
						&& value.getState() == State.FINAL_VERTEX){
					bw.write(key.toString()
							+ "\t" + value.toString());
					bw.newLine();
				}
		    }
		    reader.close();
		}
		bw.close();
	}
	public static void generateFromGraphbuildResult() throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter("textfile"));
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		Path path = new Path("data/input/part-0-out-3000000");
		SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
		KmerBytesWritable key = new KmerBytesWritable(55);
		KmerCountValue value = new KmerCountValue();
	    
	    while(reader.next(key, value)){
			if (key == null || value == null){
				break;
			}
			bw.write(key.toString());
			bw.newLine();
	    }
	    reader.close();
		bw.close();
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		generateFromPathmergeResult();
		//generateFromGraphbuildResult();
		//generateSpecificLengthChainFromPathmergeResult(68);
		//generateSpecificLengthChainFromLogPathmergeResult(68);
	}

}
