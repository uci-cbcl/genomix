package edu.uci.ics.genomix.pregelix.pathmerge;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;

public class MergePathTest {
	public static final String PATH_TO_TESTSTORE = "testcase/pathmerge/"; 
	//"genomix_result/pathmerge/new_naive";
	public static final String NAIVE_DATA_INPUT = "genomix_result/P1_nc8";//"actual/NaiveAlgorithmForMergeGraph/BinaryOutput/test";
	//"genomix_result/pathmerge/new_log";
	public static final String LOG_DATA_INPUT = "genomix_result/P2_nc8";//"actual/TwoStepLogAlgorithmForMergeGraph/BinaryOutput/test";
	public static final String TEXT_OUTPUT = PATH_TO_TESTSTORE + "textfile";
	public static final String CHAIN_OUTPUT = PATH_TO_TESTSTORE + "chain";
	
	private static int nc = 8;
	private static int kmerSize = 55;
	private static int maxLength = 102; 
	
	@Test
	public void test() throws Exception {
		FileUtils.forceMkdir(new File(PATH_TO_TESTSTORE));
		FileUtils.cleanDirectory(new File(PATH_TO_TESTSTORE));
		FileUtils.forceMkdir(new File(TEXT_OUTPUT));
		FileUtils.cleanDirectory(new File(TEXT_OUTPUT));
		FileUtils.forceMkdir(new File(CHAIN_OUTPUT));
		FileUtils.cleanDirectory(new File(CHAIN_OUTPUT));
		generateTextFromPathmergeResult(NAIVE_DATA_INPUT, TEXT_OUTPUT, "/naive");
		generateTextFromPathmergeResult(LOG_DATA_INPUT, TEXT_OUTPUT, "/log");
		//generateSpecificLengthChainFromNaivePathmergeResult(NAIVE_DATA_INPUT, CHAIN_OUTPUT, maxLength);
		//generateSpecificLengthChainFromLogPathmergeResult(LOG_DATA_INPUT, CHAIN_OUTPUT, maxLength);
	} 
	
	public static void generateTextFromPathmergeResult(String input, String outputDir, String fileName) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(outputDir + fileName)));
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		for(int i = 0; i < nc; i++){
			Path path = new Path(input + "/part-" + i);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
			KmerBytesWritable key = new KmerBytesWritable(kmerSize);
		    ValueStateWritable value = new ValueStateWritable();
		    
		    while(reader.next(key, value)){
				if (key == null || value == null){
					break;
				}
				//if(value.getState() == State.FINAL_VERTEX){
					/*bw.write(value.getMergeChain().toString()
							+ "\t" + GeneCode.getSymbolFromBitMap(value.getAdjMap()));
					bw.newLine();*/
					bw.write(key.toString()
							+ "\t" + value.toString());
					bw.newLine();
				//}
				//if(value.getLengthOfMergeChain() != 0
				//		&& value.getLengthOfMergeChain() != -1
				//		&& value.getState() == State.FINAL_VERTEX){
					//bw.write(key.toString() + "\t" + 
					//	value.toString());
					//bw.write(value.getLengthOfMergeChain() + "\t" +
					//		value.getMergeChain().toString() + "\t" +
					//		GeneCode.getSymbolFromBitMap(value.getAdjMap()) + "\t" +
					//		key.toString());
							//value.getState());
					
				//}
		    }
		    reader.close();
		}
		bw.close();
	}
	
	public static void generateSpecificLengthChainFromNaivePathmergeResult(String input, String output, int maxLength) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output + "/naive")));
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		for(int i = 0; i < nc; i++){
			Path path = new Path(input + "/part-" + i);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
			KmerBytesWritable key = new KmerBytesWritable(kmerSize);
		    ValueStateWritable value = new ValueStateWritable();
		    
		    while(reader.next(key, value)){
				if (key == null || value == null){
					break;
				} 
				if(value.getLengthOfMergeChain() != -1 && value.getLengthOfMergeChain() <= maxLength
						&& value.getLengthOfMergeChain() != kmerSize){
					bw.write(value.getLengthOfMergeChain() + "\t" +
							value.getMergeChain().toString());
					bw.newLine();
				}
		    }
		    reader.close();
		}
		bw.close();
	}
	
	public static void generateSpecificLengthChainFromLogPathmergeResult(String input, String output, int maxLength) throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(output + "/log")));
		Configuration conf = new Configuration();
		FileSystem fileSys = FileSystem.get(conf);
		for(int i = 0; i < nc; i++){
			Path path = new Path(input + "/part-" + i);
			SequenceFile.Reader reader = new SequenceFile.Reader(fileSys, path, conf);
			KmerBytesWritable key = new KmerBytesWritable(kmerSize);
		    ValueStateWritable value = new ValueStateWritable();
		    
		    while(reader.next(key, value)){
				if (key == null || value == null){
					break;
				} 
				if(value.getLengthOfMergeChain() != -1 && value.getLengthOfMergeChain() <= maxLength
						&& value.getState() == State.FINAL_VERTEX){
					bw.write(value.getLengthOfMergeChain() + "\t" +
							value.getMergeChain().toString());
					bw.newLine();
				}
		    }
		    reader.close();
		}
		bw.close();
	}
}
