package edu.uci.ics.genomix.pregelix.JobGen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.operator.LoadGraphVertex;
import edu.uci.ics.genomix.pregelix.operator.ThreeStepLogAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.NaiveAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.TwoStepLogAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;


public class JobGenerator {

    private static String outputBase = "src/test/resources/jobs/";
    private static String HDFS_INPUTPATH = "/webmap";
    private static String HDFS_OUTPUTPAH = "/result";
    
    private static void generateLoadGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(LoadGraphVertex.class);
    	job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genLoadGraph() throws IOException {
    	generateLoadGraphJob("LoadGraph", outputBase + "LoadGraph.xml");
    }
    
    private static void generateMergeGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(NaiveAlgorithmForPathMergeVertex.class);
    	job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setInt(NaiveAlgorithmForPathMergeVertex.KMER_SIZE, 5);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genMergeGraph() throws IOException {
    	generateMergeGraphJob("MergeGraph", outputBase + "MergeGraph.xml");
    }
    
    private static void generateThreeStepLogAlgorithmForMergeGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(ThreeStepLogAlgorithmForPathMergeVertex.class);
        job.setVertexInputFormatClass(LogAlgorithmForPathMergeInputFormat.class); 
        job.setVertexOutputFormatClass(LogAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setInt(ThreeStepLogAlgorithmForPathMergeVertex.KMER_SIZE, 5);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genThreeStepLogAlgorithmForMergeGraph() throws IOException {
    	generateThreeStepLogAlgorithmForMergeGraphJob("ThreeStepLogAlgorithmForMergeGraph", outputBase + "ThreeStepLogAlgorithmForMergeGraph.xml");
    }
    
    private static void generateTwoStepLogAlgorithmForMergeGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(TwoStepLogAlgorithmForPathMergeVertex.class);
        job.setVertexInputFormatClass(LogAlgorithmForPathMergeInputFormat.class); 
        job.setVertexOutputFormatClass(LogAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().setInt(TwoStepLogAlgorithmForPathMergeVertex.KMER_SIZE, 5);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genTwoStepLogAlgorithmForMergeGraph() throws IOException {
    	generateTwoStepLogAlgorithmForMergeGraphJob("TwoStepLogAlgorithmForMergeGraph", outputBase + "TwoStepLogAlgorithmForMergeGraph.xml");
    }
    
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//genLoadGraph();
		//genMergeGraph();
		genThreeStepLogAlgorithmForMergeGraph();
		//genTwoStepLogAlgorithmForMergeGraph();
	}

}
