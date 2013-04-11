package edu.uci.ics.genomix.pregelix.JobGen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.genomix.pregelix.LoadGraphVertex;
import edu.uci.ics.genomix.pregelix.format.BinaryLoadGraphInputFormat;
import edu.uci.ics.genomix.pregelix.format.BinaryLoadGraphOutputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForMergeGraphInputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForMergeGraphOutputFormat;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.LogAlgorithmForMergeGraphVertex;
import edu.uci.ics.genomix.pregelix.MergeGraphVertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;


public class JobGenerator {

    private static String outputBase = "src/test/resources/jobs/";
    private static String HDFS_INPUTPATH = "/webmap";
    private static String HDFS_OUTPUTPAH = "/result";
    
    private static void generateLoadGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(LoadGraphVertex.class);
    	job.setVertexInputFormatClass(BinaryLoadGraphInputFormat.class);
        job.setVertexOutputFormatClass(BinaryLoadGraphOutputFormat.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ByteWritable.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genLoadGraph() throws IOException {
    	generateLoadGraphJob("LoadGraph", outputBase + "LoadGraph.xml");
    }
    
    private static void generateMergeGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(MergeGraphVertex.class);
    	job.setVertexInputFormatClass(BinaryLoadGraphInputFormat.class);
        job.setVertexOutputFormatClass(BinaryLoadGraphOutputFormat.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genMergeGraph() throws IOException {
    	generateMergeGraphJob("MergeGraph", outputBase + "MergeGraph.xml");
    }
    
    private static void generateLogAlgorithmForMergeGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(LogAlgorithmForMergeGraphVertex.class);
        job.setVertexInputFormatClass(LogAlgorithmForMergeGraphInputFormat.class); 
        job.setVertexOutputFormatClass(LogAlgorithmForMergeGraphOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
        FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genLogAlgorithmForMergeGraph() throws IOException {
    	generateLogAlgorithmForMergeGraphJob("LogAlgorithmForMergeGraph", outputBase + "LogAlgorithmForMergeGraph.xml");
    }
    
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//genLoadGraph();
		genMergeGraph();
		//genLogAlgorithmForMergeGraph();
		//genSequenceLoadGraph();
		//genBasicBinaryLoadGraph();
	}

}
