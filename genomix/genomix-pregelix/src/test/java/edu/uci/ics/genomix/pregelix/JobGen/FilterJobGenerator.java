package edu.uci.ics.genomix.pregelix.JobGen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.operator.LogFilterVertex;
import edu.uci.ics.genomix.pregelix.operator.NaiveAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.NaiveFilterVertex;
import edu.uci.ics.genomix.pregelix.operator.TwoStepLogAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;


public class FilterJobGenerator {

    public static String outputBase = "src/test/resources/jobs/";
    
    private static void generateNaiveFilterJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(NaiveFilterVertex.class);
    	job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        job.getConfiguration().setInt(NaiveAlgorithmForPathMergeVertex.KMER_SIZE, 5);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genNaiveFilter() throws IOException {
    	generateNaiveFilterJob("NaiveFilterVertex", outputBase + "NaiveFilterVertex.xml");
    }
    
    private static void generateLogFilterJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = new PregelixJob(jobName);
    	job.setVertexClass(LogFilterVertex.class);
        job.setVertexInputFormatClass(LogAlgorithmForPathMergeInputFormat.class); 
        job.setVertexOutputFormatClass(LogAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        job.getConfiguration().setInt(TwoStepLogAlgorithmForPathMergeVertex.KMER_SIZE, 5);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }
    
    private static void genLogFilter() throws IOException {
    	generateLogFilterJob("LogFilterVertex", outputBase + "LogFilterVertex.xml");
    }
    
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		genNaiveFilter();
		genLogFilter();
	}

}
