package edu.uci.ics.genomix.pregelix.JobGen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.P2PathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeAddVertex;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.bubblemerge.BubbleAddVertex;
import edu.uci.ics.genomix.pregelix.operator.bubblemerge.BubbleMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P2ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.MapReduceVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P1ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P3ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.splitrepeat.SplitRepeatVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.TipAddVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.TipRemoveVertex;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class JobGenerator {

    public static String outputBase = "src/test/resources/jobs/";

//    private static void generateNaiveAlgorithmForMergeGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(P1ForPathMergeVertex.class);
//        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class); //GraphCleanInputFormat
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(PositionWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(P1ForPathMergeVertex.KMER_SIZE, 3);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genNaiveAlgorithmForMergeGraph() throws IOException {
//        generateNaiveAlgorithmForMergeGraphJob("NaiveAlgorithmForMergeGraph", outputBase
//                + "NaiveAlgorithmForMergeGraph.xml");
//    }

    private static void generateLogAlgorithmForMergeGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(P2ForPathMergeVertex.class);
        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(P2PathMergeOutputFormat.class); 
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.getConfiguration().setInt(P2ForPathMergeVertex.KMER_SIZE, 3);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genLogAlgorithmForMergeGraph() throws IOException {
        generateLogAlgorithmForMergeGraphJob("LogAlgorithmForMergeGraph", outputBase + "LogAlgorithmForMergeGraph.xml");
    }
//    
//    private static void generateP3ForMergeGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(P3ForPathMergeVertex.class);
//        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(PositionWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(P3ForPathMergeVertex.KMER_SIZE, 3);
//        job.getConfiguration().setFloat(P3ForPathMergeVertex.PSEUDORATE, 0.3f);
//        job.getConfiguration().setInt(P3ForPathMergeVertex.MAXROUND, 2);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genP3ForMergeGraph() throws IOException {
//        generateP3ForMergeGraphJob("P3ForMergeGraph", outputBase
//                + "P3ForMergeGraph.xml");
//    }
    
//    private static void generateP4ForMergeGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(P4ForPathMergeVertex.class);
//        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(KmerBytesWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(P4ForPathMergeVertex.KMER_SIZE, 3);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genP4ForMergeGraph() throws IOException {
//        generateP4ForMergeGraphJob("P4ForMergeGraph", outputBase
//                + "P4ForMergeGraph.xml");
//    }
    
    private static void generateMapReduceGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(MapReduceVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(P2PathMergeOutputFormat.class); 
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.getConfiguration().setInt(MapReduceVertex.KMER_SIZE, 3);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genMapReduceGraph() throws IOException {
        generateMapReduceGraphJob("MapReduceGraph", outputBase + "MapReduceGraph.xml");
    }
    
    private static void generateSplitRepeatGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(SplitRepeatVertex.class);
        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class); 
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.getConfiguration().setInt(SplitRepeatVertex.KMER_SIZE, 3);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genSplitRepeatGraph() throws IOException {
        generateSplitRepeatGraphJob("SplitRepeatGraph", outputBase + "SplitRepeatGraph.xml");
    }
    private static void generateTipAddGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(jobName);
        job.setVertexClass(TipAddVertex.class);
        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.getConfiguration().setInt(TipAddVertex.KMER_SIZE, 3);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genTipAddGraph() throws IOException {
        generateTipAddGraphJob("TipAddGraph", outputBase
                + "TipAddGraph.xml");
    }
//    
//    private static void generateTipRemoveGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(TipRemoveVertex.class);
//        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(PositionWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(TipRemoveVertex.KMER_SIZE, 5);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genTipRemoveGraph() throws IOException {
//        generateTipRemoveGraphJob("TipRemoveGraph", outputBase
//                + "TipRemoveGraph.xml");
//    }
//    
//    private static void generateBridgeAddGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(BridgeAddVertex.class);
//        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(PositionWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(BridgeAddVertex.KMER_SIZE, 3);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genBridgeAddGraph() throws IOException {
//        generateBridgeAddGraphJob("BridgeAddGraph", outputBase
//                + "BridgeAddGraph.xml");
//    }
//
//    private static void generateBridgeRemoveGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(BridgeRemoveVertex.class);
//        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(PositionWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(TipRemoveVertex.KMER_SIZE, 5);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genBridgeRemoveGraph() throws IOException {
//        generateBridgeRemoveGraphJob("BridgeRemoveGraph", outputBase
//                + "BridgeRemoveGraph.xml");
//    }
//    
//    private static void generateBubbleAddGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(BubbleAddVertex.class);
//        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(PositionWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(BubbleAddVertex.KMER_SIZE, 3);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genBubbleAddGraph() throws IOException {
//        generateBubbleAddGraphJob("BubbleAddGraph", outputBase
//                + "BubbleAddGraph.xml");
//    }
//    
//    private static void generateBubbleMergeGraphJob(String jobName, String outputPath) throws IOException {
//        PregelixJob job = new PregelixJob(jobName);
//        job.setVertexClass(BubbleMergeVertex.class);
//        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
//        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
//        job.setDynamicVertexValueSize(true);
//        job.setOutputKeyClass(PositionWritable.class);
//        job.setOutputValueClass(VertexValueWritable.class);
//        job.getConfiguration().setInt(BubbleMergeVertex.KMER_SIZE, 5);
//        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
//    }
//
//    private static void genBubbleMergeGraph() throws IOException {
//        generateBubbleMergeGraphJob("BubbleMergeGraph", outputBase
//                + "BubbleMergeGraph.xml");
//    }
    
    public static void main(String[] args) throws IOException {
        //genNaiveAlgorithmForMergeGraph();
//        genLogAlgorithmForMergeGraph();
        //genP3ForMergeGraph();
        //genTipAddGraph();
//        genTipRemoveGraph();
//        genBridgeAddGraph();
//        genBridgeRemoveGraph();
//        genBubbleAddGraph();
//        genBubbleMergeGraph();
//        genP4ForMergeGraph();
//        genMapReduceGraph();
        genSplitRepeatGraph();
        genTipAddGraph();
    }

}
