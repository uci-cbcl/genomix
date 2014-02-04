package edu.uci.ics.genomix.pregelix.jobgen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.pregelix.aggregator.DeBruijnVertexCounterAggregator;
import edu.uci.ics.genomix.pregelix.base.NodeToVertexInputFormat;
import edu.uci.ics.genomix.pregelix.base.VertexToNodeOutputFormat;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.complexbubblemerge.BubbleMergeWithSearchVertex;
import edu.uci.ics.genomix.pregelix.operator.extractsubgraph.ExtractSubgraphVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P1ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.removelowcoverage.RemoveLowCoverageVertex;
import edu.uci.ics.genomix.pregelix.operator.scaffolding2.RayVertex;
import edu.uci.ics.genomix.pregelix.operator.simplebubblemerge.SimpleBubbleMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.symmetrychecker.SymmetryCheckerVertex;
import edu.uci.ics.genomix.pregelix.operator.test.BridgeAddVertex;
import edu.uci.ics.genomix.pregelix.operator.test.BubbleAddVertex;
import edu.uci.ics.genomix.pregelix.operator.test.MapReduceVertex;
//import edu.uci.ics.genomix.pregelix.operator.test.RayAddSimpleBranch;
import edu.uci.ics.genomix.pregelix.operator.test.TipAddVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.SingleNodeTipRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat.UnrollTandemRepeat;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class JobGenerator {

    public static String outputBase = "src/test/resources/jobs/";

    private static void configureJob(PregelixJob job) {
        job.setCounterAggregatorClass(DeBruijnVertexCounterAggregator.class);
        job.setVertexInputFormatClass(NodeToVertexInputFormat.class);
        job.setVertexOutputFormatClass(VertexToNodeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(VKmer.class);
        job.setOutputValueClass(Node.class);
    }

    /**
     * Help Function
     */
    private static void generateTipAddGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(new GenomixJobConf(3), jobName);
        job.setVertexClass(TipAddVertex.class);
        configureJob(job);
        job.getConfiguration().set(TipAddVertex.SPLIT_NODE, "CTA");
        job.getConfiguration().set(TipAddVertex.INSERTED_TIP, "AGC");
        job.getConfiguration().setInt(TipAddVertex.TIP_TO_SPLIT_EDGETYPE, EDGETYPE.RF.get());
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genTipAddGraph() throws IOException {
        generateTipAddGraphJob("TipAddGraph", outputBase + "TIP_ADD.xml");
    }

    private static void generateBridgeAddGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(new GenomixJobConf(3), jobName);
        job.setVertexClass(BridgeAddVertex.class);
        configureJob(job);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genBridgeAddGraph() throws IOException {
        generateBridgeAddGraphJob("BridgeAddGraph", outputBase + "BRIDGE_ADD.xml");
    }

    private static void generateBubbleAddGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = new PregelixJob(new GenomixJobConf(3), jobName);
        job.setVertexClass(BubbleAddVertex.class);
        configureJob(job);
        job.getConfiguration().set(BubbleAddVertex.MAJOR_VERTEXID, "AAT"); //forward
        job.getConfiguration().set(BubbleAddVertex.MIDDLE_VERTEXID, "ATA"); //forward
        job.getConfiguration().set(BubbleAddVertex.MINOR_VERTEXID, "CTA"); //forward
        job.getConfiguration().set(BubbleAddVertex.INSERTED_BUBBLE, "ACA"); //forward
        job.getConfiguration().set(BubbleAddVertex.INTERNAL_KMER_IN_NEWBUBBLE, "ATA");
        job.getConfiguration().setFloat(BubbleAddVertex.COVERAGE_OF_INSERTED_BUBBLE, 4.0f);
        job.getConfiguration().setLong(BubbleAddVertex.READID, 2);
        job.getConfiguration().setInt(BubbleAddVertex.NEWBUBBLE_TO_MAJOR_EDGETYPE, EDGETYPE.RR.get());
        job.getConfiguration().setInt(BubbleAddVertex.NEWBUBBLE_TO_MINOR_EDGETYPE, EDGETYPE.FR.get());
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genBubbleAddGraph() throws IOException {
        generateBubbleAddGraphJob("BubbleAddGraph", outputBase + "BUBBLE_ADD.xml");
    }
    
    private static void generateRayScaffoldGraphJob(String jobName, String outputPath) throws IOException {
    	PregelixJob job = RayVertex.getConfiguredJob(new GenomixJobConf(7), RayVertex.class);
    	job.getConfiguration().setFloat(GenomixJobConf.SCAFFOLD_SEED_SCORE_PERCENTILE, 1 );
    	job.getConfiguration().setInt(GenomixJobConf.READ_LENGTHS, 9 );
    	//job.getConfiguration().setInt(GenomixJobConf.KMER_LENGTH, 7 );
    	//GenomixJobConf.SCAFFOLDING_INITIAL_DIRECTION
    	job.getConfiguration().setBoolean(GenomixJobConf.SAVE_INTERMEDIATE_RESULTS, true);
    	job.getConfiguration().set(GenomixJobConf.SCAFFOLDING_INITIAL_DIRECTION, DIR.FORWARD.toString());
    	job.getConfiguration().setBoolean(GenomixJobConf.RUN_LOCAL, true );
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));    
    }
    // TODO
    private static void genRayScaffold() throws IOException{
    	generateRayScaffoldGraphJob("RayScaffold", outputBase + "RAY_SCAFFOLD.xml");
    }
    private static void generateUnrollTandemRepeatGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = UnrollTandemRepeat.getConfiguredJob(new GenomixJobConf(3), UnrollTandemRepeat.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genUnrollTandemRepeatGraph() throws IOException {
        generateUnrollTandemRepeatGraphJob("UnrollTandemRepeatGraph", outputBase + "UNROLL_TANDEM.xml");
    }

    private static void generateMapReduceGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = MapReduceVertex.getConfiguredJob(new GenomixJobConf(3), MapReduceVertex.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genMapReduceGraph() throws IOException {
        generateMapReduceGraphJob("MapReduceGraph", outputBase + "MAP_REDUCE.xml");
    }

    private static void generateSymmetryCheckerGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = SymmetryCheckerVertex.getConfiguredJob(new GenomixJobConf(3), SymmetryCheckerVertex.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genSymmetryCheckerGraph() throws IOException {
        generateSymmetryCheckerGraphJob("SymmetryCheckerGraph", outputBase + "CHECK_SYMMETRY.xml");
    }

    private static void generateExtractSubGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = ExtractSubgraphVertex.getConfiguredJob(new GenomixJobConf(3), ExtractSubgraphVertex.class);
        job.getConfiguration().set(GenomixJobConf.PLOT_SUBGRAPH_START_SEEDS, "AAT");
        job.getConfiguration().setInt(GenomixJobConf.PLOT_SUBGRAPH_NUM_HOPS, 1);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genExtractSubGraph() throws IOException {
        generateExtractSubGraphJob("ExtractSubGraph", outputBase + "PLOT_SUBGRAPH.xml");
    }

    /**
     * Main Function
     */
    private static void generateP1ForMergeGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = P1ForPathMergeVertex.getConfiguredJob(new GenomixJobConf(3), P1ForPathMergeVertex.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genP1ForMergeGraph() throws IOException {
        generateP1ForMergeGraphJob("P1ForMergeGraph", outputBase + "P1ForMergeGraph.xml");
    }

    private static void generateP4ForMergeGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = P4ForPathMergeVertex.getConfiguredJob(new GenomixJobConf(3), P4ForPathMergeVertex.class);
        job.getConfiguration().setLong(GenomixJobConf.RANDOM_SEED, 500);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genP4ForMergeGraph() throws IOException {
        generateP4ForMergeGraphJob("P4ForMergeGraph", outputBase + "MERGE.xml");
    }

    private static void generateRemoveLowCoverageGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = RemoveLowCoverageVertex
                .getConfiguredJob(new GenomixJobConf(3), RemoveLowCoverageVertex.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genRemoveLowCoverageGraph() throws IOException {
        generateRemoveLowCoverageGraphJob("RemoveLowCoverageGraph", outputBase + "LOW_COVERAGE.xml");
    }

    private static void generateTipRemoveGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = SingleNodeTipRemoveVertex.getConfiguredJob(new GenomixJobConf(3), SingleNodeTipRemoveVertex.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genTipRemoveGraph() throws IOException {
        generateTipRemoveGraphJob("TipRemoveGraph", outputBase + "TIP_REMOVE.xml");
    }
    
    private static void generateBridgeRemoveGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = BridgeRemoveVertex.getConfiguredJob(new GenomixJobConf(3), BridgeRemoveVertex.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genBridgeRemoveGraph() throws IOException {
        generateBridgeRemoveGraphJob("BridgeRemoveGraph", outputBase + "BRIDGE.xml");
    }

    private static void generateBubbleMergeGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = SimpleBubbleMergeVertex
                .getConfiguredJob(new GenomixJobConf(3), SimpleBubbleMergeVertex.class);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genBubbleMergeGraph() throws IOException {
        generateBubbleMergeGraphJob("BubbleMergeGraph", outputBase + "BUBBLE.xml");
    }

    private static void generateBubbleMergeWithSearchGraphJob(String jobName, String outputPath) throws IOException {
        PregelixJob job = BubbleMergeWithSearchVertex.getConfiguredJob(new GenomixJobConf(3),
                BubbleMergeWithSearchVertex.class);
        job.getConfiguration().setInt(GenomixJobConf.BUBBLE_MERGE_WITH_SEARCH_MAX_LENGTH, 100);
        job.getConfiguration().writeXml(new FileOutputStream(new File(outputPath)));
    }

    private static void genBubbleMergeWithSearchGraph() throws IOException {
        generateBubbleMergeWithSearchGraphJob("BubbleMergeWithSearchGraph", outputBase + "BUBBLE_WITH_SEARCH.xml");
    }
    


    public static void main(String[] args) throws IOException {
        FileUtils.forceMkdir(new File(outputBase));
//        genUnrollTandemRepeatGraph();
//        genMapReduceGraph();
//        genP1ForMergeGraph();
//        genTipAddGraph();
//        genBridgeAddGraph();
//        genBridgeRemoveGraph();
//        genBubbleAddGraph();
//        genBubbleMergeGraph();
//        genBubbleMergeWithSearchGraph();
//        genSymmetryCheckerGraph();
        
        genExtractSubGraph();
        genTipRemoveGraph();
        genRemoveLowCoverageGraph();
        genP4ForMergeGraph();
        genRayScaffold();
    }

}
