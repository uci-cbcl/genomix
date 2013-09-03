/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.driver;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.config.GenomixJobConf.Patterns;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.minicluster.DriverUtils;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager.ClusterType;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.bubblemerge.BubbleMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P1ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P2ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.P4ForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.operator.removelowcoverage.RemoveLowCoverageVertex;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.ScaffoldingVertex;
import edu.uci.ics.genomix.pregelix.operator.splitrepeat.SplitRepeatVertex;
import edu.uci.ics.genomix.pregelix.operator.tipremove.TipRemoveVertex;
import edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat.UnrollTandemRepeat;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * The main entry point for the Genomix assembler, a hyracks/pregelix/hadoop-based deBruijn assembler.
 */
public class GenomixDriver {

    private static final Log LOG = LogFactory.getLog(GenomixDriver.class);
    private static final String HADOOP_CONF = "hadoop.conf.xml";
    private String prevOutput;
    private String curOutput;
    private int stepNum;
    private List<PregelixJob> pregelixJobs;
    private boolean followingBuild = false; // need to adapt the graph immediately after building
    private boolean runLocal;
    private int numCoresPerMachine;
    private int numMachines;

    private GenomixClusterManager manager;
    private edu.uci.ics.genomix.hyracks.graph.driver.Driver hyracksDriver;
    private edu.uci.ics.pregelix.core.driver.Driver pregelixDriver;

    private void buildGraphWithHyracks(GenomixJobConf conf) throws Exception {
        LOG.info("Building Graph using Hyracks...");
        manager.startCluster(ClusterType.HYRACKS);
        GenomixJobConf.tick("buildGraphWithHyracks");
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        hyracksDriver = new edu.uci.ics.genomix.hyracks.graph.driver.Driver(
                runLocal ? GenomixClusterManager.LOCAL_IP : conf.get(GenomixJobConf.IP_ADDRESS),
                runLocal ? GenomixClusterManager.LOCAL_CLIENT_PORT : Integer.parseInt(conf.get(GenomixJobConf.PORT)), 
                        numCoresPerMachine);
        hyracksDriver.runJob(conf, Plan.BUILD_UNMERGED_GRAPH, Boolean.parseBoolean(conf.get(GenomixJobConf.PROFILE)));
        followingBuild = true;
        manager.stopCluster(ClusterType.HYRACKS);
        LOG.info("Building the graph took " + GenomixJobConf.tock("buildGraphWithHyracks") + "ms");
        if (Boolean.parseBoolean(conf.get(GenomixJobConf.DRAW_STATISTICS)))
            DriverUtils.drawStatistics(conf, curOutput, new Path(curOutput).getName() + ".coverage.png");
    }

    private void buildGraphWithHadoop(GenomixJobConf conf) throws Exception {
        LOG.info("Building Graph using Hadoop...");
        manager.startCluster(ClusterType.HADOOP);
        GenomixJobConf.tick("buildGraphWithHadoop");
        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF)));
        conf.writeXml(confOutput);
        confOutput.close();
        edu.uci.ics.genomix.hadoop.contrailgraphbuilding.GenomixDriver hadoopDriver = new edu.uci.ics.genomix.hadoop.contrailgraphbuilding.GenomixDriver();
        hadoopDriver.run(prevOutput, curOutput, numCoresPerMachine * numMachines,
                Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)), 4 * 100000, true, HADOOP_CONF);
        FileUtils.deleteQuietly(new File(HADOOP_CONF));
        System.out.println("Finished job Hadoop-Build-Graph");
        followingBuild = true;
        manager.stopCluster(ClusterType.HADOOP);
        LOG.info("Building the graph took " + GenomixJobConf.tock("buildGraphWithHadoop") + "ms");
        if (Boolean.parseBoolean(conf.get(GenomixJobConf.DRAW_STATISTICS)))
            DriverUtils.drawStatistics(conf, curOutput, new Path(curOutput).getName() + ".coverage.png");
    }

    @SuppressWarnings("deprecation")
    private void setOutput(GenomixJobConf conf, Patterns step) {
        prevOutput = curOutput;
        curOutput = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("%02d-", stepNum) + step;
        FileInputFormat.setInputPaths(conf, new Path(prevOutput));
        FileOutputFormat.setOutputPath(conf, new Path(curOutput));
    }

    private void addJob(PregelixJob job) {
        if (followingBuild)
            job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        pregelixJobs.add(job);
        followingBuild = false;
    }

    public void runGenomix(GenomixJobConf conf) throws NumberFormatException, HyracksException, Exception {
        LOG.info("Starting Genomix Assembler Pipeline...");
        GenomixJobConf.tick("runGenomix");
        
        DriverUtils.updateCCProperties(conf);
        numCoresPerMachine = conf.get(GenomixJobConf.HYRACKS_IO_DIRS).split(",").length;
        numMachines = conf.get(GenomixJobConf.HYRACKS_SLAVES).split("\r?\n|\r").length;  // split on newlines
        GenomixJobConf.setGlobalStaticConstants(conf);
        followingBuild = Boolean.parseBoolean(conf.get(GenomixJobConf.FOLLOWS_GRAPH_BUILD));
        pregelixJobs = new ArrayList<PregelixJob>();
        stepNum = 0;
        runLocal = Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL));
        manager = new GenomixClusterManager(runLocal, conf);
        manager.stopCluster(ClusterType.HYRACKS); // shut down any existing NCs and CCs

        String localInput = conf.get(GenomixJobConf.LOCAL_INPUT_DIR);
        if (localInput != null) {
            conf.set(GenomixJobConf.INITIAL_INPUT_DIR, conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator
                    + "00-initial-input-from-genomix-driver");
            GenomixClusterManager.copyLocalToHDFS(conf, localInput, conf.get(GenomixJobConf.INITIAL_INPUT_DIR));
        }
        curOutput = conf.get(GenomixJobConf.INITIAL_INPUT_DIR);

        // currently, we just iterate over the jobs set in conf[PIPELINE_ORDER].  In the future, we may want more logic to iterate multiple times, etc
        String pipelineSteps = conf.get(GenomixJobConf.PIPELINE_ORDER);
        for (Patterns step : Patterns.arrayFromString(pipelineSteps)) {
            stepNum++;
            switch (step) {
                case BUILD:
                case BUILD_HYRACKS:
                    setOutput(conf, Patterns.BUILD_HYRACKS);
                    buildGraphWithHyracks(conf);
                    break;
                case BUILD_HADOOP:
                    setOutput(conf, Patterns.BUILD_HADOOP);
                    buildGraphWithHadoop(conf);
                    break;
                case MERGE_P1:
                    setOutput(conf, Patterns.MERGE_P1);
                    addJob(P1ForPathMergeVertex.getConfiguredJob(conf, P1ForPathMergeVertex.class));
                    break;
                case MERGE_P2:
                    setOutput(conf, Patterns.MERGE_P2);
                    addJob(P2ForPathMergeVertex.getConfiguredJob(conf, P2ForPathMergeVertex.class));
                    break;
                case MERGE:
                case MERGE_P4:
                    setOutput(conf, Patterns.MERGE_P4);
                    addJob(P4ForPathMergeVertex.getConfiguredJob(conf, P4ForPathMergeVertex.class));
                    break;
                case UNROLL_TANDEM:
                    setOutput(conf, Patterns.UNROLL_TANDEM);
                    addJob(UnrollTandemRepeat.getConfiguredJob(conf, UnrollTandemRepeat.class));
                    break;
                case TIP_REMOVE:
                    setOutput(conf, Patterns.TIP_REMOVE);
                    addJob(TipRemoveVertex.getConfiguredJob(conf, TipRemoveVertex.class));
                    break;
                case BUBBLE:
                    setOutput(conf, Patterns.BUBBLE);
                    addJob(BubbleMergeVertex.getConfiguredJob(conf, BubbleMergeVertex.class));
                    break;
                case LOW_COVERAGE:
                    setOutput(conf, Patterns.LOW_COVERAGE);
                    addJob(RemoveLowCoverageVertex.getConfiguredJob(conf, RemoveLowCoverageVertex.class));
                    break;
                case BRIDGE:
                    setOutput(conf, Patterns.BRIDGE);
                    addJob(BridgeRemoveVertex.getConfiguredJob(conf, BridgeRemoveVertex.class));
                    break;
                case SPLIT_REPEAT:
                    setOutput(conf, Patterns.SPLIT_REPEAT);
                    addJob(SplitRepeatVertex.getConfiguredJob(conf, SplitRepeatVertex.class));
                    break;
                case SCAFFOLD:
                    setOutput(conf, Patterns.SCAFFOLD);
                    addJob(ScaffoldingVertex.getConfiguredJob(conf, ScaffoldingVertex.class));
                    break;
                case DUMP_FASTA:
                    DriverUtils.dumpGraph(conf, curOutput, "genome.fasta", followingBuild);
                    break;
            }
        }

        if (pregelixJobs.size() > 0) {
            manager.startCluster(ClusterType.PREGELIX);
            pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(this.getClass());
            // if the user wants to, we can save the intermediate results to HDFS (running each job individually)
            // this would let them resume at arbitrary points of the pipeline
            if (Boolean.parseBoolean(conf.get(GenomixJobConf.SAVE_INTERMEDIATE_RESULTS))) {
                for (int i = 0; i < pregelixJobs.size(); i++) {
                    LOG.info("Starting job " + pregelixJobs.get(i).getJobName());
                    GenomixJobConf.tick("pregelix-job");
                    
                    pregelixDriver.runJob(pregelixJobs.get(i), conf.get(GenomixJobConf.IP_ADDRESS),
                            Integer.parseInt(conf.get(GenomixJobConf.PORT)));
                    
                    LOG.info("Finished job " + pregelixJobs.get(i).getJobName() + " in " + GenomixJobConf.tock("pregelix-job"));
                }
            } else {
                LOG.info("Starting pregelix job series...");
                GenomixJobConf.tick("pregelix-runJobs");
                pregelixDriver.runJobs(pregelixJobs, conf.get(GenomixJobConf.IP_ADDRESS),
                        Integer.parseInt(conf.get(GenomixJobConf.PORT)));
                LOG.info("Finished job series in " + GenomixJobConf.tock("pregelix-runJobs"));
            }
            manager.stopCluster(ClusterType.PREGELIX);
        }

        if (conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR) != null)
            GenomixClusterManager.copyBinToLocal(conf, curOutput, conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR));
        if (conf.get(GenomixJobConf.FINAL_OUTPUT_DIR) != null)
            FileSystem.get(conf).rename(new Path(curOutput), new Path(GenomixJobConf.FINAL_OUTPUT_DIR));
        
        LOG.info("Finished the Genomix Assembler Pipeline in " + GenomixJobConf.tock("runGenomix") + "ms!");
    }

    public static void main(String[] args) throws CmdLineException, NumberFormatException, HyracksException, Exception {
        String[] myArgs = {
//                "-runLocal", "true",
                "-kmerLength", "5", "-coresPerMachine", "2",
                //                        "-saveIntermediateResults", "true",
                //                        "-localInput", "../genomix-pregelix/data/input/reads/synthetic/",
                "-localInput", "../genomix-pregelix/data/input/reads/pathmerge",
                //                        "-localInput", "/home/wbiesing/code/biggerInput",
                //                        "-hdfsInput", "/home/wbiesing/code/hyracks/genomix/genomix-driver/genomix_out/01-BUILD_HADOOP",
                //                "-localInput", "/home/wbiesing/code/hyracks/genomix/genomix-pregelix/data/input/reads/test",
                //                "-localInput", "output-build/bin",
                //                        "-localOutput", "output-skip",
                //                            "-pipelineOrder", "BUILD,MERGE",
                //                            "-inputDir", "/home/wbiesing/code/hyracks/genomix/genomix-driver/graphbuild.binmerge",
                //                "-localInput", "../genomix-pregelix/data/TestSet/PathMerge/CyclePath/bin/part-00000", 
                "-pipelineOrder", "BUILD_HYRACKS,STATS,MERGE" };
        // allow Eclipse to run the maven-generated scripts
                if (System.getProperty("app.home") == null)
                    System.setProperty("app.home", new File("target/appassembler").getAbsolutePath());

        //        Patterns.BUILD, Patterns.MERGE, 
        //        Patterns.TIP_REMOVE, Patterns.MERGE,
        //        Patterns.BUBBLE, Patterns.MERGE,
        GenomixJobConf conf = GenomixJobConf.fromArguments(args);
        GenomixDriver driver = new GenomixDriver();
        driver.runGenomix(conf);
    }

}
