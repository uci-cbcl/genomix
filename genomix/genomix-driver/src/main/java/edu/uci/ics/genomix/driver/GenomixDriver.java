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

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.kohsuke.args4j.CmdLineException;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.config.GenomixJobConf.Patterns;
import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.minicluster.GenomixMiniCluster;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
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
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.driver.Driver;

/**
 * The main entry point for the Genomix assembler, a hyracks/pregelix/hadoop-based deBruijn assembler.
 */
public class GenomixDriver {
    private static final Log LOG = LogFactory.getLog(GenomixDriver.class);
    private static final String HADOOP_CONF = "hadoop.conf.xml";
    private String prevOutput;
    private String curOutput;
    private int stepNum;
    private List<PregelixJob> jobs;
    private boolean followingBuild = false; // need to adapt the graph immediately after building

    private edu.uci.ics.genomix.hyracks.graph.driver.Driver hyracksDriver;
    private edu.uci.ics.pregelix.core.driver.Driver pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(
            this.getClass());

    private void copyLocalToHDFS(JobConf conf, String localDir, String destDir) throws IOException {
        LOG.info("Copying local directory " + localDir + " to HDFS: " + destDir);
        GenomixJobConf.tick("copyLocalToHDFS");
        FileSystem dfs = FileSystem.get(conf);
        Path dest = new Path(destDir);
        dfs.delete(dest, true);
        dfs.mkdirs(dest);

        File srcBase = new File(localDir);
        if (srcBase.isDirectory())
            for (File f : srcBase.listFiles())
                dfs.copyFromLocalFile(new Path(f.toString()), dest);
        else
            dfs.copyFromLocalFile(new Path(localDir), dest);
        
        LOG.info("Copy took " + GenomixJobConf.tock("copyLocalToHDFS") + "ms");
    }

    private static void copyBinToLocal(GenomixJobConf conf, String hdfsSrcDir, String localDestDir) throws IOException {
        LOG.info("Copying HDFS directory " + hdfsSrcDir + " to local: " + localDestDir);
        GenomixJobConf.tick("copyBinToLocal");
        FileSystem dfs = FileSystem.get(conf);
        FileUtils.deleteQuietly(new File(localDestDir));

        // save original binary to output/bin
        dfs.copyToLocalFile(new Path(hdfsSrcDir), new Path(localDestDir + File.separator + "bin"));

        // convert hdfs sequence files to text as output/text
        BufferedWriter bw = null;
        SequenceFile.Reader reader = null;
        Writable key = null;
        Writable value = null;
        FileStatus[] files = dfs.globStatus(new Path(hdfsSrcDir + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0 && !f.isDir()) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    if (bw == null)
                        bw = new BufferedWriter(new FileWriter(localDestDir + File.separator + "data"));
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        bw.write(key.toString() + "\t" + value.toString());
                        bw.newLine();
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error copying " + f + " to local:\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }

            }
        }
        if (bw != null)
            bw.close();
        LOG.info("Copy took " + GenomixJobConf.tock("copyBinToLocal") + "ms");
    }

    private static void drawStatistics(GenomixJobConf conf, String inputStats, String outputChart) throws IOException {
        LOG.info("Getting coverage statistics...");
        GenomixJobConf.tick("drawStatistics");
        FileSystem dfs = FileSystem.get(conf);
        
        // stream in the graph, counting elements as you go... this would be better as a hadoop job which aggregated... maybe into counters?
        SequenceFile.Reader reader = null;
        VKmerBytesWritable key = null;
        NodeWritable value = null;
        TreeMap<Integer, Long> coverageCounts = new TreeMap<Integer, Long>();        
        FileStatus[] files = dfs.globStatus(new Path(inputStats + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmerBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (NodeWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        Integer cov = java.lang.Math.round(value.getAverageCoverage());
                        Long count = coverageCounts.get(cov);
                        if (count == null)
                            coverageCounts.put(cov, new Long(1));
                        else 
                            coverageCounts.put(cov, count + 1);
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting stats for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
        
        XYSeries series = new XYSeries("Kmer Coverage");
        for (Entry<Integer, Long> pair : coverageCounts.entrySet()) {
            series.add(pair.getKey().floatValue(), pair.getValue().longValue());
        }
        XYDataset xyDataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createXYLineChart("Coverage per kmer in " + new File(inputStats).getName(),
                "Coverage", "Count", xyDataset, PlotOrientation.VERTICAL, true, true, false);

        // Write the data to the output stream:
        FileOutputStream chartOut = new FileOutputStream(new File(outputChart));
        ChartUtilities.writeChartAsPNG(chartOut, chart, 800, 600);
        chartOut.flush();
        chartOut.close();
        LOG.info("Coverage took " + GenomixJobConf.tock("drawStatistics") + "ms");
    }
    
    
    private static void dumpGraph(GenomixJobConf conf, String inputGraph, String outputFasta, boolean followingBuild) throws IOException {
        LOG.info("Dumping graph to fasta...");
        GenomixJobConf.tick("dumpGraph");
        FileSystem dfs = FileSystem.get(conf);
        
        // stream in the graph, counting elements as you go... this would be better as a hadoop job which aggregated... maybe into counters?
        SequenceFile.Reader reader = null;
        VKmerBytesWritable key = null;
        NodeWritable value = null;
        BufferedWriter bw = null;
        FileStatus[] files = dfs.globStatus(new Path(inputGraph + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmerBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (NodeWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    if (bw == null)
                        bw = new BufferedWriter(new FileWriter(outputFasta));
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        bw.write(">node_" + key.toString() + "\n");
                        bw.write(followingBuild ? key.toString() : value.getInternalKmer().toString());
                        bw.newLine();
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting stats for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
        if (bw != null)
            bw.close();
        LOG.info("Dump graph to fasta took " + GenomixJobConf.tock("dumpGraph") + "ms");
    }

    private void buildGraphWithHyracks(GenomixJobConf conf) throws NumberFormatException, HyracksException {
        LOG.info("Building Graph using Hyracks...");
        GenomixJobConf.tick("buildGraphWithHyracks");
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        //        hyracksDriver = new edu.uci.ics.genomix.hyracks.graph.driver.Driver(conf.get(GenomixJobConf.IP_ADDRESS),
        //                Integer.parseInt(conf.get(GenomixJobConf.PORT)), Integer.parseInt(conf
        //                        .get(GenomixJobConf.CORES_PER_MACHINE)));
        hyracksDriver = new edu.uci.ics.genomix.hyracks.graph.driver.Driver(conf.get(GenomixJobConf.IP_ADDRESS),
                Integer.parseInt(conf.get(GenomixJobConf.PORT)), 1);
        hyracksDriver.runJob(conf, Plan.BUILD_UNMERGED_GRAPH, Boolean.parseBoolean(conf.get(GenomixJobConf.PROFILE)));
        followingBuild = true;
        LOG.info("Building the graph took " + GenomixJobConf.tock("buildGraphWithHyracks") + "ms");
    }

    private void buildGraphWithHadoop(GenomixJobConf conf) throws IOException {
        LOG.info("Building Graph using Hadoop...");
        GenomixJobConf.tick("buildGraphWithHadoop");
        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF)));
        conf.writeXml(confOutput);
        confOutput.close();
        edu.uci.ics.genomix.hadoop.contrailgraphbuilding.GenomixDriver hadoopDriver = new edu.uci.ics.genomix.hadoop.contrailgraphbuilding.GenomixDriver();
        hadoopDriver.run(prevOutput, curOutput, Integer.parseInt(conf.get(GenomixJobConf.CORES_PER_MACHINE)),
                Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)), 4 * 100000, true, HADOOP_CONF);
        FileUtils.deleteQuietly(new File(HADOOP_CONF));
        System.out.println("Finished job Hadoop-Build-Graph");
        followingBuild = true;
        LOG.info("Building the graph took " + GenomixJobConf.tock("buildGraphWithHadoop"));
    }

    private void setOutput(GenomixJobConf conf, Patterns step) {
        prevOutput = curOutput;
        curOutput = conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator + String.format("%02d-", stepNum) + step;
        FileInputFormat.setInputPaths(conf, new Path(prevOutput));
        FileOutputFormat.setOutputPath(conf, new Path(curOutput));
    }

    private void addJob(PregelixJob job) {
        if (followingBuild)
            job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        jobs.add(job);
        followingBuild = false;
    }

    public void runGenomix(GenomixJobConf conf) throws NumberFormatException, HyracksException, Exception {
        KmerBytesWritable.setGlobalKmerLength(Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)));
        jobs = new ArrayList<PregelixJob>();
        stepNum = 0;
        boolean dump = false; 
        boolean runLocal = Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL));
        if (runLocal)
            GenomixMiniCluster.init(conf);

        String localInput = conf.get(GenomixJobConf.LOCAL_INPUT_DIR);
        if (localInput != null) {
            conf.set(GenomixJobConf.INITIAL_INPUT_DIR, conf.get(GenomixJobConf.HDFS_WORK_PATH) + File.separator
                    + "00-initial-input-from-genomix-driver");
            copyLocalToHDFS(conf, localInput, conf.get(GenomixJobConf.INITIAL_INPUT_DIR));
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
                    addJob(P1ForPathMergeVertex.getConfiguredJob(conf));
                    break;
                case MERGE_P2:
                    setOutput(conf, Patterns.MERGE_P2);
                    addJob(P2ForPathMergeVertex.getConfiguredJob(conf));
                    break;
                case MERGE:
                case MERGE_P4:
                    setOutput(conf, Patterns.MERGE_P4);
                    addJob(P4ForPathMergeVertex.getConfiguredJob(conf));
                    break;
                case UNROLL_TANDEM:
                    setOutput(conf, Patterns.UNROLL_TANDEM);
                    addJob(UnrollTandemRepeat.getConfiguredJob(conf));
                    break;
                case TIP_REMOVE:
                    setOutput(conf, Patterns.TIP_REMOVE);
                    addJob(TipRemoveVertex.getConfiguredJob(conf));
                    break;
                case BUBBLE:
                    setOutput(conf, Patterns.BUBBLE);
                    addJob(BubbleMergeVertex.getConfiguredJob(conf));
                    break;
                case LOW_COVERAGE:
                    setOutput(conf, Patterns.LOW_COVERAGE);
                    addJob(RemoveLowCoverageVertex.getConfiguredJob(conf));
                    break;
                case BRIDGE:
                    setOutput(conf, Patterns.BRIDGE);
                    addJob(BridgeRemoveVertex.getConfiguredJob(conf));
                    break;
                case SPLIT_REPEAT:
                    setOutput(conf, Patterns.SPLIT_REPEAT);
                    addJob(SplitRepeatVertex.getConfiguredJob(conf));
                    break;
                case SCAFFOLD:
                    setOutput(conf, Patterns.SCAFFOLD);
                    addJob(ScaffoldingVertex.getConfiguredJob(conf));
                    break;
                case STATS:
                    drawStatistics(conf, curOutput, "coverage.png");
                    break;
                case DUMP_FASTA:
                    dump = true;
                    break;
            }
        }

        // if the user wants to, we can save the intermediate results to HDFS (running each job individually)
        // this would let them resume at arbitrary points of the pipeline
        if (Boolean.parseBoolean(conf.get(GenomixJobConf.SAVE_INTERMEDIATE_RESULTS))) {
            for (int i = 0; i < jobs.size(); i++) {
                LOG.info("Starting job " + jobs.get(i).getJobName());
                GenomixJobConf.tick("pregelix-job");
                
                pregelixDriver.runJob(jobs.get(i), conf.get(GenomixJobConf.IP_ADDRESS),
                        Integer.parseInt(conf.get(GenomixJobConf.PORT)));
                
                LOG.info("Finished job " + jobs.get(i).getJobName() + " in " + GenomixJobConf.tock("pregelix-job"));
            }
        } else {
            LOG.info("Starting pregelix job series...");
            GenomixJobConf.tick("pregelix-runJobs");
            pregelixDriver.runJobs(jobs, conf.get(GenomixJobConf.IP_ADDRESS),
                    Integer.parseInt(conf.get(GenomixJobConf.PORT)));
            LOG.info("Finished job series in " + GenomixJobConf.tock("pregelix-runJobs"));
        }

        if (conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR) != null)
            copyBinToLocal(conf, curOutput, conf.get(GenomixJobConf.LOCAL_OUTPUT_DIR));
        if (dump)
            dumpGraph(conf, curOutput, "genome.fasta", followingBuild);
        if (conf.get(GenomixJobConf.FINAL_OUTPUT_DIR) != null)
            FileSystem.get(conf).rename(new Path(curOutput), new Path(GenomixJobConf.FINAL_OUTPUT_DIR));

        if (runLocal)
            GenomixMiniCluster.deinit();
    }

    public static void main(String[] args) throws CmdLineException, NumberFormatException, HyracksException, Exception {
                String[] myArgs = { "-runLocal", "-kmerLength", "5",
                        "-coresPerMachine", "2",
//                        "-saveIntermediateResults", "true",
//                        "-localInput", "../genomix-pregelix/data/input/reads/synthetic/",
//                        "-localInput", "../genomix-pregelix/data/input/reads/pathmerge",
                        "-localInput", "/home/wbiesing/code/biggerInput",
//                        "-hdfsInput", "/home/wbiesing/code/hyracks/genomix/genomix-driver/genomix_out/01-BUILD_HADOOP",
        //                "-localInput", "/home/wbiesing/code/hyracks/genomix/genomix-pregelix/data/input/reads/test",
        //                "-localInput", "output-build/bin",
//                        "-localOutput", "output-skip",
                        //                            "-pipelineOrder", "BUILD,MERGE",
                        //                            "-inputDir", "/home/wbiesing/code/hyracks/genomix/genomix-driver/graphbuild.binmerge",
                        //                "-localInput", "../genomix-pregelix/data/TestSet/PathMerge/CyclePath/bin/part-00000", 
                        "-pipelineOrder", "BUILD_HADOOP,STATS,MERGE,DUMP_FASTA" };
                
        //        Patterns.BUILD, Patterns.MERGE, 
        //        Patterns.TIP_REMOVE, Patterns.MERGE,
        //        Patterns.BUBBLE, Patterns.MERGE,
                GenomixJobConf conf = GenomixJobConf.fromArguments(args);
                GenomixDriver driver = new GenomixDriver();
                driver.runGenomix(conf);
    }
}
