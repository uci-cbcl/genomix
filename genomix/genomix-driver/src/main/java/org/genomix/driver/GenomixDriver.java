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

package org.genomix.driver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.kohsuke.args4j.CmdLineException;

import com.sun.xml.internal.xsom.impl.NotationImpl;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.config.GenomixJobConf.Patterns;
import edu.uci.ics.genomix.hadoop.pmcommon.HadoopMiniClusterTest;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.graph.job.JobGenBrujinGraph;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
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
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;

/**
 * The main entry point for the Genomix assembler, a hyracks/pregelix/hadoop-based deBruijn assembler.
 */
public class GenomixDriver {

    private static final String CONF_XML = "conf.xml";

    private String prevOutput;
    private String curOutput;
    private int stepNum;
    private List<PregelixJob> jobs;
    private boolean followingBuild = true;  // need to adapt the graph immediately after building

    private edu.uci.ics.genomix.hyracks.graph.driver.Driver hyracksDriver;
    private edu.uci.ics.pregelix.core.driver.Driver pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(this.getClass());

    private void copyLocalToHDFS(JobConf conf, String localFile, String destFile) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        Path dest = new Path(destFile);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(new Path(localFile), dest);
    }

    private void copyHDFSToLocal(GenomixJobConf conf, String hdfsSrc, String localDest) throws Exception {
        FileUtils.deleteDirectory(new File(localDest));
        copyResultsToLocal(hdfsSrc, localDest, false,
                conf, true, FileSystem.get(conf));
//        FileUtil.copy(FileSystem.get(conf), new Path(hdfsSrc), FileSystem.getLocal(new Configuration()), new Path(
//                localDest), false, conf);
    }
    
    private static void copyResultsToLocal(String hdfsSrcDir, String localDestFile, boolean resultsAreText,
            Configuration conf, boolean ignoreZeroOutputs, FileSystem dfs) throws IOException {
        if (resultsAreText) {
            // for text files, just concatenate them together
            FileUtil.copyMerge(FileSystem.get(conf), new Path(hdfsSrcDir), FileSystem.getLocal(new Configuration()),
                    new Path(localDestFile), false, conf, null);
        } else {
            // file is binary
            // save the entire binary output dir
            FileUtil.copy(FileSystem.get(conf), new Path(hdfsSrcDir), FileSystem.getLocal(new Configuration()),
                    new Path(localDestFile + ".bindir"), false, conf);
            
            // chomp through output files
            FileStatus[] files = ArrayUtils.addAll(dfs.globStatus(new Path(hdfsSrcDir + "*")), dfs.globStatus(new Path(hdfsSrcDir + "*/*")));
            FileStatus validFile = null;
            for (FileStatus f : files) {
                if (f.getLen() != 0) {
                    validFile = f;
                    break;
                }
            }
            if (validFile == null) {
                if (ignoreZeroOutputs) {
                    // just make a dummy output dir
                    FileSystem lfs = FileSystem.getLocal(new Configuration());
                    lfs.mkdirs(new Path(localDestFile).getParent());
                    return;
                }
                else {
                    throw new IOException("No non-zero outputs in source directory " + hdfsSrcDir);
                }
            }

            // also load the Nodes and write them out as text locally. 
            FileSystem lfs = FileSystem.getLocal(new Configuration());
            lfs.mkdirs(new Path(localDestFile).getParent());
            File filePathTo = new File(localDestFile);
            if (filePathTo.exists() && filePathTo.isDirectory()) {
                filePathTo = new File(localDestFile + "/data");
            }
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePathTo));
            SequenceFile.Reader reader = new SequenceFile.Reader(dfs, validFile.getPath(), conf);
            SequenceFile.Writer writer = new SequenceFile.Writer(lfs, new JobConf(), new Path(localDestFile
                    + ".binmerge"), reader.getKeyClass(), reader.getValueClass());

            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            for (FileStatus f : files) {
                if (f.getLen() == 0) {
                    continue;
                }
                reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                while (reader.next(key, value)) {
                    if (key == null || value == null) {
                        break;
                    }
                    bw.write(key.toString() + "\t" + value.toString());
                    System.out.println(key.toString() + "\t" + value.toString());
                    bw.newLine();
                    writer.append(key, value);

                }
                reader.close();
            }
            writer.close();
            bw.close();
        }

    }

    private void buildGraph(GenomixJobConf conf) throws NumberFormatException, HyracksException {
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        hyracksDriver = new edu.uci.ics.genomix.hyracks.graph.driver.Driver(conf.get(GenomixJobConf.IP_ADDRESS),
                Integer.parseInt(conf.get(GenomixJobConf.PORT)), Integer.parseInt(conf
                        .get(GenomixJobConf.CPARTITION_PER_MACHINE)));
        hyracksDriver.runJob(conf, Plan.BUILD_UNMERGED_GRAPH, Boolean.parseBoolean(conf.get(GenomixJobConf.PROFILE)));
        followingBuild = true;
    }

    private void setOutput(GenomixJobConf conf, Patterns step) {
        prevOutput = curOutput;
        curOutput = File.separator + String.format("%02d-", stepNum) + step;
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
        String origInput = conf.get(GenomixJobConf.INITIAL_INPUT_DIR);
        KmerBytesWritable.setGlobalKmerLength(Integer.parseInt(conf.get(GenomixJobConf.KMER_LENGTH)));
        curOutput = "/00-initial-input";
        //        TestCluster testCluster = new TestCluster();
        TestCluster2 testCluster = new TestCluster2();

        boolean runLocal = Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL));
        stepNum = 0;
        try {
            if (runLocal) {
                testCluster.setUp(conf);
            }
            copyLocalToHDFS(conf, origInput, curOutput);

            jobs = new ArrayList<PregelixJob>();

            // currently, we just iterate over the jobs set in conf[PIPELINE_ORDER].  In the future, we may want more logic to iterate multiple times, etc
            String pipelineSteps = conf.get(GenomixJobConf.PIPELINE_ORDER);
            for (Patterns step : Patterns.arrayFromString(pipelineSteps)) {
                stepNum++;
                switch (step) {
                    case BUILD:
                    case BUILD_HYRACKS:
                        setOutput(conf, Patterns.BUILD);
                        buildGraph(conf);
                        break;
                    case BUILD_MR:
                        //TODO add the hadoop build code
                        throw new IllegalArgumentException("BUILD_MR hasn't been added to the driver yet!");
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
                }
            }
            for (int i = 0; i < jobs.size(); i++) {
//                pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(jobs.get(i).getConfiguration().getClass(PregelixJob.VERTEX_CLASS, null));
//                pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(jobs.get(i).getConfiguration().getClass(PregelixJob.VERTEX_CLASS, null));
                pregelixDriver.runJob(jobs.get(i), conf.get(GenomixJobConf.IP_ADDRESS),
                        Integer.parseInt(conf.get(GenomixJobConf.PORT)));
            }

            //            pregelixDriver.runJobs(jobs, conf.get(GenomixJobConf.IP_ADDRESS), Integer.parseInt(conf.get(GenomixJobConf.PORT)));

            // copy the final job's output to the local output
            copyHDFSToLocal(conf, curOutput, "myoutput");
        } catch (Exception e) {
            throw e;
        } finally {
            if (runLocal) {
                try {
                    testCluster.tearDown();
                } catch (Exception e) {
                    System.out.println("Exception raised while tearing down the Test Cluster: " + e);
                }
            }
        }
    }

    public static void test(String[] args) throws Exception {
        GenomixJobConf conf = GenomixJobConf.fromArguments(args);
        GenomixDriver driver = new GenomixDriver();
        driver.runGenomix(conf);
    }

    public static void main(String[] args) throws CmdLineException, NumberFormatException, HyracksException, Exception {
        String[] myArgs = { "-runLocal", "-kmerLength", "3", 
                            "-ip", "127.0.0.1", "-port", "55", 
//                            "-inputDir", "/home/wbiesing/code/hyracks/genomix/genomix-pregelix/data/input/reads/synthetic/walk_random_seq1.txt",
//                            "-pipelineOrder", "BUILD,MERGE",
//                            "-inputDir", "/home/wbiesing/code/hyracks/genomix/genomix-driver/graphbuild.binmerge",
                            "-inputDir", "../genomix-pregelix/data/TestSet/PathMerge/CyclePath/bin/part-00000",
                            "-pipelineOrder", "MERGE"
                            };
        GenomixJobConf conf = GenomixJobConf.fromArguments(myArgs);
        GenomixDriver driver = new GenomixDriver();
        driver.runGenomix(conf);
    }
}
