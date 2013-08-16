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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.config.GenomixJobConf.Patterns;
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
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;

/**
 * The main entry point for the Genomix assembler, a hyracks/pregelix/hadoop-based deBruijn assembler.
 * 
 */
public class GenomixDriver {
    
    private static final String CONF_XML = "conf.xml";
    
    private String prevOutput;
    private String curOutput;
    private int stepNum;
    
    private edu.uci.ics.genomix.hyracks.graph.driver.Driver hyracksDriver;
    private edu.uci.ics.pregelix.core.driver.Driver pregelixDriver;
    
    private void copyLocalToHDFS(JobConf conf, String localFile, String destFile) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        Path dest = new Path(destFile);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(new Path(localFile), dest);
    }
    
    private void copyHDFSToLocal(GenomixJobConf conf, String hdfsSrc, String localDest) throws Exception {
        FileUtils.deleteDirectory(new File(localDest));
        FileUtil.copy(FileSystem.get(conf), new Path(hdfsSrc),
                    FileSystem.getLocal(new Configuration()), new Path(localDest), false, conf);
    }
    
    private void buildGraph(GenomixJobConf conf) throws NumberFormatException, HyracksException {
        hyracksDriver = new edu.uci.ics.genomix.hyracks.graph.driver.Driver(conf.get(GenomixJobConf.IP_ADDRESS),
                Integer.parseInt(conf.get(GenomixJobConf.PORT)),
                Integer.parseInt(conf.get(GenomixJobConf.CPARTITION_PER_MACHINE)));
        hyracksDriver.runJob(conf, Plan.BUILD_DEBRUJIN_GRAPH, Boolean.parseBoolean(conf.get(GenomixJobConf.PROFILE)));
    }
    
    private void setOutput(GenomixJobConf conf, String string) {
        prevOutput = curOutput;
        curOutput = File.separator + String.format("%02d-", stepNum) + Patterns.MERGE_P1;
        FileInputFormat.setInputPaths(conf, new Path(prevOutput));
        FileOutputFormat.setOutputPath(conf, new Path(curOutput));
    }
    
    public void runGenomix(GenomixJobConf conf) throws NumberFormatException, HyracksException, Exception {
        String origInput = conf.get(GenomixJobConf.INITIAL_INPUT_DIR);
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
            
            List<PregelixJob> jobs = new ArrayList<PregelixJob>();
            
            // currently, we just iterate over the jobs set in conf[PIPELINE_ORDER].  In the future, we may want more logic to iterate multiple times, etc
            String pipelineSteps = conf.get(GenomixJobConf.PIPELINE_ORDER);
            for (Patterns step: Patterns.arrayFromString(pipelineSteps)) {
                stepNum++;
                switch(step) {
                    case BUILD:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.BUILD);
                        buildGraph(conf);
                        break;
                    case MERGE:
                    case MERGE_P1:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.MERGE_P1);
                        jobs.add(P1ForPathMergeVertex.getConfiguredJob(conf));
                        break;
                    case MERGE_P2:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.MERGE_P2);
                        jobs.add(P2ForPathMergeVertex.getConfiguredJob(conf));
                        break;
                    case MERGE_P4:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.MERGE_P4);
                        jobs.add(P4ForPathMergeVertex.getConfiguredJob(conf));
                        break;
                    case TIP_REMOVE:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.TIP_REMOVE);
                        jobs.add(TipRemoveVertex.getConfiguredJob(conf));
                        break;
                    case BUBBLE:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.BUBBLE);
                        jobs.add(BubbleMergeVertex.getConfiguredJob(conf));
                        break;
                    case LOW_COVERAGE:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.LOW_COVERAGE);
                        jobs.add(RemoveLowCoverageVertex.getConfiguredJob(conf));
                        break;
                    case BRIDGE:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.BRIDGE);
                        jobs.add(BridgeRemoveVertex.getConfiguredJob(conf));
                        break;
                    case SPLIT_REPEAT:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.SPLIT_REPEAT);
                        jobs.add(SplitRepeatVertex.getConfiguredJob(conf));
                        break;
                    case SCAFFOLD:
                        setOutput(conf, File.separator + String.format("%02d-", stepNum) + Patterns.SCAFFOLD);
                        jobs.add(ScaffoldingVertex.getConfiguredJob(conf));
                        break;
                }
            }
            for (int i=0; i < jobs.size(); i++) {
                pregelixDriver = new edu.uci.ics.pregelix.core.driver.Driver(P1ForPathMergeVertex.class);
//                hyracksDriver = new edu.uci.ics.genomix.hyracks.graph.driver.Driver(conf.get(GenomixJobConf.IP_ADDRESS),
//                        Integer.parseInt(conf.get(GenomixJobConf.PORT)),
//                        Integer.parseInt(conf.get(GenomixJobConf.CPARTITION_PER_MACHINE)));
                pregelixDriver.runJob(jobs.get(i), conf.get(GenomixJobConf.IP_ADDRESS), Integer.parseInt(conf.get(GenomixJobConf.PORT)));
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
        String[] myArgs = {"-runLocal", "-kmerLength", "5", "-ip", "127.0.0.1", "-port", "55", "-inputDir", "data/AddBridge/SimpleTest/text.txt", "-pipelineOrder", "BUILD,MERGE,MERGE"};
        GenomixJobConf conf = GenomixJobConf.fromArguments(myArgs);
        GenomixDriver driver = new GenomixDriver(); 
        driver.runGenomix(conf);
    }
}
