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
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineException;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.config.GenomixJobConf.Patterns;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.graph.job.JobGenBrujinGraph;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;

/**
 * The main entry point for the Genomix assembler, a hyracks/pregelix/hadoop-based deBruijn assembler.
 * 
 */
public class GenomixDriver {
    
    String prevOutput;
    String curOutput;
    
    private void copyLocalToHDFS(JobConf conf, String localFile, String destFile) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        Path dest = new Path(destFile);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(new Path(localFile), dest);
    }
    
    public void runGenomix(GenomixJobConf conf) throws NumberFormatException, HyracksException, Exception {
        String origInput = conf.getWorkingDirectory() + File.separator + conf.get(GenomixJobConf.INITIAL_INPUT_DIR);
        curOutput = "/00-initial-input";
        
        TestCluster testCluster = new TestCluster();
        boolean runLocal = Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL));

        int stepNum = 0;
        try {
            if (runLocal) {
                testCluster.setUp(conf);
            }
            copyLocalToHDFS(conf, origInput, curOutput);
            
            // currently, we just iterate over the jobs set in conf[PIPELINE_ORDER].  In the future, we may want more logic to iterate multiple times, etc
            String pipelineSteps = conf.get(GenomixJobConf.PIPELINE_ORDER);
            for (Patterns step: Patterns.arrayFromString(pipelineSteps)) {
                switch(step) {
                    case BUILD:
                        prevOutput = curOutput;
                        curOutput = Patterns.BUILD + "-" + stepNum;
//                        conf.set("mapred.input.dir", prevOutput.toString());
//                        conf.set("mapred.output.dir", curOutput.toString());
                        FileInputFormat.setInputPaths(conf, new Path(prevOutput));
                        FileOutputFormat.setOutputPath(conf, new Path(curOutput));
                        Driver driver = new Driver(conf.get(GenomixJobConf.IP_ADDRESS), 
                                Integer.parseInt(conf.get(GenomixJobConf.PORT)), 
                                Integer.parseInt(conf.get(GenomixJobConf.CPARTITION_PER_MACHINE)));
                        driver.runJob(conf, Plan.BUILD_DEBRUJIN_GRAPH, Boolean.parseBoolean(conf.get(GenomixJobConf.PROFILE)));
                    case MERGE:
                    case MERGE_P4:
                        
                    case MERGE_P1:
                        
                    case MERGE_P2:
                        
                    case TIP_REMOVE:
                        
                    case BUBBLE:
                        
                    case LOW_COVERAGE:
                        
                    case BRIDGE:
                        
                    case SPLIT_REPEAT:
                        
                    case SCAFFOLD:
                        
                }
            }
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
        String[] myArgs = {"-runLocal", "-kmerLength", "5", "-ip", "127.0.0.1", "-port", "55", "-inputDir", "data/AddBridge/SimpleTest/text.txt"};
        GenomixJobConf conf = GenomixJobConf.fromArguments(myArgs);
        GenomixDriver driver = new GenomixDriver(); 
        driver.runGenomix(conf);
    }
}
