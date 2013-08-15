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

import org.apache.hadoop.fs.Path;
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
    
    Path prevOutput;
    Path curOutput;
    
    public void runGenomix(GenomixJobConf conf) throws NumberFormatException, HyracksException {
        curOutput = new Path(conf.getWorkingDirectory(), conf.get(GenomixJobConf.INITIAL_INPUT_DIR));
        int stepNum = 0;
        
        // currently, we just iterate over the jobs set in conf[PIPELINE_ORDER].  In the future, we may want more logic to iterate multiple times, etc
        String pipelineSteps = conf.get(GenomixJobConf.PIPELINE_ORDER);
        for (Patterns step: Patterns.arrayFromString(pipelineSteps)) {
            switch(step) {
                case BUILD:
                    prevOutput = curOutput;
                    curOutput = new Path(conf.getWorkingDirectory(), Patterns.BUILD + "-" + stepNum);
                    conf.set("mapred.input.dir", prevOutput.toString());
                    conf.set("mapred.output.dir", curOutput.toString());
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
    }

    public static void test(String[] args) throws CmdLineException, NumberFormatException, HyracksException {
        GenomixJobConf conf = GenomixJobConf.fromArguments(args);
        GenomixDriver driver = new GenomixDriver(); 
        driver.runGenomix(conf);
    }
    

    public static void main(String[] args) throws CmdLineException, NumberFormatException, HyracksException {
        String[] myArgs = {"-kmerLength", "5", "-ip", "127.0.0.1", "-port", "55", "-inputDir", "/home/wbiesing/code/hyracks/genomix/genomix-pregelix/data/AddBridge/SimpleTest"};
        GenomixJobConf conf = GenomixJobConf.fromArguments(myArgs);
        GenomixDriver driver = new GenomixDriver(); 
        driver.runGenomix(conf);
    }
}
