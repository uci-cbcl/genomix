/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.genomix.pregelix.graph.GenerateGraphViz;
import edu.uci.ics.genomix.pregelix.io.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.VLongWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.sequencefile.GenerateTextFile;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

public class BasicSmallTestCase extends TestCase {
    private final PregelixJob job;
    private final String resultFileDir;
    private final String textFileDir;
    private final String graphvizFileDir;
    private final String jobFile;
    private final Driver driver = new Driver(this.getClass());
    private final FileSystem dfs;

    public BasicSmallTestCase(String hadoopConfPath, String jobName, String jobFile, FileSystem dfs,
            String hdfsInput, String resultFile, String textFile, String graphvizFile) throws Exception {
        super("test");
        this.jobFile = jobFile;
        this.job = new PregelixJob("test");
        this.job.getConfiguration().addResource(new Path(jobFile));
        this.job.getConfiguration().addResource(new Path(hadoopConfPath));
        FileInputFormat.setInputPaths(job, hdfsInput);
        FileOutputFormat.setOutputPath(job, new Path(hdfsInput + "_result"));
        job.setJobName(jobName);
        this.resultFileDir = resultFile;
        this.textFileDir = textFile;
        this.graphvizFileDir = graphvizFile;

        this.dfs = dfs;
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    public void outputCounters(HashMapWritable<ByteWritable, VLongWritable> counters){
        String output = "";
        for(ByteWritable counterName : counters.keySet()){
            output += StatisticsCounter.COUNTER_CONTENT.getContent(counterName.get());
            output += ": ";
            output += counters.get(counterName).toString() + "\n";
        }
        System.out.println(output);
    }
    
    @Test
    public void test() throws Exception {
        setUp();
        Plan[] plans = new Plan[] { Plan.OUTER_JOIN };
        for (Plan plan : plans) {
            driver.runJob(job, plan, PregelixHyracksIntegrationUtil.CC_HOST,
                    PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT, false);
//            HashMapWritable<ByteWritable, VLongWritable> counters = BasicGraphCleanVertex.readStatisticsCounterResult(job.getConfiguration());
//            //output counters
//            outputCounters(counters);
//            System.out.println("");
        }
        compareResults();
        tearDown();
        waitawhile();
    }

    private void compareResults() throws Exception {
        dfs.copyToLocalFile(FileOutputFormat.getOutputPath(job), new Path(resultFileDir));
        GenerateTextFile.generateFromPathmergeResult(3, resultFileDir, textFileDir);
        GenerateGraphViz.convertGraphCleanOutputToGraphViz(resultFileDir, graphvizFileDir);
    }

    public String toString() {
        return jobFile;
    }

}
