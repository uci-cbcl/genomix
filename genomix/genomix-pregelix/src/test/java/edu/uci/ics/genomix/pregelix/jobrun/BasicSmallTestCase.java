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

package edu.uci.ics.genomix.pregelix.jobrun;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.genomix.minicluster.GenerateGraphViz;
import edu.uci.ics.genomix.minicluster.GenerateGraphViz.GRAPH_TYPE;
import edu.uci.ics.genomix.pregelix.io.common.ByteWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.sequencefile.GenerateTextFile;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.util.TestUtils;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

public class BasicSmallTestCase extends TestCase {
    private final PregelixJob job;
    private final String binFileDir;
    private final String textFileDir;
    private final String graphvizFileDir;
    private final String statisticsFileDir;
    private final String expectedFileDir;
    private final String jobFile;
    private final Driver driver = new Driver(this.getClass());
    private final FileSystem dfs;

    public BasicSmallTestCase(String hadoopConfPath, String jobName, String jobFile, FileSystem dfs, String hdfsInput,
            String resultFile, String textFile, String graphvizFile, String statisticsFile, String expectedFile)
            throws Exception {
        super("test");
        this.jobFile = jobFile;
        this.job = new PregelixJob("test");
        this.job.getConfiguration().addResource(new Path(jobFile));
        this.job.getConfiguration().addResource(new Path(hadoopConfPath));
        FileInputFormat.setInputPaths(job, hdfsInput);
        FileOutputFormat.setOutputPath(job, new Path(hdfsInput + "_result"));
        job.setJobName(jobName);
        this.binFileDir = resultFile;
        this.textFileDir = textFile;
        this.graphvizFileDir = graphvizFile;
        this.statisticsFileDir = statisticsFile;
        this.expectedFileDir = expectedFile;

        this.dfs = dfs;
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    @Test
    public void test() throws Exception {
        setUp();
        Plan[] plans = new Plan[] { Plan.OUTER_JOIN };
        for (Plan plan : plans) {
            driver.runJob(job, plan, PregelixHyracksIntegrationUtil.CC_HOST,
                    PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT, false);
        }
        compareResults();
        tearDown();
        waitawhile();
    }

    private void compareResults() throws Exception {
        //copy bin to local
        dfs.copyToLocalFile(FileOutputFormat.getOutputPath(job), new Path(binFileDir));
        //covert bin to text
        GenerateTextFile.convertGraphCleanOutputToText(binFileDir, textFileDir);
        //covert bin to graphviz
        GenerateGraphViz
                .writeLocalBinToLocalSvg(binFileDir, graphvizFileDir, GRAPH_TYPE.DIRECTED_GRAPH_WITH_ALLDETAILS);
        // compare results
        TestUtils.compareFilesBySortingThemLineByLine(new File(expectedFileDir), new File(textFileDir));
        //generate statistic counters
        //        generateStatisticsResult(statisticsFileDir);
    }

    public void generateStatisticsResult(String outPutDir) throws IOException {
        //convert Counters to string
        HashMapWritable<ByteWritable, VLongWritable> counters = DeBruijnGraphCleanVertex
                .readStatisticsCounterResult(job.getConfiguration());
        String output = convertCountersToString(counters);

        //output Counters
        Configuration conf = new Configuration();
        FileSystem fileSys = FileSystem.getLocal(conf);

        fileSys.create(new Path(outPutDir));
        BufferedWriter bw = new BufferedWriter(new FileWriter(outPutDir));
        bw.write(output);
        bw.close();
    }

    public String convertCountersToString(HashMapWritable<ByteWritable, VLongWritable> counters) {
        String output = "";
        for (ByteWritable counterName : counters.keySet()) {
            output += StatisticsCounter.COUNTER_CONTENT.getContent(counterName.get());
            output += ": ";
            output += counters.get(counterName).toString() + "\n";
        }
        return output;
    }

    public String toString() {
        return jobFile;
    }

}
