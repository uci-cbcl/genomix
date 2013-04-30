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

package edu.uci.ics.genomix.pregelix.pathmerge;

import java.io.File;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.genomix.pregelix.example.util.TestUtils;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

public class PathMergeSmallTestCase extends TestCase {
    private static String HDFS_INPUTPATH = "/BridgePath";
    private static String HDFS_OUTPUTPAH = "/resultBridgePath";

    /*private static String HDFS_INPUTPATH2 = "/CyclePath";
    private static String HDFS_OUTPUTPAH2 = "/resultCyclePath";

    private static String HDFS_INPUTPATH3 = "/LongPath";
    private static String HDFS_OUTPUTPAH3 = "/resultLongPath";

    private static String HDFS_INPUTPATH4 = "/Path";
    private static String HDFS_OUTPUTPAH4 = "/resultPath";

    private static String HDFS_INPUTPATH5 = "/SimplePath";
    private static String HDFS_OUTPUTPAH5 = "/resultSimplePath";
    
    private static String HDFS_INPUTPATH6 = "/SinglePath";
    private static String HDFS_OUTPUTPAH6 = "/resultSinglePath";
    
    private static String HDFS_INPUTPATH7 = "/TreePath";
    private static String HDFS_OUTPUTPAH7 = "/resultTreePath";*/

    private final PregelixJob job;
    private final String resultFileDir;
    private final String jobFile;
    private final Driver driver = new Driver(this.getClass());
    private final FileSystem dfs;

    public PathMergeSmallTestCase(String hadoopConfPath, String jobName, String jobFile, String resultFile,
            FileSystem dfs) throws Exception {
        super("test");
        this.jobFile = jobFile;
        this.job = new PregelixJob("test");
        this.job.getConfiguration().addResource(new Path(jobFile));
        this.job.getConfiguration().addResource(new Path(hadoopConfPath));
        Path[] inputPaths = FileInputFormat.getInputPaths(job);
        if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH));
        } 
        /*else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH2)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH2);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH2));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH3)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH3);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH3));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH4)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH4);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH4));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH5)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH5);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH5));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH6)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH6);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH6));
        } else if (inputPaths[0].toString().endsWith(HDFS_INPUTPATH7)) {
            FileInputFormat.setInputPaths(job, HDFS_INPUTPATH7);
            FileOutputFormat.setOutputPath(job, new Path(HDFS_OUTPUTPAH7));
        }*/
        job.setJobName(jobName);
        this.resultFileDir = resultFile;
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
        dfs.copyToLocalFile(FileOutputFormat.getOutputPath(job), new Path(resultFileDir));
        //TestUtils.compareWithResultDir(new File(expectedFileDir), new File(resultFileDir));
    }

    public String toString() {
        return jobFile;
    }
}
