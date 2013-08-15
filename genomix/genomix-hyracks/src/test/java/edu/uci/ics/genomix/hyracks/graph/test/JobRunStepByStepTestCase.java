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
package edu.uci.ics.genomix.hyracks.graph.test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.genomix.hyracks.graph.driver.Driver;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.graph.job.GenomixJobConf;

@SuppressWarnings("deprecation")
public class JobRunStepByStepTestCase {
    private static final int KmerSize = 3;
    private static final int ReadLength = 7;
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

    private static final String DATA_INPUT_PATH = "src/test/resources/data/lastesttest/LowSplitRepeat.txt";
    private static final String HDFS_INPUT_PATH = "/webmap";
    private static final String HDFS_OUTPUT_PATH = "/webmap_result";
    
    private static final String DUMPED_RESULT = ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH + "/Test.txt";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private MiniDFSCluster dfsCluster;
    
    private JobConf conf = new JobConf();
    private int numberOfNC = 2;
    private int numPartitionPerMachine = 2;
    
    private Driver driver;
    
    @Test
    public void TestAll() throws Exception {
//        TestReader();
//        TestGroupby();
        TestGroupbyUnMerged();
    }
    
    
    public void TestReader() throws Exception {
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_TEXT);
        cleanUpReEntry();
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_TEXT);
        driver.runJob(new GenomixJobConf(conf), Plan.CHECK_KMERREADER, true);
        dumpResult();
    }
    
    public void TestGroupby() throws Exception {
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_TEXT);
        cleanUpReEntry();
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        driver.runJob(new GenomixJobConf(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
        dumpResult();
    }
    
    public void TestGroupbyUnMerged() throws Exception {
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_TEXT);
        cleanUpReEntry();
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        driver.runJob(new GenomixJobConf(conf), Plan.BUILD_UNMERGED_GRAPH, true);
        dumpResult();
    }
    
    @Before
    public void setUp() throws Exception {
        cleanupStores();
        edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.init();
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHDFS();

        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));

        conf.setInt(GenomixJobConf.KMER_LENGTH, KmerSize);
        conf.setInt(GenomixJobConf.READ_LENGTH, ReadLength);
        driver = new Driver(edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.CC_HOST,
                edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT, numPartitionPerMachine);
    }
    
    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    private void startHDFS() throws IOException {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(DATA_INPUT_PATH);
        Path dest = new Path(HDFS_INPUT_PATH);
        dfs.mkdirs(dest);
        // dfs.mkdirs(result);
        dfs.copyFromLocalFile(src, dest);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }
    
    private void cleanUpReEntry() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        if (lfs.exists(new Path(DUMPED_RESULT))) {
            lfs.delete(new Path(DUMPED_RESULT), true);
        }
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH), true);
        }
    }
    
    private void dumpResult() throws Exception {
        String format = conf.get(GenomixJobConf.OUTPUT_FORMAT);
        if (GenomixJobConf.OUTPUT_FORMAT_TEXT.equalsIgnoreCase(format)) {
            FileUtil.copyMerge(FileSystem.get(conf), new Path(HDFS_OUTPUT_PATH),
                    FileSystem.getLocal(new Configuration()), new Path(DUMPED_RESULT), false, conf, null);
        } 
    }
    
    @After
    public void tearDown() throws Exception {
        edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.deinit();
        cleanupHDFS();
    }

    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }
}
