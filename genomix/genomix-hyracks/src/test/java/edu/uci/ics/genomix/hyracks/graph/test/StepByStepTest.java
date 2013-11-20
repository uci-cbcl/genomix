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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver.Plan;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager;
import edu.uci.ics.genomix.util.TestUtils;

/**
 * this StepByStepTestCase only applied on OutputTextFormt
 */
public class StepByStepTest {
    private static final int TEST_KMER_SIZE = 3;

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

    private static final String LOCAL_INPUT_PATH = "src/test/resources/data/input/smalltest.txt";
    private static final String HDFS_INPUT_PATH = "/webmap";
    private static final String HDFS_OUTPUT_PATH = "/webmap_result";

    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String ACTUAL_RESULT = ACTUAL_RESULT_DIR + "/data";

    private static final String EXPECTED_RESULT_DIR = "src/test/resources/data/expected";
    private static final String EXPECTED_READ_PARSER_RESULT = EXPECTED_RESULT_DIR + "/smalltest-parser-result.txt";
    private static final String EXPECTED_BRUIJIN_GRAPH_RESULT = EXPECTED_RESULT_DIR + "/smalltest-graph-result.txt";

    private GenomixClusterManager manager;
    private GenomixJobConf conf = new GenomixJobConf(TEST_KMER_SIZE);
    private int numberOfNC = 2;
    private int numPartitionPerMachine = 2;

    private GenomixHyracksDriver driver;

    @Test
    public void TestAll() throws Exception {
       // TestReader();
        TestGroupby();
    }

    public void TestReader() throws Exception {
        cleanUpDirectory();
        driver.runJob(conf, Plan.BUILD_READ_PARSER, true);
        GenomixClusterManager.copyBinToLocal(conf, HDFS_OUTPUT_PATH, ACTUAL_RESULT_DIR);
        TestUtils.compareFilesBySortingThemLineByLine(new File(EXPECTED_READ_PARSER_RESULT), new File(ACTUAL_RESULT));
    }

    public void TestGroupby() throws Exception {
        cleanUpDirectory();
        driver.runJob(conf, Plan.BUILD_DEBRUIJN_GRAPH, true);
        GenomixClusterManager.copyBinToLocal(conf, HDFS_OUTPUT_PATH, ACTUAL_RESULT_DIR);
        TestUtils.compareFilesBySortingThemLineByLine(new File(EXPECTED_BRUIJIN_GRAPH_RESULT), new File(ACTUAL_RESULT));
    }

    @Before
    public void setUp() throws Exception {
        conf.setBoolean(GenomixJobConf.RUN_LOCAL, true);
        conf.setInt(GenomixJobConf.FRAME_SIZE, 65535);
        conf.setInt(GenomixJobConf.FRAME_LIMIT, 4096);

        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));

        conf.setInt(GenomixJobConf.KMER_LENGTH, TEST_KMER_SIZE);
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));

        manager = new GenomixClusterManager(true, conf);
        manager.setNumberOfNC(numberOfNC);
        manager.setNumberOfDataNodesInLocalMiniHDFS(numberOfNC);
        manager.startCluster();

        GenomixClusterManager.copyLocalToHDFS(conf, LOCAL_INPUT_PATH, HDFS_INPUT_PATH);

        FileInputFormat.setInputPaths(conf, new Path(HDFS_INPUT_PATH));
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));

        driver = new GenomixHyracksDriver(GenomixClusterManager.LOCAL_HOSTNAME, GenomixClusterManager.LOCAL_HYRACKS_CLIENT_PORT,
                numPartitionPerMachine);
    }

    private void cleanUpDirectory() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        if (lfs.exists(new Path(ACTUAL_RESULT))) {
            lfs.delete(new Path(ACTUAL_RESULT), true);
        }
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH), true);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (manager != null) {
            manager.stopCluster();
        }
    }

}
