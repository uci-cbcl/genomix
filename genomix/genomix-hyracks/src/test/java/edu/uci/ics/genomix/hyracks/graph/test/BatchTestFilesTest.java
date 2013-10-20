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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.minicluster.GenerateGraphViz;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager.ClusterType;

@RunWith(value = Parameterized.class)
public class BatchTestFilesTest {

    private final static GraphBuildingTestSetting testsetting = new GraphBuildingTestSetting();

    /**
     * This test utility will feed the argument in Collection<Object[]> to Test constructor each time.
     * Then we can work on the different test data set sequentially.
     * 
     * @return
     * @throws Exception
     */
    @Parameters
    public static Collection<Object[]> fillConstructorArguments() throws Exception {
        Collection<Object[]> data = new ArrayList<Object[]>();
        File dir = new File(SEQUENCE_FILE_DIR);
        for (File subdir : dir.listFiles()) {
            if (!subdir.isDirectory()) {
                continue;
            }
            for (File file : subdir.listFiles(testsetting.getFilenameFilter())) {
                String[] args = testsetting.parseArgumentFromFilename(file.getName());
                Assert.assertTrue(args.length == 1);
                data.add(new Object[] { subdir.getName(), file, Integer.parseInt(args[0]) });
            }
        }
        return data;
    }

    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String SEQUENCE_FILE_DIR = "src/test/resources/data/sequence";
    private static final String ACTUAL_RESULT_DIR = "actual/sequence";
    private static final String HDFS_INPUT_PATH = "/webmap";
    private static final String HDFS_OUTPUT_PATH = "/webmap_result";

    private static GenomixClusterManager manager;
    private static GenomixJobConf conf = new GenomixJobConf(3);
    private static int numberOfNC = 2;
    private static int numPartitionPerMachine = 2;

    private static Driver driver;

    // Instance member
    private final String testDirName;
    private final File testFile;
    private final int kmerSize;
    private final String resultFileName;
    private final String expectFileName;

    public BatchTestFilesTest(String subdir, File file, int kmerSize) throws IOException {
        this.testDirName = subdir;
        this.testFile = file;
        this.kmerSize = kmerSize;

        this.resultFileName = ACTUAL_RESULT_DIR + File.separator + testDirName + File.separator + testFile.getName()
                + File.separator + "data";
        this.expectFileName = testFile.getAbsolutePath().replaceFirst(GraphBuildingTestSetting.TEST_FILE_EXTENSION,
                ".expected.txt");
        System.out.print(expectFileName);
    }

    @BeforeClass
    public static void setUp() throws Exception {

        conf.setBoolean(GenomixJobConf.RUN_LOCAL, true);
        conf.setInt(GenomixJobConf.FRAME_SIZE, 65535);
        conf.setInt(GenomixJobConf.FRAME_LIMIT, 4096);

        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));

        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));

        conf.setInt(GenomixJobConf.FRAME_SIZE, 65535);
        conf.setInt(GenomixJobConf.FRAME_LIMIT, 4096);

        manager = new GenomixClusterManager(true, conf);
        manager.setNumberOfNC(numberOfNC);
        manager.setNumberOfDataNodesInLocalMiniHDFS(numberOfNC);
        manager.startCluster(ClusterType.HYRACKS);
        manager.startCluster(ClusterType.HADOOP);

        driver = new Driver(GenomixClusterManager.LOCAL_HOSTNAME, GenomixClusterManager.LOCAL_HYRACKS_CLIENT_PORT,
                numPartitionPerMachine);
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    @Test
    public void TestGroupby() throws Exception {
        waitawhile();
        prepareData();
        conf.setInt(GenomixJobConf.KMER_LENGTH, this.kmerSize);
        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH + File.separator + testFile.getName());
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH + File.separator + testFile.getName()));

        driver.runJob(conf, Plan.BUILD_DEBRUIJN_GRAPH, true);
        checkResult();
    }

    private void prepareData() throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH), true);
        }
        if (dfs.exists(new Path(HDFS_INPUT_PATH))) {
            dfs.delete(new Path(HDFS_INPUT_PATH), true);
        }
        GenomixClusterManager.copyLocalToHDFS(conf, testFile.getAbsolutePath(), HDFS_INPUT_PATH + File.separator
                + testFile.getName());
    }

    public void checkResult() throws Exception {
        Path path = new Path(resultFileName);
        GenomixClusterManager.copyBinToLocal(conf, HDFS_OUTPUT_PATH + File.separator + testFile.getName(), path
                .getParent().toString());
        GenerateGraphViz.convertGraphBuildingOutputToGraphViz(path.getParent().toString() + "/bin", path.getParent()
                .toString() + "/graphviz");
        TestUtils.compareFilesBySortingThemLineByLine(new File(resultFileName), new File(expectFileName));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (manager != null) {
            manager.stopCluster(ClusterType.HADOOP);
            manager.stopCluster(ClusterType.HYRACKS);
        }
    }

}