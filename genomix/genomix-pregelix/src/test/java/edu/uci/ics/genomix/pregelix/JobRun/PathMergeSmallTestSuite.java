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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;

import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

@SuppressWarnings("deprecation")
public class PathMergeSmallTestSuite extends TestSuite {
    private static final Logger LOGGER = Logger.getLogger(PathMergeSmallTestSuite.class.getName());

    public static final String PreFix = "data/actual"; //"graphbuildresult";
    public static final String[] TestDir = { PreFix + File.separator
    + "bubblemerge/BubbleMergeGraph/bin/small_bubble"};
    //+ "tipremove/TipRemoveGraph/bin/fr_with_tip"};
    //+ "graphs/pathmerge/singleread"};
    //+ "bridgeadd/BridgeAddGraph/bin/tworeads"};
    /*+ "2", PreFix + File.separator
    + "3", PreFix + File.separator
    + "4", PreFix + File.separator
    + "5", PreFix + File.separator
    + "6", PreFix + File.separator
    + "7", PreFix + File.separator
    + "8", PreFix + File.separator
    + "9", PreFix + File.separator
    + "tworeads3", PreFix + File.separator
    + "tworeads_6"};*/
    private static final String ACTUAL_RESULT_DIR = "data/actual/pathmerge";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String PATH_TO_CLUSTER_STORE = "src/test/resources/cluster/stores.properties";
    private static final String PATH_TO_CLUSTER_PROPERTIES = "src/test/resources/cluster/cluster.properties";
    private static final String PATH_TO_JOBS = "src/test/resources/jobs/";
    private static final String PATH_TO_ONLY = "src/test/resources/only_pathmerge.txt";

    public static final String HDFS_INPUTPATH = "/PathTestSet";

    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private MiniDFSCluster dfsCluster;

    private JobConf conf = new JobConf();
    private int numberOfNC = 2;

    public void setUp() throws Exception {
        ClusterConfig.setStorePath(PATH_TO_CLUSTER_STORE);
        ClusterConfig.setClusterPropertiesPath(PATH_TO_CLUSTER_PROPERTIES);
        cleanupStores();
        PregelixHyracksIntegrationUtil.init("src/test/resources/topology.xml");
        LOGGER.info("Hyracks mini-cluster started");
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHDFS();
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

        for (String testDir : TestDir) {
            File src = new File(testDir);
            Path dest = new Path(HDFS_INPUTPATH + File.separator + src.getName());
            dfs.mkdirs(dest);
            //src.listFiles()
            //src.listFiles((FilenameFilter)(new WildcardFileFilter("part*")))
            for (File f : src.listFiles()) {
                dfs.copyFromLocalFile(new Path(f.getAbsolutePath()), dest);
            }
        }

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    /**
     * cleanup hdfs cluster
     */
    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }

    public void tearDown() throws Exception {
        PregelixHyracksIntegrationUtil.deinit();
        LOGGER.info("Hyracks mini-cluster shut down");
        cleanupHDFS();
    }

    public static Test suite() throws Exception {
        List<String> onlys = getFileList(PATH_TO_ONLY);
        File testData = new File(PATH_TO_JOBS);
        File[] queries = testData.listFiles();
        PathMergeSmallTestSuite testSuite = new PathMergeSmallTestSuite();
        testSuite.setUp();
        boolean onlyEnabled = false;
        FileSystem dfs = FileSystem.get(testSuite.conf);

        if (onlys.size() > 0) {
            onlyEnabled = true;
        }

        for (File qFile : queries) {
            if (qFile.isFile()) {
                if (onlyEnabled && !isInList(onlys, qFile.getName())) {
                    continue;
                } else {
                    for (String testPathStr : TestDir) {
                        File testDir = new File(testPathStr);
                        String resultFileName = ACTUAL_RESULT_DIR + File.separator + jobExtToResExt(qFile.getName())
                                + File.separator + "bin" + File.separator + testDir.getName();
                        String textFileName = ACTUAL_RESULT_DIR + File.separator + jobExtToResExt(qFile.getName())
                                + File.separator + "txt" + File.separator + testDir.getName();
                        testSuite.addTest(new BasicSmallTestCase(HADOOP_CONF_PATH, qFile.getName(), qFile
                                .getAbsolutePath().toString(), dfs,
                                HDFS_INPUTPATH + File.separator + testDir.getName(), resultFileName, textFileName));
                    }
                }
            }
        }
        return testSuite;
    }

    /**
     * Runs the tests and collects their result in a TestResult.
     */
    @Override
    public void run(TestResult result) {
        try {
            int testCount = countTestCases();
            for (int i = 0; i < testCount; i++) {
                // cleanupStores();
                Test each = this.testAt(i);
                if (result.shouldStop())
                    break;
                runTest(each, result);
            }
            tearDown();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected static List<String> getFileList(String ignorePath) throws FileNotFoundException, IOException {
        BufferedReader reader = new BufferedReader(new FileReader(ignorePath));
        String s = null;
        List<String> ignores = new ArrayList<String>();
        while ((s = reader.readLine()) != null) {
            ignores.add(s);
        }
        reader.close();
        return ignores;
    }

    private static String jobExtToResExt(String fname) {
        int dot = fname.lastIndexOf('.');
        return fname.substring(0, dot);
    }

    private static boolean isInList(List<String> onlys, String name) {
        for (String only : onlys)
            if (name.indexOf(only) >= 0)
                return true;
        return false;
    }

}
