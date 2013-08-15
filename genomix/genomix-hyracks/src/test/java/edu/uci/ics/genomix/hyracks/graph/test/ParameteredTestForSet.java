package edu.uci.ics.genomix.hyracks.graph.test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.genomix.hadoop.graph.test.HadoopMiniClusterTest;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver;
import edu.uci.ics.genomix.hyracks.graph.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.graph.job.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.graph.test.TestSet.DirType;

@RunWith(value = Parameterized.class)
public class ParameteredTestForSet {
    public static final DirType temp = DirType.SPLITREPEAT;

    public String dataPath;
    
    public ParameteredTestForSet(String otherPath) {
        this.dataPath = otherPath;
    }

    @Parameters
    public static Collection<Object[]> getdataPath() {
        Collection<Object[]> data = new ArrayList<Object[]>();
        TestSet ts = new TestSet(temp);
        String [] dirSet = ts.getTestDir();
        for (String testDirPointer : dirSet) {
            data.add(new Object[] { testDirPointer });
        }
        return data;
    }

    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String HDFS_INPUT_PATH = "/webmap";
    private static final String HDFS_OUTPUT_PATH = "/webmap_result";

    private static final int KmerSize = 3;
    private static MiniDFSCluster dfsCluster;
    private static FileSystem dfs;
    private static JobConf conf = new JobConf();
    private static int numberOfNC = 2;
    private static int numPartitionPerMachine = 2;
    private static Driver driver;

    @BeforeClass
    public static void setUp() throws Exception {
        cleanupStores();
        edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.init();
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHDFS();
        conf.setInt(GenomixJobConf.KMER_LENGTH, KmerSize);
        driver = new Driver(edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.CC_HOST,
                edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT, numPartitionPerMachine);
    }

    private static void startHDFS() throws IOException {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
        dfs = FileSystem.get(conf);
        
        TestSet ts = new TestSet(temp);
        String [] dirSet = ts.getTestDir();
        for (String testDir : dirSet) {
            File src = new File(testDir);
            Path dest = new Path(HDFS_INPUT_PATH + File.separator + src.getName());
            dfs.mkdirs(dest);
            for (File f : src.listFiles()) {
                dfs.copyFromLocalFile(new Path(f.getAbsolutePath()), dest);
            }
        }

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    private static void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void TestGroupbyUnMerged() throws Exception {
        waitawhile();
        cleanUpReEntry();
        File src = new File(dataPath);
        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH + File.separator + src.getName());
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH + File.separator + src.getName()));
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_BINARY);
        conf.set(GenomixJobConf.GROUPBY_TYPE, GenomixJobConf.GROUPBY_TYPE_PRECLUSTER);
        
        driver.runJob(new GenomixJobConf(conf), Plan.BUILD_UNMERGED_GRAPH, true);
        dumpResult();
        
//        Assert.assertEquals(true,
//                TestUtils.compareWithSortedResult(new File(DUMPED_RESULT), new File(EXPECTED_UNMERGED)));
    }

    private void cleanUpReEntry() throws IOException {
        File src = new File(dataPath);
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH + File.separator + src.getName()))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH + File.separator + src.getName()), true);
        }
    }

    public void dumpResult() throws IOException {
        File src = new File(dataPath);
        HadoopMiniClusterTest.copyResultsToLocal(HDFS_OUTPUT_PATH + File.separator + src.getName(), ACTUAL_RESULT_DIR + File.separator + 
                 File.separator + src.getName() + "/test.txt", false, conf, true, dfs);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.deinit();
        cleanupHDFS();
    }

    private static void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }
}