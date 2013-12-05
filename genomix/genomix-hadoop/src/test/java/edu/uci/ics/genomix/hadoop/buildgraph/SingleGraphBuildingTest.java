package edu.uci.ics.genomix.hadoop.buildgraph;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.Test;

import edu.uci.ics.genomix.data.cluster.GenomixClusterManager;
import edu.uci.ics.genomix.data.utils.TestUtils;
import edu.uci.ics.genomix.hadoop.buildgraph.GenomixHadoopDriver;

@SuppressWarnings("deprecation")
public class SingleGraphBuildingTest {

    private JobConf conf = new JobConf();
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String ACTUAL_RESULT = ACTUAL_RESULT_DIR + "/data";
    
    private static final String EXPECTED_RESULT_DIR = "src/test/resources/expected/data";
    private static final String EXPECTED_BRUIJIN_GRAPH_RESULT = EXPECTED_RESULT_DIR + "/smalltest-graph-result.txt";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String DATA_PATH = "data/webmap/lastesttest/SmallTest/SmallTest.txt";
    private static final String HDFS_PATH = "/webmap";
    private static final String HDFS_RESULT_PATH = "/result";

    private static final int COUNT_REDUCER = 2;
    private static final int SIZE_KMER = 3;
    private static int LINES_PERMAP = 4 * 100000;

    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private FileSystem dfs;

    @Test
    public void test() throws Exception {
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHadoop();
        TestMapKmerToNode();
        cleanupHadoop();
        TestUtils.compareFilesBySortingThemLineByLine(new File(EXPECTED_BRUIJIN_GRAPH_RESULT), new File(ACTUAL_RESULT));
    }

    public void TestMapKmerToNode() throws Exception {
        GenomixHadoopDriver driver = new GenomixHadoopDriver();
        driver.run(HDFS_PATH, HDFS_RESULT_PATH, COUNT_REDUCER, SIZE_KMER, LINES_PERMAP, true, HADOOP_CONF_PATH);
        dumpResult();
    }

    private void startHadoop() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(1, dfs.getUri().toString(), 1);

        Path src = new Path(DATA_PATH);
        Path dest = new Path(HDFS_PATH + "/");
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    private void cleanupHadoop() throws IOException {
        mrCluster.shutdown();
        dfsCluster.shutdown();
    }

    private void dumpResult() throws IOException {
        GenomixClusterManager.copyBinAndTextToLocal(conf, HDFS_RESULT_PATH, ACTUAL_RESULT_DIR);
    }
}
