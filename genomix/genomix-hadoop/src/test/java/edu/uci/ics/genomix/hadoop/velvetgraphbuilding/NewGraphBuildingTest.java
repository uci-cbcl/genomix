package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.Test;

@SuppressWarnings("deprecation")

public class NewGraphBuildingTest {
    
    private JobConf conf = new JobConf();
    private static final String ACTUAL_RESULT_DIR = "actual1";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String DATA_PATH = "data/webmap/text.txt";
    private static final String HDFS_PATH = "/webmap";
    private static final String RESULT_PATH = "/result1";
    private static final String EXPECTED_PATH = "expected/";
    private static final int COUNT_REDUCER = 2;
    private static final int SIZE_KMER = 5;
    private static final int READ_LENGTH = 8;
    
    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private FileSystem dfs;

    @SuppressWarnings("resource")
    @Test
    public void test() throws Exception {
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHadoop();
        TestGroupbyKmer();
        TestMapKmerToRead();
        cleanupHadoop();
    }

    public void TestGroupbyKmer() throws Exception {
        GraphBuildingDriver tldriver = new GraphBuildingDriver();
        tldriver.run(HDFS_PATH, RESULT_PATH, COUNT_REDUCER, SIZE_KMER, READ_LENGTH, true, false, HADOOP_CONF_PATH);
        dumpGroupByKmerResult();
    }

    public void TestMapKmerToRead() throws Exception {
        GraphBuildingDriver tldriver = new GraphBuildingDriver();
        tldriver.run(HDFS_PATH, RESULT_PATH, 0, SIZE_KMER, READ_LENGTH, false, false, HADOOP_CONF_PATH);
        dumpResult();
    }

    public void TestGroupByReadID() throws Exception {
        GraphBuildingDriver tldriver = new GraphBuildingDriver();
        tldriver.run(HDFS_PATH, RESULT_PATH, 2, SIZE_KMER, READ_LENGTH, false, false, HADOOP_CONF_PATH);
        dumpResult();
    }
    
    private void startHadoop() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, 2, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(4, dfs.getUri().toString(), 2);

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

    private void dumpGroupByKmerResult() throws IOException {
        Path src = new Path(HDFS_PATH + "-step1");
        Path dest = new Path(ACTUAL_RESULT_DIR);
        dfs.copyToLocalFile(src, dest);
    }
    
    private void dumpResult() throws IOException {
        Path src = new Path(RESULT_PATH);
        Path dest = new Path(ACTUAL_RESULT_DIR);
        dfs.copyToLocalFile(src, dest);
    }
}
