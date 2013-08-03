package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.Test;

import edu.uci.ics.genomix.hadoop.pmcommon.HadoopMiniClusterTest;

@SuppressWarnings("deprecation")
public class GraphBuildingTestCase extends TestCase{

    private final String RESULT_PATH;
    private final String HADOOP_CONF_PATH;
    private final String HDFS_INPUTPATH;
    private FileSystem dfs;

    private static final int COUNT_REDUCER = 1;
    private final int SIZE_KMER;
        
    public GraphBuildingTestCase(String resultFileDir, String hadoopConfPath,
            String hdfsInputPath, int kmerSize, FileSystem dfs){
        this.RESULT_PATH = resultFileDir;
        this.HADOOP_CONF_PATH = hadoopConfPath;
        this.HDFS_INPUTPATH = hdfsInputPath;
        this.SIZE_KMER = kmerSize;
        this.dfs = dfs;
    }
    
    @Test
    public void test() throws Exception {
        TestMapKmerToNode();
    }
    
    public void TestMapKmerToNode() throws Exception {
        GenomixDriver driver = new GenomixDriver();
        driver.run(HDFS_INPUTPATH, RESULT_PATH, COUNT_REDUCER, SIZE_KMER, true, HADOOP_CONF_PATH);
        dumpResult();
    }
    
    
    
    private void dumpResult() throws IOException {
        Path src = new Path(RESULT_PATH);
        Path dest = new Path(RESULT_PATH);
        dfs.copyToLocalFile(src, dest);
//        HadoopMiniClusterTest.copyResultsToLocal(RESULT_PATH, "actual/test.txt", false, conf, true, dfs);
    }
}
