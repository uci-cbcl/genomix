package edu.uci.ics.genomix.hyracks.newgraph.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.junit.After;
import org.junit.Test;

import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.newgraph.driver.Driver;
import edu.uci.ics.genomix.hyracks.newgraph.driver.Driver.Plan;
import edu.uci.ics.genomix.hyracks.test.TestUtils;
import edu.uci.ics.genomix.oldtype.NodeWritable;

public class JobRun {
    private static final int KmerSize = 5;
    private static final int ReadLength = 8;
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

    private static final String DATA_INPUT_PATH = "src/test/resources/data/webmap/text.txt";
    private static final String HDFS_INPUT_PATH = "/webmap";
    private static final String HDFS_OUTPUT_PATH = "/webmap_result";

    private static final String EXPECTED_DIR = "src/test/resources/expected/";
    private static final String EXPECTED_READER_RESULT = EXPECTED_DIR + "result_after_initial_read";
    private static final String EXPECTED_OUPUT_KMER = EXPECTED_DIR + "result_after_kmerAggregate";
    private static final String EXPECTED_KMER_TO_READID = EXPECTED_DIR + "result_after_kmer2readId";
    private static final String EXPECTED_GROUPBYREADID = EXPECTED_DIR + "result_after_readIDAggreage";
    private static final String EXPECTED_OUPUT_NODE = EXPECTED_DIR + "result_after_generateNode";
    private static final String EXPECTED_UNMERGED = EXPECTED_DIR + "result_unmerged";

    private static final String DUMPED_RESULT = ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH + "/merged.txt";
    private static final String CONVERT_RESULT = DUMPED_RESULT + ".txt";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";;
    private MiniDFSCluster dfsCluster;
    
    private JobConf conf = new JobConf();
    private int numberOfNC = 2;
    private int numPartitionPerMachine = 2;
    
    private Driver driver;
    
    @Test
    public void TestAll() throws Exception {
        TestReader();
    }
    
    public void TestReader() throws Exception {
        cleanUpReEntry();
        conf.set(GenomixJobConf.OUTPUT_FORMAT, GenomixJobConf.OUTPUT_FORMAT_TEXT);
        driver.runJob(new GenomixJobConf(conf), Plan.CHECK_KMERREADER, true);
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
    
    @After
    public void tearDown() throws Exception {
        edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.deinit();
        cleanupHDFS();
    }

    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }
}
