package edu.uci.ics.graphcountfilter;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.utils.TestUtils;


@SuppressWarnings("deprecation")
public class CountFilterTest {
    private static final String ACTUAL_RESULT_DIR = "actual2";
    private static final String COMPARE_DIR = "compare";
    private JobConf conf = new JobConf();
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String DATA_PATH = "actual1" + "/result1" + "/part-00000";
    private static final String HDFS_PATH = "/webmap";
    private static final String RESULT_PATH = "/result2";
    private static final String EXPECTED_PATH = "expected/result2";
    private static final String TEST_SOURCE_DIR = COMPARE_DIR + RESULT_PATH + "/comparesource.txt";
    private static final int COUNT_REDUCER = 4;
    private static final int SIZE_KMER = 3;
    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private FileSystem dfs;

    @SuppressWarnings("resource")
    @Test
    public void test() throws Exception {
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHadoop();

        // run graph transformation tests
        CountFilterDriver tldriver = new CountFilterDriver();
        tldriver.run(HDFS_PATH, RESULT_PATH, COUNT_REDUCER, 1, HADOOP_CONF_PATH);

        SequenceFile.Reader reader = null;
        Path path = new Path(RESULT_PATH + "/part-00000");
        reader = new SequenceFile.Reader(dfs, path, conf);
        KmerBytesWritable key = (KmerBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        ByteWritable value = (ByteWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
        File filePathTo = new File(TEST_SOURCE_DIR);
        BufferedWriter bw = new BufferedWriter(new FileWriter(filePathTo));
        while (reader.next(key, value)) {
            bw.write(key.toString() + "\t" + value.toString());
            bw.newLine();
        }
        bw.close();

        dumpResult();
        TestUtils.compareWithResult(new File(TEST_SOURCE_DIR), new File(EXPECTED_PATH));

        cleanupHadoop();

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

    private void dumpResult() throws IOException {
        Path src = new Path(RESULT_PATH);
        Path dest = new Path(ACTUAL_RESULT_DIR + "/");
        dfs.copyToLocalFile(src, dest);
    }
}
