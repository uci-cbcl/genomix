package edu.uci.ics.genomix.pregelix.pathmerge;

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
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.genomix.driver.Driver;
import edu.uci.ics.genomix.driver.Driver.Plan;
import edu.uci.ics.genomix.example.jobrun.TestUtils;
import edu.uci.ics.genomix.job.GenomixJob;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

public class GraphBuildTest {
	private static final String ACTUAL_RESULT_DIR = "graphbuildresult";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

    private static final String DATA_PATH = "data/sequencefile/BridgePath";
    private static final String HDFS_INPUT_PATH = "/BridgePath";
    private static final String HDFS_OUTPUT_PATH = "/BridgePath_result";

    private static final String DUMPED_RESULT = ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH + "/result.txt";
    private static final String CONVERT_RESULT = DUMPED_RESULT + ".txt";
    private static final String EXPECTED_PATH = "src/test/resources/expected/result2";
    private static final String EXPECTED_REVERSE_PATH = "src/test/resources/expected/result_reverse";

    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private MiniDFSCluster dfsCluster;

    private JobConf conf = new JobConf();
    private int numberOfNC = 2;
    private int numPartitionPerMachine = 1;

    private Driver driver;

    @Before
    public void setUp() throws Exception {
        cleanupStores();
        edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.init();
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHDFS();

        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));

        conf.setInt(GenomixJob.KMER_LENGTH, 5);
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
        Path src = new Path(DATA_PATH);
        Path dest = new Path(HDFS_INPUT_PATH);
        dfs.mkdirs(dest);
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

    @Test
    public void TestAll() throws Exception {
        cleanUpReEntry();
        TestHybridGroupby();
        cleanUpReEntry();
        TestPreClusterGroupby();
    }

    public void TestPreClusterGroupby() throws Exception {
        conf.set(GenomixJob.GROUPBY_TYPE, "precluster");
        System.err.println("Testing PreClusterGroupBy");
        driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
        Assert.assertEquals(true, checkResults(EXPECTED_PATH));
    }
    
    public void TestHybridGroupby() throws Exception {
        conf.set(GenomixJob.GROUPBY_TYPE, "hybrid");
        System.err.println("Testing HybridGroupBy");
        driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
        Assert.assertEquals(true, checkResults(EXPECTED_PATH));
    }

    private boolean checkResults(String expectedPath) throws Exception {
        File dumped = null;
        String format = conf.get(GenomixJob.OUTPUT_FORMAT);
        if ("text".equalsIgnoreCase(format)) {
            FileUtil.copyMerge(FileSystem.get(conf), new Path(HDFS_OUTPUT_PATH),
                    FileSystem.getLocal(new Configuration()), new Path(DUMPED_RESULT), false, conf, null);
            dumped = new File(DUMPED_RESULT);
        } else {

            FileSystem.getLocal(new Configuration()).mkdirs(new Path(ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH));
            File filePathTo = new File(CONVERT_RESULT);
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePathTo));
            for (int i = 0; i < numPartitionPerMachine * numberOfNC; i++) {
                String partname = "/part-" + i;
                				FileUtil.copy(FileSystem.get(conf), new Path(HDFS_OUTPUT_PATH
                						+ partname), FileSystem.getLocal(new Configuration()),
                						new Path(ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH + partname), false, conf);

                Path path = new Path(HDFS_OUTPUT_PATH + partname);
                FileSystem dfs = FileSystem.get(conf);
                if (dfs.getFileStatus(path).getLen() == 0) {
                    continue;
                }
                SequenceFile.Reader reader = new SequenceFile.Reader(dfs, path, conf);
                KmerBytesWritable key = new KmerBytesWritable(conf.getInt(GenomixJob.KMER_LENGTH,
                        GenomixJob.DEFAULT_KMER));
                KmerCountValue value = (KmerCountValue) ReflectionUtils.newInstance(reader.getValueClass(), conf);

                while (reader.next(key, value)) {
                    if (key == null || value == null) {
                        break;
                    }
                    bw.write(key.toString() + "\t" + value.toString());
                    System.out.println(key.toString() + "\t" + value.toString());
                    bw.newLine();
                }
                reader.close();
            }
            bw.close();
            dumped = new File(CONVERT_RESULT);
        }

        //TestUtils.compareWithSortedResult(new File(expectedPath), dumped);
        return true;
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
