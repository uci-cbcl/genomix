package edu.uci.ics.genomix.pregelix.pathmerge;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;

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

import edu.uci.ics.genomix.driver.Driver;
import edu.uci.ics.genomix.driver.Driver.Plan;
import edu.uci.ics.genomix.job.GenomixJob;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;
import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

public class GraphBuildTestSuite extends TestSuite {
	private static final String ACTUAL_RESULT_DIR = "graphbuildresult";
	private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

	private static final String DATA_PATH = "graph/7/TreePath";
	private static final String HDFS_INPUT_PATH = "/test";
	private static final String HDFS_OUTPUT_PATH = "/result";

	private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR
			+ File.separator + "conf.xml";
	private MiniDFSCluster dfsCluster;

	private static JobConf conf = new JobConf();
	private int numberOfNC = 2;
	private int numPartitionPerMachine = 1;

	private static Driver driver;

	public void setUp() throws Exception {
		cleanupStores();
		edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.init();
		FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
		FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
		startHDFS();

		FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
		FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));

		conf.setInt(GenomixJob.KMER_LENGTH, 7);
		driver = new Driver(
				edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.CC_HOST,
				edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT,
				numPartitionPerMachine);
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

		DataOutputStream confOutput = new DataOutputStream(
				new FileOutputStream(new File(HADOOP_CONF_PATH)));
		conf.writeXml(confOutput);
		confOutput.flush();
		confOutput.close();
	}
	
	public static Test suite() throws Exception {
		GraphBuildTestSuite testSuite = new GraphBuildTestSuite();
		testSuite.setUp();
		testSuite.addTest(new GraphBuildTestCase(conf, driver));
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
	
	public void tearDown() throws Exception {
		edu.uci.ics.hyracks.hdfs.utils.HyracksUtils.deinit();
		cleanupHDFS();
	}

	private void cleanupHDFS() throws Exception {
		dfsCluster.shutdown();
	}
}
