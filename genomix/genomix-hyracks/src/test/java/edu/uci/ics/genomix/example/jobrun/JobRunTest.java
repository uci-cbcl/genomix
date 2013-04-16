package edu.uci.ics.genomix.example.jobrun;

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
import org.apache.hadoop.io.BytesWritable;
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
import edu.uci.ics.genomix.job.GenomixJob;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerCountValue;

public class JobRunTest {
	private static final String ACTUAL_RESULT_DIR = "actual";
	private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

	private static final String DATA_PATH = "src/test/resources/data/mergeTest/ThreeKmer";
	private static final String HDFS_INPUT_PATH = "/webmap";
	private static final String HDFS_OUTPUT_PATH = "/webmap_result";

	private static final String DUMPED_RESULT = ACTUAL_RESULT_DIR
			+ HDFS_OUTPUT_PATH + "/merged.txt";
	private static final String CONVERT_RESULT = DUMPED_RESULT + ".txt";
	private static final String EXPECTED_PATH = "src/test/resources/expected/result2";
	private static final String EXPECTED_REVERSE_PATH = "src/test/resources/expected/result_reverse";

	private static final String HYRACKS_APP_NAME = "genomix";
	private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR
			+ File.separator + "conf.xml";
	private MiniDFSCluster dfsCluster;

	private JobConf conf = new JobConf();
	private int numberOfNC = 2;
	private int numPartitionPerMachine = 1;

	private Driver driver;

	@Before
	public void setUp() throws Exception {
		cleanupStores();
		HyracksUtils.init();
		FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
		FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
		startHDFS();

		FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
		FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));

		conf.setInt(GenomixJob.KMER_LENGTH, 5);
		driver = new Driver(HyracksUtils.CC_HOST,
				HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT,
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
		Path result = new Path(HDFS_OUTPUT_PATH);
		dfs.mkdirs(dest);
		// dfs.mkdirs(result);
		dfs.copyFromLocalFile(src, dest);

		DataOutputStream confOutput = new DataOutputStream(
				new FileOutputStream(new File(HADOOP_CONF_PATH)));
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
		TestExternalGroupby();
		cleanUpReEntry();
		TestPreClusterGroupby();
		cleanUpReEntry();
		TestHybridGroupby();
		cleanUpReEntry();
		conf.setBoolean(GenomixJob.REVERSED_KMER, true);
		TestExternalReversedGroupby();
		cleanUpReEntry();
		TestPreClusterReversedGroupby();
		cleanUpReEntry();
		TestHybridReversedGroupby();
	}

	public void TestExternalGroupby() throws Exception {
		conf.set(GenomixJob.GROUPBY_TYPE, "external");
		System.err.println("Testing ExternalGroupBy");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults(EXPECTED_PATH));
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

	public void TestExternalReversedGroupby() throws Exception {
		conf.set(GenomixJob.GROUPBY_TYPE, "external");
		conf.setBoolean(GenomixJob.REVERSED_KMER, true);
		System.err.println("Testing ExternalGroupBy + Reversed");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults(EXPECTED_REVERSE_PATH));
	}

	public void TestPreClusterReversedGroupby() throws Exception {
		conf.set(GenomixJob.GROUPBY_TYPE, "precluster");
		conf.setBoolean(GenomixJob.REVERSED_KMER, true);
		System.err.println("Testing PreclusterGroupBy + Reversed");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults(EXPECTED_REVERSE_PATH));
	}

	public void TestHybridReversedGroupby() throws Exception {
		conf.set(GenomixJob.GROUPBY_TYPE, "hybrid");
		conf.setBoolean(GenomixJob.REVERSED_KMER, true);
		System.err.println("Testing HybridGroupBy + Reversed");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults(EXPECTED_REVERSE_PATH));
	}

	private boolean checkResults(String expectedPath) throws Exception {
		File dumped = null;
		String format = conf.get(GenomixJob.OUTPUT_FORMAT);
		if ("text".equalsIgnoreCase(format)) {
			FileUtil.copyMerge(FileSystem.get(conf),
					new Path(HDFS_OUTPUT_PATH), FileSystem
							.getLocal(new Configuration()), new Path(
							DUMPED_RESULT), false, conf, null);
			dumped = new File(DUMPED_RESULT);
		} else {

			FileSystem.getLocal(new Configuration()).mkdirs(
					new Path(ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH));
			File filePathTo = new File(CONVERT_RESULT);
			BufferedWriter bw = new BufferedWriter(new FileWriter(filePathTo));
			for (int i = 0; i < numPartitionPerMachine * numberOfNC; i++) {
				String partname = "/part-" + i;
				FileUtil.copy(FileSystem.get(conf), new Path(HDFS_OUTPUT_PATH
						+ partname), FileSystem.getLocal(new Configuration()),
						new Path(ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH
								+ partname), false, conf);

				Path path = new Path(HDFS_OUTPUT_PATH + partname);
				FileSystem dfs = FileSystem.get(conf);
				if (dfs.getFileStatus(path).getLen() == 0) {
					continue;
				}
				SequenceFile.Reader reader = new SequenceFile.Reader(dfs, path,
						conf);
				BytesWritable key = (BytesWritable) ReflectionUtils
						.newInstance(reader.getKeyClass(), conf);
				KmerCountValue value = (KmerCountValue) ReflectionUtils
						.newInstance(reader.getValueClass(), conf);

				int k = conf.getInt(GenomixJob.KMER_LENGTH, 25);
				while (reader.next(key, value)) {
					if (key == null || value == null) {
						break;
					}
					bw.write(Kmer.recoverKmerFrom(k, key.getBytes(), 0,
							key.getLength())
							+ "\t" + value.toString());
					System.out.println(Kmer.recoverKmerFrom(k, key.getBytes(),
							0, key.getLength()) + "\t" + value.toString());
					bw.newLine();
				}
				reader.close();

			}
			bw.close();
			dumped = new File(CONVERT_RESULT);
		}

		TestUtils.compareWithResult(new File(expectedPath), dumped);
		return true;
	}

	@After
	public void tearDown() throws Exception {
		HyracksUtils.deinit();
		cleanupHDFS();
	}

	private void cleanupHDFS() throws Exception {
		dfsCluster.shutdown();
	}

}
