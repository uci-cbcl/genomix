package edu.uci.ics.genomix.example.jobrun;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.genomix.driver.Driver;
import edu.uci.ics.genomix.driver.Driver.Plan;
import edu.uci.ics.genomix.job.GenomixJob;
import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningMergingConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.lib.RawBinaryComparatorFactory;
import edu.uci.ics.hyracks.hdfs.lib.RawBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.hdfs.lib.TextKeyValueParserFactory;
import edu.uci.ics.hyracks.hdfs.lib.TextTupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;
import edu.uci.ics.hyracks.hdfs.utils.HyracksUtils;
import edu.uci.ics.hyracks.hdfs.utils.TestUtils;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;

public class JobRunTestCase {
	private static final String ACTUAL_RESULT_DIR = "actual";
	private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

	private static final String DATA_PATH = "src/test/resources/data/webmap/text.txt";
	private static final String HDFS_INPUT_PATH = "/webmap";
	private static final String HDFS_OUTPUT_PATH = "/webmap_result/";

    private static final String DUMPED_RESULT = ACTUAL_RESULT_DIR + HDFS_OUTPUT_PATH + "/merged.txt";
    private static final String EXPECTED_PATH = "src/test/resources/expected/result2";

	private static final String HYRACKS_APP_NAME = "genomix";
	private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR
			+ File.separator + "conf.xml";
	private MiniDFSCluster dfsCluster;

	private JobConf conf = new JobConf();
	private int numberOfNC = 2;
	private int numPartitionPerMachine = 2;

	private Driver driver;

	@Before
	public void setUp() throws Exception {
		cleanupStores();
		HyracksUtils.init();
		HyracksUtils.createApp(HYRACKS_APP_NAME);
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
		dfs.mkdirs(result);
		dfs.copyFromLocalFile(src, dest);

		DataOutputStream confOutput = new DataOutputStream(
				new FileOutputStream(new File(HADOOP_CONF_PATH)));
		conf.writeXml(confOutput);
		confOutput.flush();
		confOutput.close();
	}
	
	private void cleanUpReEntry() throws IOException{
		FileSystem lfs = FileSystem.getLocal(new Configuration());
		if (lfs.exists(new Path(DUMPED_RESULT))){
			lfs.delete(new Path(DUMPED_RESULT), true);
		}
		FileSystem dfs = FileSystem.get(conf);
		if (dfs.exists(new Path(HDFS_OUTPUT_PATH))){
			dfs.delete(new Path(HDFS_OUTPUT_PATH), true);
		}
	}

	@Test
	public void TestExternalGroupby() throws Exception {
		cleanUpReEntry();
		conf.set(GenomixJob.GROUPBY_TYPE, "external");
		conf.set(GenomixJob.OUTPUT_FORMAT, "text");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults());
	}

	//@Test
	public void TestPreClusterGroupby() throws Exception {
		cleanUpReEntry();
		conf.set(GenomixJob.GROUPBY_TYPE, "precluster");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults());
	}

	//@Test
	public void TestHybridGroupby() throws Exception {
		cleanUpReEntry();
		conf.set(GenomixJob.GROUPBY_TYPE, "hybrid");
		conf.set(GenomixJob.OUTPUT_FORMAT, "text");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults());
	}

	private boolean checkResults() throws Exception {
		FileUtil.copyMerge(FileSystem.get(conf), new Path(HDFS_OUTPUT_PATH), FileSystem.getLocal(new Configuration()), new Path(DUMPED_RESULT), false, conf, null);
		TestUtils.compareWithResult(new File(EXPECTED_PATH
				), new File(DUMPED_RESULT));
		return true;
	}

	@After
	public void tearDown() throws Exception {
		HyracksUtils.destroyApp(HYRACKS_APP_NAME);
		HyracksUtils.deinit();
		cleanupHDFS();
	}

	private void cleanupHDFS() throws Exception {
		dfsCluster.shutdown();
	}

}
