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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

import edu.uci.ics.genomix.driver.Driver;
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

public class JobRunTestCase extends TestCase {
	private static final String ACTUAL_RESULT_DIR = "actual";
	private static final String EXPECTED_RESULT_PATH = "src/test/resources/expected";
	private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

	private static final String DATA_PATH = "src/test/resources/data/customer.tbl";
	private static final String HDFS_INPUT_PATH = "/customer/";
	private static final String HDFS_OUTPUT_PATH = "/customer_result/";

	private static final String HYRACKS_APP_NAME = "genomix";
	private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR
			+ File.separator + "conf.xml";
	private MiniDFSCluster dfsCluster;

	private JobConf conf = new JobConf();
	private int numberOfNC = 2;
	private int numPartitionPerMachine = 2;

	private Driver myDriver;

	@Override
	protected void setUp() throws Exception {
		cleanupStores();
		HyracksUtils.init();
		HyracksUtils.createApp(HYRACKS_APP_NAME);
		FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
		FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
		startHDFS();

		myDriver = new Driver(HyracksUtils.CC_HOST,
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

	@Override
	protected void runTest() throws Throwable {
		TestExternalGroupby();
		TestPreClusterGroupby();
		TestHybridGroupby();
	}

	public void runHdfsJob() throws Throwable {

		FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH);
		FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH));
		conf.setInputFormat(TextInputFormat.class);

		Scheduler scheduler = new Scheduler(HyracksUtils.CC_HOST,
				HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT);
		InputSplit[] splits = conf.getInputFormat().getSplits(conf,
				numberOfNC * 4);

		String[] readSchedule = scheduler.getLocationConstraints(splits);
		JobSpecification jobSpec = new JobSpecification();
		RecordDescriptor recordDesc = new RecordDescriptor(
				new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE });

		String[] locations = new String[] { HyracksUtils.NC1_ID,
				HyracksUtils.NC1_ID, HyracksUtils.NC2_ID, HyracksUtils.NC2_ID };
		HDFSReadOperatorDescriptor readOperator = new HDFSReadOperatorDescriptor(
				jobSpec, recordDesc, conf, splits, readSchedule,
				new TextKeyValueParserFactory());
		PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec,
				readOperator, locations);

		ExternalSortOperatorDescriptor sortOperator = new ExternalSortOperatorDescriptor(
				jobSpec,
				10,
				new int[] { 0 },
				new IBinaryComparatorFactory[] { RawBinaryComparatorFactory.INSTANCE },
				recordDesc);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec,
				sortOperator, locations);

		HDFSWriteOperatorDescriptor writeOperator = new HDFSWriteOperatorDescriptor(
				jobSpec, conf, new TextTupleWriterFactory());
		PartitionConstraintHelper.addAbsoluteLocationConstraint(jobSpec,
				writeOperator, HyracksUtils.NC1_ID);

		jobSpec.connect(new OneToOneConnectorDescriptor(jobSpec), readOperator,
				0, sortOperator, 0);
		jobSpec.connect(
				new MToNPartitioningMergingConnectorDescriptor(
						jobSpec,
						new FieldHashPartitionComputerFactory(
								new int[] { 0 },
								new IBinaryHashFunctionFactory[] { RawBinaryHashFunctionFactory.INSTANCE }),
						new int[] { 0 },
						new IBinaryComparatorFactory[] { RawBinaryComparatorFactory.INSTANCE }),
				sortOperator, 0, writeOperator, 0);
		jobSpec.addRoot(writeOperator);

		IHyracksClientConnection client = new HyracksConnection(
				HyracksUtils.CC_HOST, HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT);
		JobId jobId = client.startJob(HYRACKS_APP_NAME, jobSpec);
		client.waitForCompletion(jobId);

		Assert.assertEquals(true, checkResults());
	}

	void TestExternalGroupby() throws Exception {
	}

	void TestPreClusterGroupby() throws Exception {
		// TODO
	}

	void TestHybridGroupby() throws Exception {
		// TODO
	}

	private boolean checkResults() throws Exception {
		FileSystem dfs = FileSystem.get(conf);
		Path result = new Path(HDFS_OUTPUT_PATH);
		Path actual = new Path(ACTUAL_RESULT_DIR);
		dfs.copyToLocalFile(result, actual);

		TestUtils.compareWithResult(new File(EXPECTED_RESULT_PATH
				+ File.separator + "part-0"), new File(ACTUAL_RESULT_DIR
				+ File.separator + "customer_result" + File.separator
				+ "part-0"));
		return true;
	}

	@Override
	protected void tearDown() throws Exception {
		HyracksUtils.destroyApp(HYRACKS_APP_NAME);
		HyracksUtils.deinit();
		cleanupHDFS();
	}

	private void cleanupHDFS() throws Exception {
		dfsCluster.shutdown();
	}

}
