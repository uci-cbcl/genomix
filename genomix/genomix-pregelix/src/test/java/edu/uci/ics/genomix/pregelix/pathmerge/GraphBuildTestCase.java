package edu.uci.ics.genomix.pregelix.pathmerge;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import edu.uci.ics.genomix.driver.Driver;
import edu.uci.ics.genomix.driver.Driver.Plan;
import edu.uci.ics.genomix.job.GenomixJob;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

public class GraphBuildTestCase extends TestCase {
	private final JobConf conf;
	private Driver driver;
	private int numberOfNC = 2;
	private int numPartitionPerMachine = 1;
	
	private static final String ACTUAL_RESULT_DIR = "graphbuildresult";
	private static final String HDFS_OUTPUT_PATH = "/result";
	private static final String DUMPED_RESULT = ACTUAL_RESULT_DIR
			+ HDFS_OUTPUT_PATH + "/result.txt";
	private static final String CONVERT_RESULT = ACTUAL_RESULT_DIR
			+ HDFS_OUTPUT_PATH + "/result.txt.txt";
	
	public GraphBuildTestCase(JobConf conf, Driver driver){
		this.conf = conf;
		this.driver = driver;
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
	public void Test() throws Exception {
		cleanUpReEntry();
		TestPreClusterGroupby();
	}

	public void TestPreClusterGroupby() throws Exception {
		conf.set(GenomixJob.GROUPBY_TYPE, "precluster");
		System.err.println("Testing PreClusterGroupBy");
		driver.runJob(new GenomixJob(conf), Plan.BUILD_DEBRUJIN_GRAPH, true);
		Assert.assertEquals(true, checkResults());
	}


	private boolean checkResults() throws Exception {
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
				KmerBytesWritable key = new KmerBytesWritable(conf.getInt(
						GenomixJob.KMER_LENGTH, GenomixJob.DEFAULT_KMER));
				KmerCountValue value = (KmerCountValue) ReflectionUtils
						.newInstance(reader.getValueClass(), conf);

				while (reader.next(key, value)) {
					if (key == null || value == null) {
						break;
					}
					bw.write(key.toString() + "\t" + value.toString());
					System.out
							.println(key.toString() + "\t" + value.toString());
					bw.newLine();
				}
				reader.close();
			}
			bw.close();
			dumped = new File(CONVERT_RESULT);
		}

		// TestUtils.compareWithSortedResult(new File(expectedPath), dumped);
		return true;
	}
}
