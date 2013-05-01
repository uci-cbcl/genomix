/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.pregelix.pathmerge;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

import edu.uci.ics.genomix.pregelix.sequencefile.GenerateTextFile;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.core.base.IDriver.Plan;
import edu.uci.ics.pregelix.core.driver.Driver;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;

public class PathMergeSmallTestCase extends TestCase {
	private final PregelixJob job;
	private final String resultFileDir;
	private final String textFileDir;
	private final String jobFile;
	private final Driver driver = new Driver(this.getClass());
	private final FileSystem dfs;

	public PathMergeSmallTestCase(String hadoopConfPath, String jobName,
			String jobFile, FileSystem dfs, String hdfsInput, String resultFile, String textFile)
			throws Exception {
		super("test");
		this.jobFile = jobFile;
		this.job = new PregelixJob("test");
		this.job.getConfiguration().addResource(new Path(jobFile));
		this.job.getConfiguration().addResource(new Path(hadoopConfPath));
		FileInputFormat.setInputPaths(job, hdfsInput);
		FileOutputFormat.setOutputPath(job, new Path(hdfsInput + "_result"));
		this.textFileDir = textFile;
		job.setJobName(jobName);
		this.resultFileDir = resultFile;
		
		this.dfs = dfs;
	}

	private void waitawhile() throws InterruptedException {
		synchronized (this) {
			this.wait(20);
		}
	}

	@Test
	public void test() throws Exception {
		setUp();
		Plan[] plans = new Plan[] { Plan.OUTER_JOIN };
		for (Plan plan : plans) {
			driver.runJob(job, plan, PregelixHyracksIntegrationUtil.CC_HOST,
					PregelixHyracksIntegrationUtil.TEST_HYRACKS_CC_CLIENT_PORT,
					false);
		}
		compareResults();
		tearDown();
		waitawhile();
	}

	private void compareResults() throws Exception {
		dfs.copyToLocalFile(FileOutputFormat.getOutputPath(job), new Path(
				resultFileDir));
		GenerateTextFile.generateFromPathmergeResult(5, resultFileDir, textFileDir);
		// TestUtils.compareWithResultDir(new File(expectedFileDir), new
		// File(resultFileDir));
	}

	public String toString() {
		return jobFile;
	}

}
