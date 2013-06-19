/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h4;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial;

@SuppressWarnings("deprecation")
public class MergePathsH4Driver {

	private static final String TO_MERGE = "toMerge";
	private static final String COMPLETE = "complete";
	private static final String UPDATES = "updates";
	private String mergeOutput;
	private String completeOutput;
	private String updatesOutput;

	private void setOutputPaths(String basePath, int mergeIteration) {
		basePath = basePath.replaceAll("/$", ""); // strip trailing slash
		mergeOutput = basePath + "_" + TO_MERGE + "_i" + mergeIteration;
		completeOutput = basePath + "_" + COMPLETE + "_i" + mergeIteration;
		updatesOutput = basePath + "_" + UPDATES + "_i" + mergeIteration;
	}

	private static class Options {
		@Option(name = "-inputpath", usage = "the input path", required = true)
		public String inputPath;

		@Option(name = "-outputpath", usage = "the output path", required = true)
		public String outputPath;

		@Option(name = "-mergeresultpath", usage = "the merging results path", required = true)
		public String mergeResultPath;

		@Option(name = "-num-reducers", usage = "the number of reducers", required = true)
		public int numReducers;

		@Option(name = "-kmer-size", usage = "the size of kmer", required = true)
		public int sizeKmer;

		@Option(name = "-merge-rounds", usage = "the maximum number of rounds to merge", required = false)
		public int mergeRound;

		@Option(name = "-hadoop-conf", usage = "an (optional) hadoop configuration xml", required = false)
		public String hadoopConf;

	}

	/*
	 * Main driver for path merging. Given a graph, this driver runs
	 * PathNodeInitial to ID heads and tails, then does up to @mergeRound
	 * iterations of path merging. Updates during the merge are batch-processed
	 * at the end in a final update job.
	 */
	public void run(String inputGraphPath, String outputGraphPath,
			int numReducers, int sizeKmer, int mergeRound,
			String defaultConfPath, JobConf defaultConf) throws IOException {
		JobConf baseConf = defaultConf == null ? new JobConf() : defaultConf;
		if (defaultConfPath != null) {
			baseConf.addResource(new Path(defaultConfPath));
		}
		baseConf.setNumReduceTasks(numReducers);
		baseConf.setInt("sizeKmer", sizeKmer);
		FileSystem dfs = FileSystem.get(baseConf);

		int iMerge = 0;

		// identify head and tail nodes with pathnode initial
		PathNodeInitial inith4 = new PathNodeInitial();
		setOutputPaths(inputGraphPath, iMerge);
		String prevToMergeOutput = inputGraphPath;
		inith4.run(prevToMergeOutput, mergeOutput, completeOutput, baseConf);

		// several iterations of merging
		MergePathsH4 merger = new MergePathsH4();
		for (iMerge = 1; iMerge <= mergeRound; iMerge++) {
			prevToMergeOutput = mergeOutput;
			setOutputPaths(inputGraphPath, iMerge);
			merger.run(prevToMergeOutput, mergeOutput, completeOutput,
					updatesOutput, baseConf);
			if (dfs.listStatus(new Path(mergeOutput)).length == 0) {
				// no output from previous run-- we are done!
				break;
			}
		}

		// finally, combine all the completed paths and update messages to
		// create a single merged graph output
		dfs.delete(new Path(outputGraphPath), true); // clear any previous
														// output
		// use all the "complete" and "update" outputs in addition to the final
		// (possibly empty) toMerge directories
		// as input to the final update step. This builds a comma-delim'ed
		// String of said files.
		final String lastMergeOutput = mergeOutput;
		PathFilter updateFilter = new PathFilter() {
			@Override
			public boolean accept(Path arg0) {
				String path = arg0.toString();
				return path.contains(COMPLETE + "_i")
						|| path.contains(UPDATES + "_i")
						|| path.equals(lastMergeOutput);
			}
		};
		StringBuilder sb = new StringBuilder();
		String delim = "";
		for (FileStatus file : dfs.globStatus(new Path("*"), updateFilter)) {
			sb.append(delim).append(file);
			delim = ",";
		}
		String finalInputs = sb.toString();
		// TODO run the update iteration
	}

	public void run(String inputPath, String outputGraphPath, int numReducers,
			int sizeKmer, int mergeRound, String defaultConfPath)
			throws IOException {
		run(inputPath, outputGraphPath, numReducers, sizeKmer, mergeRound,
				defaultConfPath, null);
	}

	public void run(String inputPath, String outputGraphPath, int numReducers,
			int sizeKmer, int mergeRound, JobConf defaultConf)
			throws IOException {
		run(inputPath, outputGraphPath, numReducers, sizeKmer, mergeRound,
				null, defaultConf);
	}

	public static void main(String[] args) throws Exception {
		Options options = new Options();
		CmdLineParser parser = new CmdLineParser(options);
		parser.parseArgument(args);
		MergePathsH4Driver driver = new MergePathsH4Driver();
		driver.run(options.inputPath, options.outputPath, options.numReducers,
				options.sizeKmer, options.mergeRound, null, null);
	}
}
