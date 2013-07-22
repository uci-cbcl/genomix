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
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tools.ant.util.IdentityMapper;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.hadoop.pmcommon.ConvertGraphFromNodeWithFlagToNodeWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial;

@SuppressWarnings("deprecation")
public class MergePathsH4Driver {

    private static final String TO_MERGE = "toMerge";
    private static final String TO_UPDATE = "toUpdate";
    private static final String COMPLETE = "complete";
    private String mergeOutput;
    private String toUpdateOutput;
    private String completeOutput;

    private void setOutputPaths(String basePath, int mergeIteration) {
        basePath = basePath.replaceAll("/$", ""); // strip trailing slash
        mergeOutput = basePath + "_" + TO_MERGE + "_i" + mergeIteration;
        toUpdateOutput = basePath + "_" + TO_UPDATE + "_i" + mergeIteration;
        completeOutput = basePath + "_" + COMPLETE + "_i" + mergeIteration;
    }

    private static class Options {
        @Option(name = "-inputpath", usage = "the input path", required = true)
        public String inputPath;

        @Option(name = "-mergeresultpath", usage = "the merging results path", required = true)
        public String mergeResultPath;

        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;

        @Option(name = "-kmer-kmerByteSize", usage = "the kmerByteSize of kmer", required = true)
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
    public String run(String inputGraphPath, int numReducers, int sizeKmer, int mergeRound, String defaultConfPath,
            JobConf defaultConf) throws IOException {
        JobConf baseConf = defaultConf == null ? new JobConf() : defaultConf;
        if (defaultConfPath != null) {
            baseConf.addResource(new Path(defaultConfPath));
        }
        baseConf.setNumReduceTasks(numReducers);
        baseConf.setInt("sizeKmer", sizeKmer);
        FileSystem dfs = FileSystem.get(baseConf);

        int iMerge = 0;
        boolean mergeComplete = false;
        String prevToMergeOutput = inputGraphPath;
        ArrayList<String> completeOutputs = new ArrayList<String>();

        // identify head and tail nodes with pathnode initial
        PathNodeInitial inith4 = new PathNodeInitial();
        setOutputPaths(inputGraphPath, iMerge);
        inith4.run(prevToMergeOutput, mergeOutput, toUpdateOutput, completeOutput, baseConf);
        completeOutputs.add(completeOutput);
        //        dfs.copyToLocalFile(new Path(mergeOutput), new Path("initial-toMerge"));
        //        dfs.copyToLocalFile(new Path(completeOutput), new Path("initial-complete"));

        // several iterations of merging
        MergePathsH4 merger = new MergePathsH4();
        for (iMerge = 1; iMerge <= mergeRound; iMerge++) {
            prevToMergeOutput = mergeOutput;
            setOutputPaths(inputGraphPath, iMerge);
            merger.run(prevToMergeOutput, mergeOutput, toUpdateOutput, completeOutput, baseConf);
            completeOutputs.add(completeOutput);
            //            dfs.copyToLocalFile(new Path(mergeOutput), new Path("i" + iMerge +"-toMerge"));
            //            dfs.copyToLocalFile(new Path(completeOutput), new Path("i" + iMerge +"-complete"));

            if (dfs.listStatus(new Path(mergeOutput)) == null || dfs.listStatus(new Path(mergeOutput)).length == 0) {
                // no output from previous run-- we are done!
                mergeComplete = true;
                break;
            }
        }
        if (!mergeComplete) {
            // if the merge didn't finish, we have to do one final iteration to convert back into (NodeWritable, NullWritable) pairs
            prevToMergeOutput = mergeOutput;
            setOutputPaths(inputGraphPath, iMerge);
            ConvertGraphFromNodeWithFlagToNodeWritable converter = new ConvertGraphFromNodeWithFlagToNodeWritable();
            converter.run(prevToMergeOutput, completeOutput, baseConf);
            completeOutputs.add(completeOutput);
        }

        // final output string is a comma-separated list of completeOutputs
        StringBuilder sb = new StringBuilder();
        String delim = "";
        for (String output : completeOutputs) {
            sb.append(delim).append(output);
            delim = ",";
        }
        String finalInputs = sb.toString();
        return finalInputs;
    }

    public String run(String inputPath, int numReducers, int sizeKmer, int mergeRound, String defaultConfPath)
            throws IOException {
        return run(inputPath, numReducers, sizeKmer, mergeRound, defaultConfPath, null);
    }

    public String run(String inputPath, int numReducers, int sizeKmer, int mergeRound, JobConf defaultConf)
            throws IOException {
        return run(inputPath, numReducers, sizeKmer, mergeRound, null, defaultConf);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        MergePathsH4Driver driver = new MergePathsH4Driver();
        String outputs = driver.run(options.inputPath, options.numReducers, options.sizeKmer, options.mergeRound, null, null);
        System.out.println("Job ran.  Find outputs in " + outputs);
    }
}
