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
package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

@SuppressWarnings("deprecation")
public class MergePathsH3Driver {

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

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int mergeRound,
            String defaultConfPath, JobConf defaultConf) throws IOException {
        JobConf baseConf = defaultConf == null ? new JobConf() : defaultConf;
        if (defaultConfPath != null) {
            baseConf.addResource(new Path(defaultConfPath));
        }
        baseConf.setNumReduceTasks(numReducers);
        baseConf.setInt("sizeKmer", sizeKmer);

        FileSystem dfs = FileSystem.get(baseConf);
        String prevOutput = inputPath;
        dfs.delete(new Path(outputPath), true); // clear any previous output

        String tmpOutputPath = "NO_JOBS_DONE";
        boolean finalMerge = false;
        for (int iMerge = 1; iMerge <= mergeRound; iMerge++) {
            baseConf.setInt("iMerge", iMerge);
            baseConf.setBoolean("finalMerge", finalMerge);
            MergePathsH3 merger = new MergePathsH3();
            tmpOutputPath = inputPath + ".mergepathsH3." + String.valueOf(iMerge);
            RunningJob job = merger.run(prevOutput, tmpOutputPath, baseConf);
            if (job.getCounters().findCounter("genomix", "num_merged").getValue() == 0) {
                if (!finalMerge) {
                    // all of the pseudoheads have found each other.  H3 now behaves like H1
                    finalMerge = true;
                } else {
                    // already in final merge stage and all paths were merged before.  We're done!
                    break;
                }
            }
        }
        dfs.rename(new Path(tmpOutputPath), new Path(outputPath)); // save final results
    }

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int mergeRound,
            String defaultConfPath) throws IOException {
        run(inputPath, outputPath, numReducers, sizeKmer, mergeRound, defaultConfPath, null);
    }

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int mergeRound,
            JobConf defaultConf) throws IOException {
        run(inputPath, outputPath, numReducers, sizeKmer, mergeRound, null, defaultConf);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        MergePathsH3Driver driver = new MergePathsH3Driver();
        driver.run(options.inputPath, options.outputPath, options.numReducers, options.sizeKmer, options.mergeRound,
                null, null);
    }
}
