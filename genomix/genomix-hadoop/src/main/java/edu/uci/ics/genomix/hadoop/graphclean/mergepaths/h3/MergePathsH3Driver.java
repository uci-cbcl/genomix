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

        @Option(name = "-merge-rounds", usage = "the while rounds of merging", required = true)
        public int mergeRound;

    }

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int mergeRound)
            throws IOException {
        JobConf baseConf = new JobConf(); // I don't know the semantics here.  do i use a base conf file or something?
        baseConf.setNumReduceTasks(numReducers);
        baseConf.setInt("sizeKmer", sizeKmer);

        FileSystem dfs = FileSystem.get(baseConf);
        String prevOutput = inputPath;
        dfs.delete(new Path(outputPath), true); // clear any previous output

        String tmpOutputPath = "NO_JOBS_DONE";
        for (int iMerge = 1; iMerge <= mergeRound; iMerge++) {
            MergePathsH3 merger = new MergePathsH3();
            tmpOutputPath = inputPath + ".mergepathsH3." + String.valueOf(iMerge);
            merger.run(prevOutput, tmpOutputPath, baseConf);
        }
        dfs.rename(new Path(tmpOutputPath), new Path(outputPath)); // save final results
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        MergePathsH3Driver driver = new MergePathsH3Driver();
        driver.run(options.inputPath, options.outputPath, options.numReducers, 
                options.sizeKmer, options.mergeRound);
    }
}
