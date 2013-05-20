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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.hadoop.pmcommon.MergePathMultiSeqOutputFormat;
import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.SNodeInitialMapper;
import edu.uci.ics.genomix.hadoop.pmcommon.SNodeInitialReducer;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
@SuppressWarnings("deprecation")
public class MergePathH3Driver {

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

    public void run(String inputPath, String outputPath, String mergeResultPath, int numReducers, int sizeKmer,
            int mergeRound, String defaultConfPath) throws IOException {
        JobConf conf;
        FileSystem dfs = FileSystem.get(conf);
        String prevOutput = inputPath;
        
        for (int iMerge = 1; iMerge <= mergeRound; iMerge++) {
            conf = makeJobConf(sizeKmer, iMerge, numReducers, defaultConfPath);

            dfs.delete(new Path(outputPath), true);
            JobClient.runJob(conf);
            dfs.delete(new Path(inputPath + "stepNext"), true);
            dfs.rename(new Path(outputPath + "/" + uncompSinglePath), new Path(inputPath + "stepNext"));
            dfs.rename(new Path(outputPath + "/" + comSinglePath), new Path(mergeResultPath + "/" + comSinglePath));
            dfs.rename(new Path(outputPath + "/" + comCircle), new Path(mergeResultPath + "/" + comCircle));
        }
    }
    
    private JobConf makeJobConf(int sizeKmer, int iMerge, int numReducers, String defaultConfPath) {
        JobConf conf = new JobConf(MergePathH3Driver.class);
        conf.setInt("sizeKmer", sizeKmer);
        conf.setInt("iMerge", iMerge);
        conf.setJobName("Path Merge-" + iMerge);
        
        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }
        
        conf.setMapperClass(MergePathH3Mapper.class);
        conf.setReducerClass(MergePathH3Reducer.class);
        conf.setMapOutputKeyClass(VKmerBytesWritable.class);
        conf.setMapOutputValueClass(MergePathValueWritable.class);
        conf.setInputFormat(SequenceFileInputFormat.class);

        MultipleOutputs.addNamedOutput(conf, "unmergedSinglePath" + iMerge, MergePathMultiSeqOutputFormat.class,
                VKmerBytesWritable.class, MergePathValueWritable.class);

        MultipleOutputs.addNamedOutput(conf, "mergedSinglePath" + iMerge, MergePathMultiSeqOutputFormat.class,
                VKmerBytesWritable.class, MergePathValueWritable.class);

        conf.setOutputKeyClass(VKmerBytesWritable.class);
        conf.setOutputValueClass(MergePathValueWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath + "stepNext"));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(numReducers);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        MergePathH3Driver driver = new MergePathH3Driver();
        driver.run(options.inputPath, options.outputPath, options.mergeResultPath, options.numReducers,
                options.sizeKmer, options.mergeRound, null);
    }
}
