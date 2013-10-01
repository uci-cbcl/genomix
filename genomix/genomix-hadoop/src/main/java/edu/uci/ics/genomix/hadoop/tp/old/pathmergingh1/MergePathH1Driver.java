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
package edu.uci.ics.genomix.hadoop.tp.old.pathmergingh1;

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

import edu.uci.ics.genomix.hadoop.tp.graphclean.pmcommon.MergePathMultiSeqOutputFormat;
import edu.uci.ics.genomix.hadoop.tp.graphclean.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.pmcommon.SNodeInitialMapper;
import edu.uci.ics.genomix.hadoop.tp.graphclean.pmcommon.SNodeInitialReducer;
import edu.uci.ics.genomix.hadoop.tp.old.type.nonreversed.*;
@SuppressWarnings("deprecation")
public class MergePathH1Driver {

    private static class Options {
        @Option(name = "-inputpath", usage = "the input path", required = true)
        public String inputPath;

        @Option(name = "-outputpath", usage = "the output path", required = true)
        public String outputPath;

        @Option(name = "-mergeresultpath", usage = "the merging results path", required = true)
        public String mergeResultPath;

        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;

        @Option(name = "-kmer-kmerByteSize", usage = "the kmerByteSize of kmer", required = true)
        public int sizeKmer;

        @Option(name = "-merge-rounds", usage = "the while rounds of merging", required = true)
        public int mergeRound;

    }

    public void run(String inputPath, String outputPath, String mergeResultPath, int numReducers, int sizeKmer,
            int mergeRound, String defaultConfPath) throws IOException {

        JobConf conf = new JobConf(MergePathH1Driver.class);
        conf.setInt("sizeKmer", sizeKmer);

        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }
        conf.setJobName("Initial Path-Starting-Points Table");
        conf.setMapperClass(SNodeInitialMapper.class);
        conf.setReducerClass(SNodeInitialReducer.class);

        conf.setMapOutputKeyClass(KmerBytesWritable.class);
        conf.setMapOutputValueClass(MergePathValueWritable.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        String singlePointPath = "comSinglePath0";

        MultipleOutputs.addNamedOutput(conf, singlePointPath, MergePathMultiSeqOutputFormat.class,
                VKmerBytesWritable.class, MergePathValueWritable.class);

        conf.setOutputKeyClass(VKmerBytesWritable.class);
        conf.setOutputValueClass(MergePathValueWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(inputPath + "stepNext"));
        conf.setNumReduceTasks(numReducers);
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(inputPath + "stepNext"), true);
        JobClient.runJob(conf);
        dfs.rename(new Path(inputPath + "stepNext" + "/" + singlePointPath), new Path(mergeResultPath + "/"
                + singlePointPath));
        int iMerge = 0;
        /*----------------------------------------------------------------------*/
        for (iMerge = 1; iMerge <= mergeRound; iMerge++) {
//            if (!dfs.exists(new Path(inputPath + "-step1")))
//                break;
            conf = new JobConf(MergePathH1Driver.class);
            conf.setInt("sizeKmer", sizeKmer);
            conf.setInt("iMerge", iMerge);

            if (defaultConfPath != null) {
                conf.addResource(new Path(defaultConfPath));
            }
            conf.setJobName("Path Merge");

            conf.setMapperClass(MergePathH1Mapper.class);
            conf.setReducerClass(MergePathH1Reducer.class);

            conf.setMapOutputKeyClass(VKmerBytesWritable.class);
            conf.setMapOutputValueClass(MergePathValueWritable.class);

            conf.setInputFormat(SequenceFileInputFormat.class);

            String uncompSinglePath = "uncompSinglePath" + iMerge;
            String comSinglePath = "comSinglePath" + iMerge;
            String comCircle = "comCircle" + iMerge;

            MultipleOutputs.addNamedOutput(conf, uncompSinglePath, MergePathMultiSeqOutputFormat.class,
                    VKmerBytesWritable.class, MergePathValueWritable.class);

            MultipleOutputs.addNamedOutput(conf, comSinglePath, MergePathMultiSeqOutputFormat.class,
                    VKmerBytesWritable.class, MergePathValueWritable.class);

            MultipleOutputs.addNamedOutput(conf, comCircle, MergePathMultiSeqOutputFormat.class,
                    VKmerBytesWritable.class, MergePathValueWritable.class);

            conf.setOutputKeyClass(VKmerBytesWritable.class);
            conf.setOutputValueClass(MergePathValueWritable.class);

            FileInputFormat.setInputPaths(conf, new Path(inputPath + "stepNext"));
            FileOutputFormat.setOutputPath(conf, new Path(outputPath));
            conf.setNumReduceTasks(numReducers);
            dfs.delete(new Path(outputPath), true);
            JobClient.runJob(conf);
            dfs.delete(new Path(inputPath + "stepNext"), true);
            dfs.rename(new Path(outputPath + "/" + uncompSinglePath), new Path(inputPath + "stepNext"));
            dfs.rename(new Path(outputPath + "/" + comSinglePath), new Path(mergeResultPath + "/" + comSinglePath));
            dfs.rename(new Path(outputPath + "/" + comCircle), new Path(mergeResultPath + "/" + comCircle));
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        MergePathH1Driver driver = new MergePathH1Driver();
        driver.run(options.inputPath, options.outputPath, options.mergeResultPath, options.numReducers,
                options.sizeKmer, options.mergeRound, null);
    }
}
