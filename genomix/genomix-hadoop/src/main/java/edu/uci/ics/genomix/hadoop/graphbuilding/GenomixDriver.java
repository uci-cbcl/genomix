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

package edu.uci.ics.genomix.hadoop.graphbuilding;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.hadoop.oldtype.*;
/**
 * This class implement driver which start the mapreduce program for graphbuilding
 */
@SuppressWarnings("deprecation")
public class GenomixDriver {
    private static class Options {
        @Option(name = "-inputpath", usage = "the input path", required = true)
        public String inputPath;

        @Option(name = "-outputpath", usage = "the output path", required = true)
        public String outputPath;

        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;

        @Option(name = "-kmer-size", usage = "the size of kmer", required = true)
        public int sizeKmer;
    }

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, String defaultConfPath)
            throws IOException {

        JobConf conf = new JobConf(GenomixDriver.class);
        conf.setInt("sizeKmer", sizeKmer);

        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }

        conf.setJobName("Genomix Graph Building");
        conf.setMapperClass(GenomixMapper.class);
        conf.setReducerClass(GenomixReducer.class);
        conf.setCombinerClass(GenomixCombiner.class);

        conf.setMapOutputKeyClass(KmerBytesWritable.class);
        conf.setMapOutputValueClass(KmerCountValue.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(KmerBytesWritable.class);
        conf.setOutputValueClass(KmerCountValue.class);
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(numReducers);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(outputPath), true);
        JobClient.runJob(conf);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        GenomixDriver driver = new GenomixDriver();
        driver.run(options.inputPath, options.outputPath, options.numReducers, options.sizeKmer, null);
    }

}
