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
package edu.uci.ics.genomix.hadoop.gbresultschecking;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

@SuppressWarnings("deprecation")
public class ResultsCheckingDriver {
    private static class Options {
        @Option(name = "-inputpath1", usage = "the input path", required = true)
        public String inputPath1;

        @Option(name = "-inputpath2", usage = "the input path", required = true)
        public String inputPath2;

        @Option(name = "-outputpath", usage = "the output path", required = true)
        public String outputPath;

        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;

        @Option(name = "-kmer-size", usage = "the size of kmer", required = true)
        public int sizeKmer;

    }

    public void run(String inputPath1, String inputPath2, String outputPath, int numReducers, int sizeKmer,
            String defaultConfPath) throws IOException {

        JobConf conf = new JobConf(ResultsCheckingDriver.class);

        conf.setInt("sizeKmer", sizeKmer);

        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }

        conf.setJobName("Results Checking");
        conf.setMapperClass(ResultsCheckingMapper.class);
        conf.setReducerClass(ResultsCheckingReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        Path[] inputList = new Path[2];
        inputList[0] = new Path(inputPath1);
        inputList[1] = new Path(inputPath2);

        FileInputFormat.setInputPaths(conf, inputList);
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
        ResultsCheckingDriver driver = new ResultsCheckingDriver();
        driver.run(options.inputPath1, options.inputPath2, options.outputPath, options.numReducers, options.sizeKmer,
                null);
    }

}
