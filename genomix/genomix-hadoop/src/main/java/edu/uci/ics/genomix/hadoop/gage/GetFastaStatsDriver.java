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
package edu.uci.ics.genomix.hadoop.gage;

import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


@SuppressWarnings("deprecation")
public class GetFastaStatsDriver {
    
    private static class Options {
        @Option(name = "-inputpath", usage = "the input path", required = true)
        public String inputPath;

        @Option(name = "-outputpath", usage = "the output path", required = true)
        public String outputPath;

        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;
    }
    
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        GetFastaStatsJob driver = new GetFastaStatsJob();
        driver.run(options.inputPath, options.outputPath, options.numReducers, new JobConf());
    }
    
}
