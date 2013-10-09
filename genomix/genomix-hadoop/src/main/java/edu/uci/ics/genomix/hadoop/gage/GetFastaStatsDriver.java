package edu.uci.ics.genomix.hadoop.gage;

import org.apache.hadoop.mapred.JobConf;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class GetFastaStatsDriver {
    
    private static class Options {
        @Option(name = "-inputpath", usage = "the input path", required = true)
        public String inputPath;

        @Option(name = "-outputpath", usage = "the output path", required = true)
        public String outputPath;

        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;
    }
    
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        GetFastaStatsTest driver = new GetFastaStatsTest();
        driver.run(options.inputPath, options.outputPath, options.numReducers, new JobConf());
    }
    
}
