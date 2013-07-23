package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.IOException;

import org.apache.commons.cli.Options;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


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
        
        @Option(name = "-read-length", usage = "the length of read", required = true)
        public int readLength;
    }
    
    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int readLength) throws IOException{
        
    }
    
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        GenomixDriver driver = new GenomixDriver();
        driver.run(options.inputPath, options.outputPath, options.numReducers, options.sizeKmer, options.readLength);
    }
}
