package edu.uci.ics.genomix.driver.pipelinetests;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import edu.uci.ics.genomix.driver.GenomixDriver;

public class FrameSizePressureTest {

    private static class Options {
        @Option(name = "-kmerLength", usage = "the length of each kmer", required = true)
        public int kmerLength;

        @Option(name = "-readLength", usage = "the length of this single long read", required = true)
        public int readLength;
        
        @Option(name = "-runLocal", usage = "Run a local instance using the Hadoop MiniCluster. NOTE: overrides settings for -ip and -port and those in conf/*.properties", required=false)
        private boolean runLocal = false;
    }

    public static void main(String[] args) throws Exception{
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        GenRandSingleLongRead test = new GenRandSingleLongRead(options.kmerLength, options.readLength);
        test.cleanDiskFile();
        test.generateString();
        test.writeToDisk();
        System.out.println(System.getProperty("java.class.path"));
        System.out.println(System.getenv("HADOOP_HOME"));
        System.out.println(System.getenv("JAVA_HOME"));
        String[] fsPressureArgs = { "-kmerLength", String.valueOf(options.kmerLength), "-saveIntermediateResults",
                "true", "-localInput", test.getTestDir(), "-pipelineOrder", "BUILD_HYRACKS,MERGE" };
        GenomixDriver.main(fsPressureArgs);
        test.cleanDiskFile();
    }
}