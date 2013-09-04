package edu.uci.ics.genomix.driver.realtests;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import edu.uci.ics.genomix.driver.GenomixDriver;

public class FrameSizePressureTest {

    private static class Options {
        @Option(name = "-kmerLength", usage = "the length of each kmer", required = true)
        public int kmerLength;

        @Option(name = "-readLength", usage = "the length of this single long read", required = true)
        public int readLength;

        @Option(name = "-coresPerMachine", usage = "how many partitions per machine for hyracks job", required = false)
        public int coresPerMachine;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        SingleLongReadCreateTool test = new SingleLongReadCreateTool(options.kmerLength, options.readLength);
        test.cleanDiskFile();
        test.generateString();
        test.writeToDisk();
        String[] fsPressureArgs = { "-kmerLength", String.valueOf(options.kmerLength), "-coresPerMachine",
                String.valueOf(options.coresPerMachine), "-saveIntermediateResults", "true", "-localInput",
                test.getTestDir(), "-pipelineOrder", "BUILD_HYRACKS,MERGE" };
        GenomixDriver.main(fsPressureArgs);
    }

}