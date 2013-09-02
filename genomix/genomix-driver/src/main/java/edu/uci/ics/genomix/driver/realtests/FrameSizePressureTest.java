package edu.uci.ics.genomix.driver.realtests;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.driver.GenomixDriver;

public class FrameSizePressureTest {

    private static class Options {
        @Option(name = "-test-working-path", usage = "the test case generated path", required = true)
        public String inputPath;

        @Option(name = "-runLocal", usage = "Run a local instance using the Hadoop MiniCluster.", required = true)
        public boolean runLocal = true;
    }

    public static void main(String[] args) throws Exception {
        //        if (System.getProperty("app.home") == null)
        //            System.setProperty("app.home", new File("src/main/resources").getAbsolutePath());
        //        String[] myArgs = { "-kmerLength", "5", "-coresPerMachine", "2",
        //                        "-saveIntermediateResults", "true",
        //                        "-localInput", "../genomix-pregelix/data/input/reads/synthetic/",
        //              "-localInput", "../genomix-pregelix/data/input/reads/pathmerge",
        //                        "-localInput", "/home/wbiesing/code/biggerInput",
        //                        "-hdfsInput", "/home/wbiesing/code/hyracks/genomix/genomix-driver/genomix_out/01-BUILD_HADOOP",
        //                "-localInput", "/home/wbiesing/code/hyracks/genomix/genomix-pregelix/data/input/reads/test",
        //                "-localInput", "output-build/bin",
        //                        "-localOutput", "output-skip",
        //                            "-pipelineOrder", "BUILD,MERGE",
        //                            "-inputDir", "/home/wbiesing/code/hyracks/genomix/genomix-driver/graphbuild.binmerge",
        //                "-localInput", "../genomix-pregelix/data/TestSet/PathMerge/CyclePath/bin/part-00000", 
        //            "-pipelineOrder", "BUILD_HYRACKS,MERGE",
        //          "-clusterWaitTime", "1000"};
        SingleLongReadCreateTool test = new SingleLongReadCreateTool(3, 10);
        test.cleanDiskFile();
        test.generateString();
        test.writeToDisk();
        //        GenomixDriver.main(myArgs);
    }

}