package edu.uci.ics.genomix.driver.pipelinetests;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.driver.GenomixDriver;


public class EdgeSizePressureTest {

    private String targetPath;
    
    private static class Options {
        @Option(name = "-kmerLength", usage = "the length of each kmer", required = true)
        public int kmerLength;

        @Option(name = "-countOfLines", usage = "the length of this single long read", required = true)
        public int countOfLines;
        
        @Option(name = "-runLocal", usage = "Run a local instance using the Hadoop MiniCluster. NOTE: overrides settings for -ip and -port and those in conf/*.properties", required=false)
        public boolean runLocal = false;
    }
    
    public EdgeSizePressureTest() {
        targetPath = "edgepressurefortest" + File.separator + "edgepressure.fastq";
    }

    void createInputData(int countOfLines) throws IOException {
        String origiRead = "ATGCATGCGCTAGCTAGCTAGACTACGATGCATGCTAGCTAATCGATCGATCGATC";
        String path = targetPath;
        File file = new File(path);
        file.getParentFile().mkdir();
        file.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));;
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 1; i <= countOfLines; i++) {
            strBuilder.setLength(0);
            strBuilder.append(Integer.toString(i));
            strBuilder.append('\t');
            strBuilder.append(origiRead);
            strBuilder.append('\n');
            writer.write(strBuilder.toString());
        }
        writer.close();
    }

    public void cleanDiskFile() throws IOException, InterruptedException {
        File targetFile = new File(targetPath);
        if(targetFile.getParentFile().exists()){
        String cleanFile = "rm -r -f " + targetFile.getParent();
        Process p = Runtime.getRuntime().exec(cleanFile);
        p.waitFor();
        if (p.exitValue() != 0)
            throw new RuntimeException("Failed to delete the path" + targetFile.getParent());
        }
    }

    public String getTestDir() {
        return targetPath;
    }
    
    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        EdgeSizePressureTest test = new EdgeSizePressureTest();
        
        test.cleanDiskFile();
        test.createInputData(options.countOfLines);
        System.out.println("create complete!");
        String[] esPressureArgs = {"-runLocal", "true", "-kmerLength", String.valueOf(options.kmerLength), "-saveIntermediateResults",
                "true", "-localInput", test.getTestDir(), "-pipelineOrder", "BUILD_HYRACKS,MERGE" };
        GenomixDriver.main(esPressureArgs);
//        test.cleanDiskFile();
    }
}
