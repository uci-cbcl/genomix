package edu.uci.ics.genomix.driver.randommerge;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.driver.GenomixDriver;

public class RandomMergeDriver {

    public static void readSequenceFile(String inputGraph) throws IOException {
        JobConf conf = new JobConf();
        FileSystem dfs = FileSystem.getLocal(conf);

        SequenceFile.Reader reader = null;
        VKmer key = null;
        Node value = null;
        FileStatus[] files = dfs.globStatus(new Path(inputGraph + File.separator + "part*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmer) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Node) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        System.out.println(key.toString());
                        System.out.println(value.toString());
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting stats for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
    }

    public static void runPipeLine(String[] args) throws NumberFormatException, Exception {
        GenRandomNodeForMerging test = new GenRandomNodeForMerging(5, 6);
        test.generateString();
        test.writeToDisk();
        String[] randmergePressureArgs = { "-runLocal", "-kmerLength", String.valueOf(5), "-readLengths",
                String.valueOf(6), "-saveIntermediateResults", "-localInput", test.getTestDir(), "-localOutput",
                "output", "-pipelineOrder", "BUILD_HYRACKS,MERGE" };
        GenomixDriver.main(randmergePressureArgs);
    }

    public static void main(String[] args) throws NumberFormatException, Exception {
        runPipeLine(args);
        readSequenceFile("output/FINAL-02-MERGE/bin");
        System.out.println("complete!");
    }
}
