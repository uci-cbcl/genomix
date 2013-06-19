package edu.uci.ics.genomix.hadoop.pmcommon;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import edu.uci.ics.genomix.hyracks.test.TestUtils;

/*
 * A base class providing most of the boilerplate for Hadoop-based tests
 */
@SuppressWarnings("deprecation")
public class HadoopMiniClusterTest {
    protected int KMER_LENGTH = 5;
    protected int READ_LENGTH = 8;

    // subclass should modify this to include the HDFS directories that should be cleaned up
    protected ArrayList<String> HDFS_PATHS = new ArrayList<String>();

    protected static String EXPECTED_ROOT = "src/test/resources/expected/";
    protected static String ACTUAL_ROOT = "src/test/resources/actual/";

    protected static String HADOOP_CONF_ROOT = "src/test/resources/hadoop/conf/";
    protected static String HADOOP_CONF = HADOOP_CONF_ROOT + "conf.xml";

    protected static MiniDFSCluster dfsCluster;
    protected static MiniMRCluster mrCluster;
    protected static FileSystem dfs;
    protected static JobConf conf = new JobConf();
    protected static int numberOfNC = 1;
    protected static int numPartitionPerMachine = 1;

    @BeforeClass
    public static void setUpMiniCluster() throws Exception {
        cleanupStores();
        startHDFS();
        FileUtils.forceMkdir(new File(ACTUAL_ROOT));
        FileUtils.cleanDirectory(new File(ACTUAL_ROOT));
    }

    /*
     * Merge and copy a DFS directory to a local destination, converting to text if necessary. 
     * Also locally store the binary-formatted result if available.
     */
    protected static void copyResultsToLocal(String hdfsSrcDir, String localDestFile, boolean resultsAreText,
            Configuration conf) throws IOException {
        if (resultsAreText) {
            // for text files, just concatenate them together
            FileUtil.copyMerge(FileSystem.get(conf), new Path(hdfsSrcDir), FileSystem.getLocal(new Configuration()),
                    new Path(localDestFile), false, conf, null);
        } else {
            // file is binary
            // save the entire binary output dir
            FileUtil.copy(FileSystem.get(conf), new Path(hdfsSrcDir), FileSystem.getLocal(new Configuration()),
                    new Path(localDestFile + ".bindir"), false, conf);
            
            // chomp through output files
            FileStatus[] files = ArrayUtils.addAll(dfs.globStatus(new Path(hdfsSrcDir + "*")), dfs.globStatus(new Path(hdfsSrcDir + "*/*")));
            FileStatus validFile = null;
            for (FileStatus f : files) {
            	if (f.getLen() != 0) {
            		validFile = f;
            		break;
            	}
            }
            if (validFile == null) {
                throw new IOException("No non-zero outputs in source directory " + hdfsSrcDir);
            }

            // also load the Nodes and write them out as text locally. 
            FileSystem lfs = FileSystem.getLocal(new Configuration());
            lfs.mkdirs(new Path(localDestFile).getParent());
            File filePathTo = new File(localDestFile);
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePathTo));
            SequenceFile.Reader reader = new SequenceFile.Reader(dfs, validFile.getPath(), conf);
            SequenceFile.Writer writer = new SequenceFile.Writer(lfs, new JobConf(), new Path(localDestFile
                    + ".binmerge"), reader.getKeyClass(), reader.getValueClass());

            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            for (FileStatus f : files) {
                if (f.getLen() == 0) {
                    continue;
                }
                reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                while (reader.next(key, value)) {
                    if (key == null || value == null) {
                        break;
                    }
                    bw.write(key.toString() + "\t" + value.toString());
                    System.out.println(key.toString() + "\t" + value.toString());
                    bw.newLine();
                    writer.append(key, value);

                }
                reader.close();
            }
            writer.close();
            bw.close();
        }

    }

    protected static boolean checkResults(String expectedPath, String actualPath, int[] poslistField) throws Exception {
        File dumped = new File(actualPath);
        if (poslistField != null) {
            TestUtils.compareWithUnSortedPosition(new File(expectedPath), dumped, poslistField);
        } else {
            TestUtils.compareWithSortedResult(new File(expectedPath), dumped);
        }
        return true;
    }

    protected static void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    protected static void startHDFS() throws IOException {
//        conf.addResource(new Path(HADOOP_CONF_ROOT + "core-site.xml"));
        //        conf.addResource(new Path(HADOOP_CONF_ROOT + "mapred-site.xml"));
//        conf.addResource(new Path(HADOOP_CONF_ROOT + "hdfs-site.xml"));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(4, dfs.getUri().toString(), 2);
        System.out.println(dfs.getUri().toString());

        DataOutputStream confOutput = new DataOutputStream(
                new FileOutputStream(new File(HADOOP_CONF)));
        conf.writeXml(confOutput);
        confOutput.close();
    }

    protected static void copyLocalToDFS(String localSrc, String hdfsDest) throws IOException {
        Path dest = new Path(hdfsDest);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(new Path(localSrc), dest);
    }

    /*
     * Remove the local "actual" folder and any hdfs folders in use by this test
     */
    public void cleanUpOutput() throws IOException {
        // local cleanup
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        if (lfs.exists(new Path(ACTUAL_ROOT))) {
            lfs.delete(new Path(ACTUAL_ROOT), true);
        }
        // dfs cleanup
        for (String path : HDFS_PATHS) {
            if (dfs.exists(new Path(path))) {
                dfs.delete(new Path(path), true);
            }
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        cleanupHDFS();
    }

    protected static void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
        mrCluster.shutdown();
    }
}
