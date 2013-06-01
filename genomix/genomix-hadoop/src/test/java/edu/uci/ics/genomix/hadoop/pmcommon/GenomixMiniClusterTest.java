package edu.uci.ics.genomix.hadoop.pmcommon;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.junit.After;
import org.junit.Before;

import edu.uci.ics.genomix.hyracks.driver.Driver;
import edu.uci.ics.genomix.hyracks.job.GenomixJobConf;
import edu.uci.ics.genomix.hyracks.test.TestUtils;
import edu.uci.ics.hyracks.hdfs.utils.HyracksUtils;

/*
 * A base class providing most of the boilerplate for Genomix-based tests
 */
@SuppressWarnings("deprecation")
public class GenomixMiniClusterTest {
    protected int KMER_LENGTH = 5;
    protected int READ_LENGTH = 8;
    
    // subclass should modify this to include the HDFS directories that should be cleaned up
    protected ArrayList<String> HDFS_PATHS = new ArrayList<String>();

    protected String EXPECTED_ROOT = "src/test/resources/expected/";
    protected String ACTUAL_ROOT = "src/test/resources/actual/";
    
    protected String HADOOP_CONF_ROOT = "src/test/resources/hadoop/conf/";
    
    protected MiniDFSCluster dfsCluster;
    protected JobConf conf = new JobConf();
    protected int numberOfNC = 1;
    protected int numPartitionPerMachine = 1;
    protected Driver driver;

    protected Writable key;
    protected Writable value;
    protected MiniMRCluster mrCluster;


    @Before
    public void setUp() throws Exception {
        cleanupStores();
        HyracksUtils.init();
        FileUtils.forceMkdir(new File(ACTUAL_ROOT));
        FileUtils.cleanDirectory(new File(ACTUAL_ROOT));
        startHDFS();

        conf.setInt(GenomixJobConf.KMER_LENGTH, KMER_LENGTH);
        conf.setInt(GenomixJobConf.READ_LENGTH, READ_LENGTH);
        driver = new Driver(HyracksUtils.CC_HOST,
                HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT, numPartitionPerMachine);
    }
    
    /*
     * Merge and copy a DFS directory to a local destination, converting to text if necessary. 
     * Also locally store the binary-formatted result if available.
     */
    protected void copyResultsToLocal(String hdfsSrcDir, String localDestFile, Configuration conf) throws IOException {
        String fileFormat = conf.get(GenomixJobConf.OUTPUT_FORMAT);
        if (GenomixJobConf.OUTPUT_FORMAT_TEXT.equalsIgnoreCase(fileFormat)) {
            // for text files, just concatenate them together
            FileUtil.copyMerge(FileSystem.get(conf), new Path(hdfsSrcDir),
                    FileSystem.getLocal(new Configuration()), new Path(localDestFile),
                    false, conf, null);
        } else {
            // file is binary
            // merge and store the binary format
//            FileUtil.copyMerge(FileSystem.get(conf), new Path(hdfsSrcDir),
//                    FileSystem.getLocal(new Configuration()), new Path(localDestFile + ".bin"),
//                    false, conf, null);
            // load the Node's and write them out as text locally
            FileSystem lfs = FileSystem.getLocal(new Configuration());
            lfs.mkdirs(new Path(localDestFile).getParent());
            File filePathTo = new File(localDestFile);
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePathTo));
            FileSystem dfs = FileSystem.get(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(dfs, new Path(hdfsSrcDir + "part-0"), conf);
            SequenceFile.Writer writer = new SequenceFile.Writer(lfs, new JobConf(), new Path(localDestFile + ".bin"), reader.getKeyClass(), reader.getValueClass());
            
            for (int i=0; i < java.lang.Integer.MAX_VALUE; i++) {
                Path path = new Path(hdfsSrcDir + "part-" + i);
                if (!dfs.exists(path)) {
                    break;
                }
                if (dfs.getFileStatus(path).getLen() == 0) {
                    continue;
                }
                reader = new SequenceFile.Reader(dfs, path, conf);
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
    
    protected boolean checkResults(String expectedPath, String actualPath, int[] poslistField) throws Exception {
        File dumped = new File(actualPath); 
        if (poslistField != null) {
            TestUtils.compareWithUnSortedPosition(new File(expectedPath), dumped, poslistField);
        } else {
            TestUtils.compareWithSortedResult(new File(expectedPath), dumped);
        }
        return true;
    }

    protected void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    protected void startHDFS() throws IOException {
        conf.addResource(new Path(HADOOP_CONF_ROOT + "core-site.xml"));
        conf.addResource(new Path(HADOOP_CONF_ROOT + "mapred-site.xml"));
        conf.addResource(new Path(HADOOP_CONF_ROOT + "hdfs-site.xml"));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
        //mrCluster = new MiniMRCluster(4, dfsCluster.getFileSystem().getUri().toString(), 2);
        
        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(ACTUAL_ROOT + "conf.xml")));
        conf.writeXml(confOutput);
        confOutput.close();
    }
    
    protected void copyLocalToDFS(String localSrc, String hdfsDest) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(localSrc);
        Path dest = new Path(hdfsDest);
        dfs.mkdirs(dest);
        dfs.copyFromLocalFile(src, dest);
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
        FileSystem dfs = FileSystem.get(conf);
        for (String path : HDFS_PATHS) {
            if (dfs.exists(new Path(path))) {
                dfs.delete(new Path(path), true);
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        HyracksUtils.deinit();
        cleanupHDFS();
    }

    protected void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
        //mrCluster.shutdown();
    }
}
