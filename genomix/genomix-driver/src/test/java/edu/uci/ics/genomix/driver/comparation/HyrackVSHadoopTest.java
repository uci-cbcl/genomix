package edu.uci.ics.genomix.driver.comparation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hadoop.contrailgraphbuilding.GenomixHadoopDriver;
import edu.uci.ics.genomix.hyracks.graph.driver.GenomixHyracksDriver;
import edu.uci.ics.genomix.hyracks.graph.test.GraphBuildingTestSetting;
import edu.uci.ics.genomix.hyracks.graph.test.TestUtils;
import edu.uci.ics.genomix.minicluster.GenerateGraphViz;
import edu.uci.ics.genomix.minicluster.GenerateGraphViz.GRAPH_TYPE;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager;

@RunWith(value = Parameterized.class)
public class HyrackVSHadoopTest {
    private final static GraphBuildingTestSetting testsetting = new GraphBuildingTestSetting();

    /**
     * This test utility will feed the argument in Collection<Object[]> to Test constructor each time.
     * Then we can work on the different test data set sequentially.
     * 
     * @return
     * @throws Exception
     */
    @Parameters
    public static Collection<Object[]> fillConstructorArguments() throws Exception {
        Collection<Object[]> data = new ArrayList<Object[]>();
        File dir = new File(SEQUENCE_FILE_DIR);
        for (File subdir : dir.listFiles()) {
            if (!subdir.isDirectory()) {
                continue;
            }
            for (File file : subdir.listFiles(testsetting.getFilenameFilter())) {
                String[] args = testsetting.parseArgumentFromFilename(file.getName());
                Assert.assertTrue(args.length == 1);
                data.add(new Object[] { subdir.getName(), file, Integer.parseInt(args[0]) });
            }
        }
        return data;
    }

    private static final String SEQUENCE_FILE_DIR = "src/test/resources/data/sequence";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";
    private static final String ACTUAL_RESULT_DIR_HYRACKS = "actual/sequence/hyracks";
    private static final String ACTUAL_RESULT_DIR_HADOOP = "actual/sequence/hadoop";
    private static final String HDFS_INPUT_PATH = "/webmap";
    private static final String HDFS_OUTPUT_PATH_HYRACKS = "/webmap_result_hyracks";
    private static final String HDFS_OUTPUT_PATH_HADOOP = "/webmap_result_hadoop";

    private static GenomixClusterManager manager;
    private static GenomixJobConf conf = new GenomixJobConf(3);
    private static int numberOfNC = 2;
    private static int numPartitionPerMachine = 2;
    private static GenomixHyracksDriver hyracksDriver;
    private static GenomixHadoopDriver hadoopDriver;

    // Instance member
    private final String testDirName;
    private final File testFile;
    private final String hyracksResultFileName;
    private final String hadoopResultFileName;
    private final int kmerLength;

    public HyrackVSHadoopTest(String subdir, File file, int kmerLength) throws IOException {
        this.testDirName = subdir;
        this.testFile = file;
        this.kmerLength = kmerLength;

        this.hyracksResultFileName = ACTUAL_RESULT_DIR_HYRACKS + File.separator + testDirName + File.separator + "data";
        this.hadoopResultFileName = ACTUAL_RESULT_DIR_HADOOP + File.separator + testDirName + File.separator + "data";
    }

    @BeforeClass
    public static void setUp() throws Exception {
        conf.setBoolean(GenomixJobConf.RUN_LOCAL, true);
        conf.setInt(GenomixJobConf.FRAME_SIZE, 65535);
        conf.setInt(GenomixJobConf.FRAME_LIMIT, 4096);

        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR_HYRACKS));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR_HYRACKS));

        manager = new GenomixClusterManager(true, conf);
        manager.setNumberOfNC(numberOfNC);
        manager.setNumberOfDataNodesInLocalMiniHDFS(numberOfNC);

        manager.startCluster();

        hyracksDriver = new GenomixHyracksDriver(GenomixClusterManager.LOCAL_HOSTNAME,
                GenomixClusterManager.LOCAL_HYRACKS_CLIENT_PORT, numPartitionPerMachine);
        hadoopDriver = new GenomixHadoopDriver();
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    @Test
    public void TestEachFile() throws Exception {
        waitawhile();
        conf.setInt(GenomixJobConf.KMER_LENGTH, kmerLength);

        GenomixJobConf.setGlobalStaticConstants(conf);

        prepareData();

        hadoopDriver.run(HDFS_INPUT_PATH + File.separator + testFile.getName(), HDFS_OUTPUT_PATH_HADOOP
                + File.separator + testFile.getName(), numberOfNC, kmerLength, 4, true, conf);

        FileInputFormat.setInputPaths(conf, HDFS_INPUT_PATH + File.separator + testFile.getName());
        FileOutputFormat.setOutputPath(conf, new Path(HDFS_OUTPUT_PATH_HYRACKS + File.separator + testFile.getName()));

        hyracksDriver.runJob(conf);

        checkResult();
    }

    private void prepareData() throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH_HADOOP))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH_HADOOP), true);
        }
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH_HYRACKS))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH_HYRACKS), true);
        }
        if (dfs.exists(new Path(HDFS_INPUT_PATH))) {
            dfs.delete(new Path(HDFS_INPUT_PATH), true);
        }
        GenomixClusterManager.copyLocalToHDFS(conf, testFile.getAbsolutePath(), HDFS_INPUT_PATH + File.separator
                + testFile.getName());
    }

    public void checkResult() throws Exception {
        Path path = new Path(hyracksResultFileName);
        GenomixClusterManager.copyBinToLocal(conf, HDFS_OUTPUT_PATH_HYRACKS + File.separator + testFile.getName(), path
                .getParent().toString());
        GenerateGraphViz.convertBinToGraphViz(path.getParent().toString() + "/bin", path.getParent()
                .toString() + "/graphviz", GRAPH_TYPE.DIRECTED_GRAPH_WITH_ALLDETAILS);

        path = new Path(hadoopResultFileName);
        GenomixClusterManager.copyBinToLocal(conf, HDFS_OUTPUT_PATH_HADOOP + File.separator + testFile.getName(), path
                .getParent().toString());
        GenerateGraphViz.convertBinToGraphViz(path.getParent().toString() + "/bin", path.getParent()
                .toString() + "/graphviz", GRAPH_TYPE.DIRECTED_GRAPH_WITH_ALLDETAILS);

        TestUtils.compareFilesBySortingThemLineByLine(new File(hyracksResultFileName), new File(hadoopResultFileName));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (manager != null) {
            manager.stopCluster();
        }
    }
}
