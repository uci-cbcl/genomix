package edu.uci.ics.genomix.driver.comparation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.driver.GenomixDriver;
import edu.uci.ics.genomix.hyracks.graph.test.GraphBuildingTestSetting;
import edu.uci.ics.genomix.hyracks.graph.test.TestUtils;
import edu.uci.ics.genomix.minicluster.GenerateGraphViz;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager;
import edu.uci.ics.genomix.minicluster.GenomixClusterManager.ClusterType;

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
    private static final String ACTUAL_RESULT_HYRACKS_DIR = "actual/sequence/hyracks";
    private static final String ACTUAL_RESULT_HADOOP_DIR = "actual/sequence/hadoop";
    private static final String HDFS_INPUT_PATH = "/webmap";
    private static final String HDFS_OUTPUT_PATH_HYRACKS = "/webmap_result_hyracks";
    private static final String HDFS_OUTPUT_PATH_HADOOP = "/webmap_result_hadoop";

    private static GenomixClusterManager manager;

    private static GenomixDriver driver;

    // Instance member
    private final String testDirName;
    private final File testFile;
    private final String hyracksResultFileName;
    private final String hadoopResultFileName;
    private final String expectFileName;

    private GenomixJobConf confHyracks;
    private GenomixJobConf confHadoop;

    public HyrackVSHadoopTest(String subdir, File file) throws IOException {
        this.testDirName = subdir;
        this.testFile = file;

        this.hyracksResultFileName = ACTUAL_RESULT_HYRACKS_DIR + File.separator + testDirName + File.separator
                + testFile.getName() + File.separator + "data";
        this.hadoopResultFileName = ACTUAL_RESULT_HADOOP_DIR + File.separator + testDirName + File.separator
                + testFile.getName() + File.separator + "data";
        this.expectFileName = testFile.getAbsolutePath().replaceFirst(GraphBuildingTestSetting.TEST_FILE_EXTENSION,
                ".expected.txt");
        System.out.print(expectFileName);
    }

    @BeforeClass
    public static void setUp() throws Exception {

        FileUtils.forceMkdir(new File(ACTUAL_RESULT_HYRACKS_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_HYRACKS_DIR));

        driver = new GenomixDriver();
    }

    private void waitawhile() throws InterruptedException {
        synchronized (this) {
            this.wait(20);
        }
    }

    @Test
    public void TestEachFile() throws Exception {
        waitawhile();
        String cmd = "-kmerLength 55 -localInput " + this.testFile.getAbsolutePath();

        String[] argsHyracks = (cmd + " -pipelineOrder BUILD_HYRACKS").split("\\s+");
        confHyracks = GenomixJobConf.fromArguments(argsHyracks);

        String[] argsHadoop = (cmd + " -pipelineOrder BUILD_HADOOP").split("\\s+");
        confHadoop = GenomixJobConf.fromArguments(argsHadoop);

        prepareData();
        driver.runGenomix(confHyracks);
        driver.runGenomix(confHadoop);
        checkResult();
    }

    private void prepareData() throws IOException {
        FileSystem dfs = FileSystem.get(confHadoop);
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH_HYRACKS))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH_HYRACKS), true);
        }
        if (dfs.exists(new Path(HDFS_OUTPUT_PATH_HADOOP))) {
            dfs.delete(new Path(HDFS_OUTPUT_PATH_HADOOP), true);
        }
        if (dfs.exists(new Path(HDFS_INPUT_PATH))) {
            dfs.delete(new Path(HDFS_INPUT_PATH), true);
        }
        GenomixClusterManager.copyLocalToHDFS(confHadoop, testFile.getAbsolutePath(), HDFS_INPUT_PATH + File.separator
                + testFile.getName());
    }

    public void checkResult() throws Exception {
        Path path = new Path(hyracksResultFileName);
        GenomixClusterManager.copyBinToLocal(confHadoop,
                HDFS_OUTPUT_PATH_HYRACKS + File.separator + testFile.getName(), path.getParent().toString());
        GenerateGraphViz.convertGraphBuildingOutputToGraphViz(path.getParent().toString() + "/bin", path.getParent()
                .toString() + "/graphviz");

        path = new Path(hadoopResultFileName);
        GenomixClusterManager.copyBinToLocal(confHadoop, HDFS_OUTPUT_PATH_HADOOP + File.separator + testFile.getName(),
                path.getParent().toString());
        GenerateGraphViz.convertGraphBuildingOutputToGraphViz(path.getParent().toString() + "/bin", path.getParent()
                .toString() + "/graphviz");

        TestUtils.compareFilesBySortingThemLineByLine(new File(hyracksResultFileName), new File(hadoopResultFileName));
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (manager != null) {
            manager.stopCluster(ClusterType.HADOOP);
            manager.stopCluster(ClusterType.HYRACKS);
        }
    }
}
