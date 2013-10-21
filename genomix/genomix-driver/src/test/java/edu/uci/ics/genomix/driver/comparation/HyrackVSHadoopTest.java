package edu.uci.ics.genomix.driver.comparation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import junit.framework.Assert;

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
    private static GenomixDriver driver;

    // Instance member
    private final String testDirName;
    private final File testFile;
    private final String hyracksResultFileName;
    private final String hadoopResultFileName;
    private final String expectFileName;
    private final int kmerLength;
    private final String ACTUAL_RESULT_HYRACKS_DIR = "actual/sequence/hyracks";
    private final String ACTUAL_RESULT_HADOOP_DIR = "actual/sequence/hadoop";

    private GenomixJobConf confHyracks;
    private GenomixJobConf confHadoop;

    public HyrackVSHadoopTest(String subdir, File file, int kmerLength) throws IOException {
        this.testDirName = subdir;
        this.testFile = file;
        this.kmerLength = kmerLength;

        this.hyracksResultFileName = ACTUAL_RESULT_HYRACKS_DIR + File.separator + testDirName + File.separator + "data";
        this.hadoopResultFileName = ACTUAL_RESULT_HADOOP_DIR + File.separator + testDirName + File.separator + "data";
        this.expectFileName = testFile.getAbsolutePath().replaceFirst(GraphBuildingTestSetting.TEST_FILE_EXTENSION,
                ".expected.txt");
        System.out.print(expectFileName);
    }

    @BeforeClass
    public static void setUp() throws Exception {
        System.setProperty("app.home", System.getProperty("user.dir") + "/target/appassembler/");
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
        String cmd = "-kmerLength " + kmerLength + " -localInput " + this.testFile.getAbsolutePath();

        String[] argsHyracks = (cmd + " -pipelineOrder BUILD_HYRACKS -localOutput " + new Path(hyracksResultFileName)
                .getParent()).split("\\s+");
        confHyracks = GenomixJobConf.fromArguments(argsHyracks);

        String[] argsHadoop = (cmd + " -pipelineOrder BUILD_HADOOP -localOutput " + new Path(hadoopResultFileName)
                .getParent()).split("\\s+");
        confHadoop = GenomixJobConf.fromArguments(argsHadoop);

        driver.runGenomix(confHyracks);
        driver.runGenomix(confHadoop);
        checkResult();
    }

    public void checkResult() throws Exception {
        TestUtils.compareFilesBySortingThemLineByLine(new File(hyracksResultFileName), new File(hadoopResultFileName));
    }

    @AfterClass
    public static void tearDown() throws Exception {
    }
}
