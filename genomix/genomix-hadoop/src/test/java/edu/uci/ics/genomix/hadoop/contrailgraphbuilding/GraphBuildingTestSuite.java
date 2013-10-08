package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;

@SuppressWarnings("deprecation")
public class GraphBuildingTestSuite extends TestSuite {

    private static int SIZE_KMER = 3;
    private static int LINES_PERMAP = 4 * 100000;
    public static final String PreFix = "data/webmap/BubbleMerge_TestSet";
    public static final String[] TestDir = { PreFix + File.separator
    	// PathMerge
//                + "2", PreFix + File.separator
//                + "3", PreFix + File.separator
//                + "4", PreFix + File.separator
//                + "5", PreFix + File.separator
//                + "6", PreFix + File.separator
//                + "7", PreFix + File.separator
//                + "8", PreFix + File.separator
//                + "9", PreFix + File.separator
//                + "Head_3", PreFix + File.separator
//                + "Head_4", PreFix + File.separator
//                + "Head_5", PreFix + File.separator
//                + "Head_6", PreFix + File.separator
//                + "Head_7", PreFix + File.separator
//                + "Head_8", PreFix + File.separator
//                
//                + "LeftAdj", PreFix + File.separator
//                + "RightAdj", PreFix + File.separator
//                
//      		  + "ThreeDuplicate", PreFix + File.separator 
//                + "SimplePath", PreFix + File.separator
//                + "CyclePath", PreFix + File.separator
//                + "RingPath", PreFix + File.separator
//                + "SimpleBridgePath", PreFix + File.separator
//                + "SimpleTreePath", PreFix + File.separator

//                + "SelfTandemRepeat", PreFix + File.separator
//                + "TandemRepeatWithMergeEdge", PreFix + File.separator 
//                + "TandemRepeatWithUnmergeEdge", PreFix + File.separator
//                + "ComplexTandemRepeat", PreFix + File.separator
//                + "TandemRepeatAndCycle", PreFix + File.separator
//      	      + "TandemRepeatAndThreeNodes", PreFix + File.separator
//    			  + "ThreeNodesCycle", PreFix + File.separator
//                
//                + "MultiTandemRepeat", PreFix + File.separator
//                + "MultiTandemRepeat2", PreFix + File.separator
//                + "MultiTandemRepeat3"};//, PreFix + File.separator
//                
//                + "AlreadyInEdgeList", PreFix + File.separator
//                
//                + "Cluster", PreFix + File.separator
//                + "SameGeneCodeWithoutEdge", PreFix + File.separator 
//                + "SameGeneCodeWithEdge", PreFix + File.separator 
//                + "synthetic"
              // Bubble Merge
//    	+ "LtoL", PreFix + File.separator
//    	+ "LtoR", PreFix + File.separator
//    	+ "RtoL", PreFix + File.separator
//    	+ "RtoR"
    	+ "SimpleBubble", PreFix + File.separator
    	+ "BubbleWithTip", PreFix + File.separator
    	+ "SideBubble", PreFix + File.separator
    	+ "OverlapBubble", PreFix + File.separator
    	+ "FRandRF"
                };
//    + "InvalidBubble", PreFix + File.separator
//                + "SimpleRectangle", PreFix + File.separator
//                + "MediumRectangle", PreFix + File.separator
//                + "ComplexRectangle", PreFix + File.separator
//                + "SimpleBubble", PreFix + File.separator
//                + "InvalidBubble", PreFix + File.separator
//                + "BubbleWithTip", PreFix + File.separator
//                + "SideBubble", PreFix + File.separator
//                + "OverlapBubble", PreFix + File.separator
    //        + "SmallGenome"
    //        + "SmallCycle"
    //        + "sameWithEdge"
           
    //    };
    //        + "Rectangle", PreFix + File.separator
    //        + "Grid"};
    //        + "SimpleSplitManyTimes", PreFix + File.separator
    //        + "ComplexSplitManyTimes"};
    //        + "TandemRepeatWithMergeEdge", PreFix + File.separator
    //        + "TandemRepeatWithUnmergeEdge"};
    //        + "SimplePath", PreFix + File.separator
    //        + "BridgePath", PreFix + File.separator
    //        + "TreePath", PreFix + File.separator
    //        + "CyclePath", PreFix + File.separator
    //        + "ThreeDuplicate", PreFix + File.separator
    //        + "SelfPath", PreFix + File.separator
    //        + "PairedEndTest" };//, PreFix + File.separator};
    //        + "HighSplitRepeat", PreFix + File.separator
    //        + "LowSplitRepeat", PreFix + File.separator
    //        + "MidSplitRepeat", PreFix + File.separator
    //        + "Tips1", PreFix + File.separator
    //        + "Tips2", PreFix + File.separator
    //        + "Tips3", PreFix + File.separator
    //        + "Tips4"};

    private static JobConf conf = new JobConf();
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private static final String HDFS_INPUTPATH = "/webmap";

    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private static FileSystem dfs;

    public void setUp() throws Exception {
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHadoop();
    }

    private void startHadoop() throws IOException {
        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        dfs = dfsCluster.getFileSystem();
        mrCluster = new MiniMRCluster(1, dfs.getUri().toString(), 1);

        for (String testDir : TestDir) {
            File src = new File(testDir);
            Path dest = new Path(HDFS_INPUTPATH + File.separator + src.getName());
            dfs.mkdirs(dest);
            for (File f : src.listFiles()) {
                dfs.copyFromLocalFile(new Path(f.getAbsolutePath()), dest);
            }
        }
        //        
        //        Path src = new Path(DATA_PATH);
        //        Path dest = new Path(HDFS_PATH + "/");
        //        dfs.mkdirs(dest);
        //        dfs.copyFromLocalFile(src, dest);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    public void tearDown() throws Exception {
        cleanupHadoop();
    }

    private void cleanupHadoop() throws IOException {
        mrCluster.shutdown();
        dfsCluster.shutdown();
    }

    public static Test suite() throws Exception {
        GraphBuildingTestSuite testSuite = new GraphBuildingTestSuite();
        testSuite.setUp();
        //        FileSystem dfs = FileSystem.get(testSuite.conf);
        for (String testPathStr : TestDir) {
            File testDir = new File(testPathStr);
            String resultFileName = ACTUAL_RESULT_DIR + File.separator + "bin" + File.separator + testDir.getName();
            testSuite.addTest(new GraphBuildingTestCase(resultFileName, HADOOP_CONF_PATH, HDFS_INPUTPATH
                    + File.separator + testDir.getName(), SIZE_KMER, LINES_PERMAP, dfs, conf));
        }
        return testSuite;
    }

    /**
     * Runs the tests and collects their result in a TestResult.
     */
    @Override
    public void run(TestResult result) {
        try {
            int testCount = countTestCases();
            for (int i = 0; i < testCount; i++) {
                // cleanupStores();
                Test each = this.testAt(i);
                if (result.shouldStop())
                    break;
                runTest(each, result);
            }
            tearDown();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
