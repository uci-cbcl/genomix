package edu.uci.ics.genomix.example.jobrun;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import edu.uci.ics.genomix.driver.Driver;
import edu.uci.ics.hyracks.hdfs.utils.HyracksUtils;

public class JobRunTestCase extends TestCase{
    private static final String ACTUAL_RESULT_DIR = "actual";
    private static final String EXPECTED_RESULT_PATH = "src/test/resources/expected";
    private static final String PATH_TO_HADOOP_CONF = "src/test/resources/hadoop/conf";

    private static final String DATA_PATH = "src/test/resources/data/customer.tbl";
    private static final String HDFS_INPUT_PATH = "/customer/";
    private static final String HDFS_OUTPUT_PATH = "/customer_result/";

    private static final String HYRACKS_APP_NAME = "genomix";
    private static final String HADOOP_CONF_PATH = ACTUAL_RESULT_DIR + File.separator + "conf.xml";
    private MiniDFSCluster dfsCluster;

    private JobConf conf = new JobConf();
    private int numberOfNC = 2;
    private int numPartitionPerMachine=2;
    
    private Driver myDriver;
	@Override
	protected void setUp() throws Exception {
        cleanupStores();
        HyracksUtils.init();
        HyracksUtils.createApp(HYRACKS_APP_NAME);
        FileUtils.forceMkdir(new File(ACTUAL_RESULT_DIR));
        FileUtils.cleanDirectory(new File(ACTUAL_RESULT_DIR));
        startHDFS();
        
        myDriver = new Driver(HyracksUtils.CC_HOST, HyracksUtils.TEST_HYRACKS_CC_CLIENT_PORT, numPartitionPerMachine);
    }

    private void cleanupStores() throws IOException {
        FileUtils.forceMkdir(new File("teststore"));
        FileUtils.forceMkdir(new File("build"));
        FileUtils.cleanDirectory(new File("teststore"));
        FileUtils.cleanDirectory(new File("build"));
    }

    private void startHDFS() throws IOException {
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/core-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/mapred-site.xml"));
        conf.addResource(new Path(PATH_TO_HADOOP_CONF + "/hdfs-site.xml"));

        FileSystem lfs = FileSystem.getLocal(new Configuration());
        lfs.delete(new Path("build"), true);
        System.setProperty("hadoop.log.dir", "logs");
        dfsCluster = new MiniDFSCluster(conf, numberOfNC, true, null);
        FileSystem dfs = FileSystem.get(conf);
        Path src = new Path(DATA_PATH);
        Path dest = new Path(HDFS_INPUT_PATH);
        Path result = new Path(HDFS_OUTPUT_PATH);
        dfs.mkdirs(dest);
        dfs.mkdirs(result);
        dfs.copyFromLocalFile(src, dest);

        DataOutputStream confOutput = new DataOutputStream(new FileOutputStream(new File(HADOOP_CONF_PATH)));
        conf.writeXml(confOutput);
        confOutput.flush();
        confOutput.close();
    }

    
	@Override
	protected void runTest() throws Throwable {
		TestExternalGroupby();
		TestPreClusterGroupby();
		TestHybridGroupby();
	}
	
	void TestExternalGroupby() throws Exception{
		// TODO
	}
	
	void TestPreClusterGroupby() throws Exception{
		// TODO
	}
	
	void TestHybridGroupby() throws Exception{
		// TODO
	}

	
	@Override
	protected void tearDown() throws Exception {
        HyracksUtils.destroyApp(HYRACKS_APP_NAME);
        HyracksUtils.deinit();
        cleanupHDFS();
	}
	
    private void cleanupHDFS() throws Exception {
        dfsCluster.shutdown();
    }
	
}
