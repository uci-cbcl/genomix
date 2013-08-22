package edu.uci.ics.genomix.minicluster;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.wicket.util.file.File;

import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.runtime.bootstrap.NCApplicationEntryPoint;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

public class GenomixMiniCluster {
    
    public static final String NC1_ID = "nc1";
    public static final int TEST_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_CLIENT_PORT = 2099;
    public static final String CC_HOST = "localhost";

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static MiniDFSCluster dfsCluster;
    private static Path clusterProperties;
    private static Path clusterStores;
    private static Path clusterWorkDir;
    private static Path clusterStoresDir;
    
    public static void init(GenomixJobConf conf) throws Exception {
        makeLocalClusterConfig();
        ClusterConfig.setClusterPropertiesPath(clusterProperties.toAbsolutePath().toString());
        ClusterConfig.setStorePath(clusterStores.toAbsolutePath().toString());
//        dfsCluster = new MiniDFSCluster(conf, Integer.parseInt(conf.get(GenomixJobConf.CORES_PER_MACHINE)), true, null);
        
        // cluster controller
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = CC_HOST;
        ccConfig.clusterNetIpAddress = CC_HOST;
        ccConfig.clusterNetPort = TEST_HYRACKS_CC_PORT;
        ccConfig.clientNetPort = TEST_HYRACKS_CC_CLIENT_PORT;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 1;
        ccConfig.profileDumpPeriod = -1;
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // one node controller
        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.clusterNetIPAddress = "localhost";
        ncConfig1.ccPort = TEST_HYRACKS_CC_PORT;
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.datasetIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        ncConfig1.ioDevices = clusterWorkDir.toString() + File.separator + "tmp" + File.separator + "t3";
        ncConfig1.appNCMainClass = NCApplicationEntryPoint.class.getName();
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();
    }

    public static void deinit() throws Exception {
        nc1.stop();
        cc.stop();
//        dfsCluster.shutdown();
        FileUtils.deleteQuietly(new File("build"));
        FileUtils.deleteQuietly(clusterProperties.toFile());
        FileUtils.deleteQuietly(clusterStores.toFile());
        FileUtils.deleteQuietly(clusterWorkDir.toFile());
        FileUtils.deleteQuietly(clusterStoresDir.toFile());
    }

    /**
     * create the necessary .properties and directories for a minicluster instance 
     */
    private static void makeLocalClusterConfig() throws IOException {
        clusterProperties = Files.createTempFile(FileSystems.getDefault().getPath("."), "tmp.cluster", ".properties");
        clusterWorkDir = Files.createTempDirectory(FileSystems.getDefault().getPath("."), "tmp.clusterWorkDir");
        PrintWriter writer = new PrintWriter(clusterProperties.toString(), "US-ASCII");
        writer.println("CC_CLIENTPORT=" + TEST_HYRACKS_CC_CLIENT_PORT);
        writer.println("CC_CLUSTERPORT=" + TEST_HYRACKS_CC_PORT);
        writer.println(String.format("WORKPATH=%s", clusterWorkDir.toAbsolutePath().toString()));
        writer.println("CCTMP_DIR=${WORKPATH}/tmp/t1");
        writer.println("NCTMP_DIR=${WORKPATH}/tmp/t2");
        writer.println("CCLOGS_DIR=$CCTMP_DIR/logs");
        writer.println("NCLOGS_DIR=$NCTMP_DIR/logs");
        writer.println("IO_DIRS=${WORKPATH}/tmp/t3");
        writer.println("JAVA_HOME=$JAVA_HOME");
        writer.println("CLASSPATH=\"${HADOOP_HOME}:${CLASSPATH}:.\"");
        writer.println("FRAME_SIZE=65536");
        writer.println("CCJAVA_OPTS=\"-Xmx1g -Djava.util.logging.config.file=logging.properties\"");
        writer.println("NCJAVA_OPTS=\"-Xmx1g -Djava.util.logging.config.file=logging.properties\"");
        writer.close();

        clusterStoresDir = Files.createTempDirectory(FileSystems.getDefault().getPath("."), "tmp.clusterStores");
        clusterStores = Files.createTempFile(FileSystems.getDefault().getPath("."), "tmp.stores", ".properties");
        writer = new PrintWriter(clusterStores.toString(), "US-ASCII");
        writer.println(String.format("store=%s", clusterStoresDir.toString()));
        writer.close();
    }
}
