package edu.uci.ics.genomix.minicluster;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.Path;

import org.apache.hadoop.hdfs.MiniDFSCluster;

import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.core.util.PregelixHyracksIntegrationUtil;
import edu.uci.ics.genomix.config.GenomixJobConf;

@SuppressWarnings("deprecation")
public class GenomixMiniCluster {
    private MiniDFSCluster dfsCluster;
    private Path clusterProperties;
    private Path clusterStores;
    private Path clusterWorkDir;
    private Path clusterStoresDir;

    private void makeLocalClusterConfig() throws IOException {
        clusterProperties = Files.createTempFile(FileSystems.getDefault().getPath("."), "tmp.cluster", ".properties");
        clusterWorkDir = Files.createTempDirectory(FileSystems.getDefault().getPath("."), "tmp.clusterWorkDir");
        PrintWriter writer = new PrintWriter(clusterProperties.toString(), "US-ASCII");
        writer.println(String.format("WORKPATH=\"%s\"", clusterWorkDir.toAbsolutePath().toString()));
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
        writer.println(String.format("store=\"%s\"", clusterStoresDir.toAbsolutePath().toString()));
        writer.close();
    }

    public void setUp(GenomixJobConf conf, String clusterStore, String clusterProperties) throws Exception {
        makeLocalClusterConfig();
        ClusterConfig.setClusterPropertiesPath(clusterPropertiesDir.toAbsolutePath().toString() + File.separator + "cluster.properties");
        ClusterConfig.setStorePath(clusterPropertiesDir.toAbsolutePath().toString() + File.separator + "stores.properties");
        PregelixHyracksIntegrationUtil.init();
        dfsCluster = new MiniDFSCluster(conf, Integer.parseInt(conf.get(GenomixJobConf.CPARTITION_PER_MACHINE)), true,
                null);
    }

    public void tearDown() throws Exception {
        PregelixHyracksIntegrationUtil.deinit();
        dfsCluster.shutdown();
    }
}
