/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.genomix.minicluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ReflectionUtils;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;
import edu.uci.ics.pregelix.runtime.bootstrap.NCApplicationEntryPoint;

/**
 * 
 *
 */
public class GenomixClusterManager {

    public enum ClusterType {
        HYRACKS,
        PREGELIX,
        HADOOP
    }
    
    private static final Log LOG = LogFactory.getLog(GenomixClusterManager.class);
    public static final String LOCAL_HOSTNAME = "localhost";
    public static final String LOCAL_IP = "127.0.0.1";
    public static final int LOCAL_CLIENT_PORT = 3099;
    public static final int LOCAL_CC_PORT = 1099;

    private ClusterControllerService localCC;
    private NodeControllerService localNC;
    private MiniDFSCluster localDFSCluster;
    private MiniMRCluster localMRCluster;

    private final boolean runLocal;
    private final GenomixJobConf conf;
    private boolean jarsCopiedToHadoop = false;

    private HashMap<ClusterType, Thread> shutdownHooks = new HashMap<ClusterType, Thread>(); 

    public GenomixClusterManager(boolean runLocal, GenomixJobConf conf) {
        this.runLocal = runLocal;
        this.conf = conf;
    }

    /**
     * Start a cluster of the given type. If runLocal is specified, we will create an in-memory version of the cluster.
     * 
     */
    public void startCluster(ClusterType clusterType) throws Exception {
        addClusterShutdownHook(clusterType);
        switch (clusterType) {
            case HYRACKS:
            case PREGELIX:
                if (runLocal) {
                    startLocalCC();
                    startLocalNC(clusterType);
                } else {
                    int sleepms = Integer.parseInt(conf.get(GenomixJobConf.CLUSTER_WAIT_TIME));
                    startCC(sleepms);
                    startNCs(clusterType, sleepms);
                }
                break;
            case HADOOP:
                if (runLocal)
                    startLocalMRCluster();
                else
                    deployJarsToHadoop();
                break;
        }
    }

    public void stopCluster(ClusterType clusterType) throws Exception {
            switch (clusterType) {
                case HYRACKS:
                case PREGELIX:
                    if (runLocal) {
                        localCC.stop();
                        localNC.stop();
                    } else {
                        shutdownCC();
                        shutdownNCs();
                    }
                    break;
                case HADOOP:
                    if (runLocal) {
                        localMRCluster.shutdown();
                        localDFSCluster.shutdown();
                    }
                    break;
            }
        removeClusterShutdownHook(clusterType);
    }

    private void startLocalCC() throws Exception {
        LOG.info("Starting local CC...");
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = LOCAL_HOSTNAME;
        ccConfig.clusterNetIpAddress = LOCAL_HOSTNAME;
        ccConfig.clusterNetPort = LOCAL_CC_PORT;
        ccConfig.clientNetPort = LOCAL_CLIENT_PORT;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 1;
        ccConfig.profileDumpPeriod = -1;
        localCC = new ClusterControllerService(ccConfig);
        localCC.start();
    }

    private void startLocalNC(ClusterType clusterType) throws Exception {
        LOG.info("Starting local NC...");
//        ClusterConfig.setClusterPropertiesPath(System.getProperty("app.home") + "/conf/cluster.properties");
//        ClusterConfig.setStorePath(...);
        NCConfig ncConfig = new NCConfig();
        ncConfig.ccHost = LOCAL_HOSTNAME;
        ncConfig.clusterNetIPAddress = LOCAL_HOSTNAME;
        ncConfig.ccPort = LOCAL_CC_PORT;
        ncConfig.dataIPAddress = LOCAL_IP;
        ncConfig.datasetIPAddress = LOCAL_IP;
        ncConfig.nodeId = "nc1";
        ncConfig.ioDevices = "tmp" + File.separator + "t3";
        
        if (clusterType == ClusterType.PREGELIX)
            ncConfig.appNCMainClass = NCApplicationEntryPoint.class.getName();
        localNC = new NodeControllerService(ncConfig);
        localNC.start();
    }

    private void startLocalMRCluster() throws IOException {
        LOG.info("Starting local DFS and MR cluster...");
        localDFSCluster = new MiniDFSCluster(conf, 1, true, null);
        localMRCluster = new MiniMRCluster(1, localDFSCluster.getFileSystem().getUri().toString(), 1);
    }
    
    /**
     * Walk the current CLASSPATH to get all jar's in use and copy them up to all HDFS nodes
     * @throws IOException 
     */
    private void deployJarsToHadoop() throws IOException {
        if (!jarsCopiedToHadoop) {
            LOG.info("Deploying jars in my classpath to HDFS Distributed Cache...");
            FileSystem dfs = FileSystem.get(conf);
            String[] classPath = {System.getenv().get("CLASSPATH"), System.getProperty("java.class.path")};
            for (String cp : classPath) {
                for (String item : cp.split(":")) {
                    if (item.endsWith(".jar")) {
                        Path localJar = new Path(item);
                        Path jarDestDir = new Path(conf.get(GenomixJobConf.HDFS_WORK_PATH) + "/tmp-jars/");
                        dfs.mkdirs(jarDestDir);
                        Path destJar = new Path(jarDestDir + localJar.getName());
                        dfs.copyFromLocalFile(localJar, destJar);
                        DistributedCache.addFileToClassPath(destJar, conf);
                    }
                }
            }
        }
    }

    private static void startNCs(ClusterType type, int sleepms) throws IOException, InterruptedException {
        LOG.info("Starting NC's");
        String startNCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "startAllNCs.sh " + type;
        Process p = Runtime.getRuntime().exec(startNCCmd);
        p.waitFor(); // wait for ssh 
        Thread.sleep(sleepms); // wait for NC -> CC registration
        if (p.exitValue() != 0)
            throw new RuntimeException("Failed to start the" + type + " NC's! Script returned exit code: "
                    + p.exitValue() + "\nstdout: " + IOUtils.toString(p.getInputStream()) + "\nstderr: "
                    + IOUtils.toString(p.getErrorStream()));
    }

    private static void startCC(int sleepms) throws IOException, InterruptedException {
        LOG.info("Starting CC");
        String startCCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "startcc.sh";
        Process p = Runtime.getRuntime().exec(startCCCmd);
        p.waitFor(); // wait for cmd execution
        Thread.sleep(sleepms); // wait for CC registration
        if (p.exitValue() != 0)
            throw new RuntimeException("Failed to start the genomix CC! Script returned exit code: " + p.exitValue()
                    + "\nstdout: " + IOUtils.toString(p.getInputStream()) + "\nstderr: "
                    + IOUtils.toString(p.getErrorStream()));
    }

    private static void shutdownCC() throws IOException, InterruptedException {
        LOG.info("Shutting down CC");
        String stopCCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator + "stopcc.sh";
        Process p = Runtime.getRuntime().exec(stopCCCmd);
        p.waitFor(); // wait for cmd execution
    }

    private static void shutdownNCs() throws IOException, InterruptedException {
        LOG.info("Shutting down any previous NC's");
        String stopNCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "stopAllNCs.sh";
        Process p = Runtime.getRuntime().exec(stopNCCmd);
        p.waitFor(); // wait for ssh 
    }

    private void addClusterShutdownHook(final ClusterType clusterType) {
        if (shutdownHooks.containsKey(clusterType))
            throw new IllegalArgumentException("Already specified a hook for shutting down a " + clusterType + " cluster! (Try removing the existing hook first?)");
        Thread hook = new Thread() {
            @Override
            public void run() {
                LOG.info("Interrupt received... Shutting down the cluster!");
                try {
                    stopCluster(clusterType);
                } catch (Exception e) {
                    System.err.println("Error while shutting the cluster down:");
                    e.printStackTrace();
                }
            }
        };
        shutdownHooks.put(clusterType, hook);
        Runtime.getRuntime().addShutdownHook(hook);
    }
    
    private void removeClusterShutdownHook(final ClusterType clusterType) {
        if (!shutdownHooks.containsKey(clusterType))
            throw new IllegalArgumentException("There is no shutdown hook for " + clusterType + "!");
        Runtime.getRuntime().removeShutdownHook(shutdownHooks.get(clusterType));
    }

    public static void copyLocalToHDFS(JobConf conf, String localDir, String destDir) throws IOException {
        LOG.info("Copying local directory " + localDir + " to HDFS: " + destDir);
        GenomixJobConf.tick("copyLocalToHDFS");
        FileSystem dfs = FileSystem.get(conf);
        Path dest = new Path(destDir);
        dfs.delete(dest, true);
        dfs.mkdirs(dest);

        File srcBase = new File(localDir);
        if (srcBase.isDirectory())
            for (File f : srcBase.listFiles())
                dfs.copyFromLocalFile(new Path(f.toString()), dest);
        else
            dfs.copyFromLocalFile(new Path(localDir), dest);

        LOG.info("Copy took " + GenomixJobConf.tock("copyLocalToHDFS") + "ms");
    }

    public static void copyBinToLocal(JobConf conf, String hdfsSrcDir, String localDestDir) throws IOException {
        LOG.info("Copying HDFS directory " + hdfsSrcDir + " to local: " + localDestDir);
        GenomixJobConf.tick("copyBinToLocal");
        FileSystem dfs = FileSystem.get(conf);
        FileUtils.deleteQuietly(new File(localDestDir));

        // save original binary to output/bin
        dfs.copyToLocalFile(new Path(hdfsSrcDir), new Path(localDestDir + File.separator + "bin"));

        // convert hdfs sequence files to text as output/text
        BufferedWriter bw = null;
        SequenceFile.Reader reader = null;
        Writable key = null;
        Writable value = null;
        FileStatus[] files = dfs.globStatus(new Path(hdfsSrcDir + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0 && !f.isDir()) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    if (bw == null)
                        bw = new BufferedWriter(new FileWriter(localDestDir + File.separator + "data"));
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        bw.write(key.toString() + "\t" + value.toString());
                        bw.newLine();
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error copying " + f + " to local:\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }

            }
        }
        if (bw != null)
            bw.close();
        LOG.info("Copy took " + GenomixJobConf.tock("copyBinToLocal") + "ms");
    }
}
