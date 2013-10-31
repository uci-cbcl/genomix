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
import edu.uci.ics.pregelix.runtime.bootstrap.NCApplicationEntryPoint;

/**
 * 
 *
 */
public class GenomixClusterManager {
    private static final Log LOG = LogFactory.getLog(GenomixClusterManager.class);
    public static final String LOCAL_HOSTNAME = "localhost";
    public static final String LOCAL_IP = "127.0.0.1";
    public static final int LOCAL_HYRACKS_CLIENT_PORT = 3099;
    public static final int LOCAL_HYRACKS_CC_PORT = 1099;
    public static final int LOCAL_PREGELIX_CLIENT_PORT = 3097;
    public static final int LOCAL_PREGELIX_CC_PORT = 1097;

    private ClusterControllerService localHyracksCC;
    private NodeControllerService localHyracksNC;
    private ClusterControllerService localPregelixCC;
    private NodeControllerService localPregelixNC;
    private MiniDFSCluster localDFSCluster;
    private MiniMRCluster localMRCluster;

    private final boolean runLocal;
    private final GenomixJobConf conf;
    private boolean jarsCopiedToHadoop = false;

    private Thread shutdownHook;
    private int numberDataNodes = 1; // For testing, the number of DataNodes in the mini hdfs setting
    private int numberNC = 1; // For testing, the number of NC's in local hyracks/pregelix setting.

    public GenomixClusterManager(boolean runLocal, GenomixJobConf conf) {
        this.runLocal = runLocal;
        this.conf = conf;
    }

    public void setNumberOfDataNodesInLocalMiniHDFS(int number) {
        numberDataNodes = number;
    }

    public void setNumberOfNC(int number) {
        numberNC = number;
    }

    /**
     * Start a cluster of the given type. If runLocal is specified, we will
     * create an in-memory version of the cluster.
     */
    public void startCluster() throws Exception {
        addClusterShutdownHook();
        if (runLocal) {
            startLocalMRCluster();
            startLocalCC();
            for (int i = 0; i < numberNC; i++) {
                startLocalNC(i);
            }
        } else {
            startRemoteCluster();
            deployJarsToHadoop();
        }
    }

    private void startRemoteCluster() throws IOException, InterruptedException {
        LOG.info("Starting hyracks and pregelix clusters");
        int threadsPerMachine = Integer.parseInt(conf.get(GenomixJobConf.THREADS_PER_MACHINE));
        String startCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "startCluster.sh " + threadsPerMachine;
        Process p = Runtime.getRuntime().exec(startCmd);
        p.waitFor(); // wait for cmd execution
        if (p.exitValue() != 0) {
            throw new RuntimeException("Failed to start the genomix cluster! Script returned exit code: "
                    + p.exitValue() + "\nstdout: " + IOUtils.toString(p.getInputStream()) + "\nstderr: "
                    + IOUtils.toString(p.getErrorStream()));
        }

    }

    public void stopCluster() throws Exception {
        if (runLocal) {
            // allow multiple calls to stopCluster
            if (localHyracksCC != null) {
                localHyracksCC.stop();
                localHyracksCC = null;
            }
            if (localHyracksNC != null) {
                localHyracksNC.stop();
                localHyracksNC = null;
            }
            if (localPregelixCC != null) {
                localPregelixCC.stop();
                localPregelixCC = null;
            }
            if (localPregelixNC != null) {
                localPregelixNC.stop();
                localPregelixNC = null;
            }
            if (localMRCluster != null) {
                localMRCluster.shutdown();
                localMRCluster = null;
            }
            if (localDFSCluster != null) {
                localDFSCluster.shutdown();
                localDFSCluster = null;
            }
        } else {
            stopRemoteCluster();
        }
        removeClusterShutdownHook();
    }

    private void stopRemoteCluster() throws IOException, InterruptedException {
        LOG.info("Stopping hyracks and pregelix clusters");
        String stopCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "stopCluster.sh";
        Process p = Runtime.getRuntime().exec(stopCmd);
        p.waitFor(); // wait for cmd execution
        if (p.exitValue() != 0) {
            LOG.debug("Failed to stop the genomix cluster! Script returned exit code: " + p.exitValue() + "\nstdout: "
                    + IOUtils.toString(p.getInputStream()) + "\nstderr: " + IOUtils.toString(p.getErrorStream()));
        }
    }

    private void startLocalCC() throws Exception {
        LOG.info("Starting local CC's...");
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = LOCAL_HOSTNAME;
        ccConfig.clusterNetIpAddress = LOCAL_HOSTNAME;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 1;
        ccConfig.profileDumpPeriod = -1;

        ccConfig.clusterNetPort = LOCAL_HYRACKS_CC_PORT;
        ccConfig.clientNetPort = LOCAL_HYRACKS_CLIENT_PORT;
        localHyracksCC = new ClusterControllerService(ccConfig);
        localHyracksCC.start();

        ccConfig.clusterNetPort = LOCAL_PREGELIX_CC_PORT;
        ccConfig.clientNetPort = LOCAL_PREGELIX_CLIENT_PORT;
        localPregelixCC = new ClusterControllerService(ccConfig);
        localPregelixCC.start();
    }

    private void startLocalNC(int id) throws Exception {
        LOG.info("Starting local NC...");
        NCConfig ncConfig = new NCConfig();
        ncConfig.ccHost = LOCAL_HOSTNAME;
        ncConfig.clusterNetIPAddress = LOCAL_HOSTNAME;
        ncConfig.dataIPAddress = LOCAL_IP;
        ncConfig.datasetIPAddress = LOCAL_IP;

        ncConfig.ccPort = LOCAL_HYRACKS_CC_PORT;
        ncConfig.nodeId = "nc-HYRACKS-id-" + id;
        ncConfig.ioDevices = "tmp" + File.separator + "t3" + File.separator + "HYRACKS";
        localHyracksNC = new NodeControllerService(ncConfig);
        localHyracksNC.start();

        ncConfig.ccPort = LOCAL_PREGELIX_CC_PORT;
        ncConfig.appNCMainClass = NCApplicationEntryPoint.class.getName();
        ncConfig.nodeId = "nc-PREGELIX-id-" + id;
        ncConfig.ioDevices = "tmp" + File.separator + "t3" + File.separator + "PREGELIX";
        localPregelixNC = new NodeControllerService(ncConfig);
        localPregelixNC.start();
    }

    private void startLocalMRCluster() throws IOException {
        LOG.info("Starting local DFS and MR cluster...");
        System.setProperty("hadoop.log.dir", "logs");
        localDFSCluster = new MiniDFSCluster(conf, numberDataNodes, true, null);
        localMRCluster = new MiniMRCluster(1, localDFSCluster.getFileSystem().getUri().toString(), 1);
    }

    /**
     * Walk the current CLASSPATH to get all jar's in use and copy them up to
     * all HDFS nodes
     * 
     * @throws IOException
     */
    private void deployJarsToHadoop() throws IOException {
        if (!jarsCopiedToHadoop) {
            LOG.info("Deploying jars in my classpath to HDFS Distributed Cache...");
            FileSystem dfs = FileSystem.get(conf);
            String[] classPath = { System.getenv().get("CLASSPATH"), System.getProperty("java.class.path") };
            for (String cp : classPath) {
                if (cp == null)
                    continue;
                for (String item : cp.split(":")) {
                    // LOG.info("Checking " + item);
                    if (item.endsWith(".jar")) {
                        // LOG.info("Deploying " + item);
                        Path localJar = new Path(item);
                        Path jarDestDir = new Path(conf.get(GenomixJobConf.HDFS_WORK_PATH) + "/jar-dependencies");
                        // dist cache requires absolute paths. we have to use
                        // the working directory if HDFS_WORK_PATH is relative
                        if (!jarDestDir.isAbsolute()) {
                            jarDestDir = new Path(getAbsolutePathFromURI(dfs.getWorkingDirectory().toString())
                                    + File.separator + jarDestDir);
                        }
                        dfs.mkdirs(jarDestDir);
                        Path destJar = new Path(jarDestDir + File.separator + localJar.getName());
                        dfs.copyFromLocalFile(localJar, destJar);
                        // LOG.info("Jar in distributed cache: " + destJar);
                        DistributedCache.addFileToClassPath(destJar, conf);
                    }
                }
            }
        }
    }

    private static String getAbsolutePathFromURI(String URI) {
        // working dir is the correct base, but we must use the path version
        // (not a URI). Get URI and strip out leading identifiers
        String hostNameRE = "([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])(\\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9]))*";
        String[] workDirs = URI.split("(hdfs://" + hostNameRE + ":\\d+|file:)", 2);
        if (workDirs.length <= 1) {
            LOG.info("Weird.... didn't find a URI header matching hdfs://host:port or file:  Just using the original instead.");
            return URI;
        } else {
            return workDirs[1];
        }
    }

    private void addClusterShutdownHook() {
        if (shutdownHook != null) {
            throw new IllegalArgumentException(
                    "Already specified a hook for shutting down the cluster (Try removing the existing hook first?)");
        }
        shutdownHook = new Thread() {
            @Override
            public void run() {
                LOG.info("Shutting down the cluster...");
                try {
                    stopCluster();
                } catch (Exception e) {
                    System.err.println("Error while shutting the cluster down:");
                    e.printStackTrace();
                }
            }
        };
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void removeClusterShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // ignore: we must already be shutting down
            } finally {
                shutdownHook = null;
            }
        }
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

    /**
     * Copy the file from hdfs and transform the binary format into text format.
     * The original bin files are stored in the {@code localDestDir}/bin folder.
     * The transformed text files are stored in the {@code localDestDir}/data file.
     * 
     * @param conf
     * @param hdfsSrcDir
     * @param localDestDir
     * @throws IOException
     */
    public static void copyBinToLocal(JobConf conf, String hdfsSrcDir, String localDestDir) throws IOException {
        LOG.info("Copying HDFS directory " + hdfsSrcDir + " to local: " + localDestDir);
        GenomixJobConf.tick("copyBinToLocal");
        FileSystem dfs = FileSystem.get(conf);
        FileUtils.deleteQuietly(new File(localDestDir));

        // save original binary to output/bin
        dfs.copyToLocalFile(new Path(hdfsSrcDir), new Path(localDestDir + File.separator + "bin"));

        // convert hdfs sequence files to text as output/data
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
                    System.err.println("Encountered an error copying " + f + " to local:\n" + e);
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
