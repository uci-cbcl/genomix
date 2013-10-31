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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

public class DriverUtils {

    public static final Logger LOG = Logger.getLogger(DriverUtils.class.getName());

    /*
     * Get the IP address of the master node using the bin/getip.sh script
     */
    public static String getIP(String hostName) throws IOException, InterruptedException {
        String getIPCmd = "ssh -n " + hostName + " \"" + System.getProperty("app.home", ".") + File.separator + "bin"
                + File.separator + "getip.sh\"";
        Process p = Runtime.getRuntime().exec(getIPCmd);
        p.waitFor(); // wait for ssh 
        String stdout = IOUtils.toString(p.getInputStream()).trim();
        if (p.exitValue() != 0)
            throw new RuntimeException("Failed to get the ip address of the master node! Script returned exit code: "
                    + p.exitValue() + "\nstdout: " + stdout + "\nstderr: " + IOUtils.toString(p.getErrorStream()));
        return stdout;
        //      InetAddress address = InetAddress.getByName(hostName);
        //      System.out.println("inetAddress for " + hostName + address.getHostAddress());
        //      return address.getHostAddress();
    }

    /**
     * set the CC's IP address and port from the cluster.properties and `getip.sh` script
     */
    public static void updateCCProperties(GenomixJobConf conf) throws FileNotFoundException, IOException,
            InterruptedException {
        Properties CCProperties = new Properties();
        CCProperties.load(new FileInputStream(System.getProperty("app.home", ".") + File.separator + "conf"
                + File.separator + "cluster.properties"));
        if (Boolean.parseBoolean(conf.get(GenomixJobConf.RUN_LOCAL))) {
            if (conf.get(GenomixJobConf.IP_ADDRESS) == null) {
                conf.set(GenomixJobConf.IP_ADDRESS, GenomixClusterManager.LOCAL_IP);
            }
            if (conf.getInt(GenomixJobConf.PORT, -1) == -1) {
                conf.setInt(GenomixJobConf.PORT, GenomixClusterManager.LOCAL_HYRACKS_CC_PORT);
            }
        } else {
            if (conf.get(GenomixJobConf.IP_ADDRESS) == null) {
                conf.set(GenomixJobConf.IP_ADDRESS, getIP("localhost"));
            }
            if (conf.getInt(GenomixJobConf.PORT, -1) == -1) {
                conf.set(GenomixJobConf.PORT, CCProperties.getProperty("CC_CLIENTPORT"));
            }
        }

        if (conf.get(GenomixJobConf.FRAME_SIZE) == null)
            conf.set(GenomixJobConf.FRAME_SIZE, CCProperties.getProperty("FRAME_SIZE"));
        if (conf.get(GenomixJobConf.FRAME_LIMIT) == null)
            conf.set(GenomixJobConf.FRAME_LIMIT, CCProperties.getProperty("FRAME_LIMIT"));
        if (conf.get(GenomixJobConf.HYRACKS_IO_DIRS) == null)
            conf.set(GenomixJobConf.HYRACKS_IO_DIRS, CCProperties.getProperty("IO_DIRS"));
        if (conf.get(GenomixJobConf.HYRACKS_SLAVES) == null) {
            String slaves = FileUtils.readFileToString(new File(System.getProperty("app.home", ".") + File.separator
                    + "conf" + File.separator + "slaves"));
            conf.set(GenomixJobConf.HYRACKS_SLAVES, slaves);
        }
    }

    public static void dumpGraphLocally(JobConf conf, String inputGraph, String outputFasta, boolean followingBuild)
            throws IOException {
        LOG.info("Dumping graph to fasta...");
        GenomixJobConf.tick("dumpGraph");
        FileSystem dfs = FileSystem.get(conf);

        // stream in the graph, counting elements as you go... this would be better as a hadoop job which aggregated... maybe into counters?
        SequenceFile.Reader reader = null;
        VKmer key = null;
        Node value = null;
        BufferedWriter bw = null;
        FileStatus[] files = dfs.globStatus(new Path(inputGraph + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {

                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmer) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Node) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    if (bw == null)
                        bw = new BufferedWriter(new FileWriter(outputFasta));
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        bw.write(">node_" + key.toString() + "\n");
                        bw.write(followingBuild ? key.toString() : value.getInternalKmer().toString());
                        bw.newLine();
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting stats for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
        if (bw != null)
            bw.close();
        LOG.info("Dump graph to fasta took " + GenomixJobConf.tock("dumpGraph") + "ms");
    }

    public static int getNumCoresPerMachine(GenomixJobConf conf) {
        return conf.get(GenomixJobConf.HYRACKS_IO_DIRS).split(",").length;
    }

    public static String[] getSlaveList(GenomixJobConf conf) {
        return conf.get(GenomixJobConf.HYRACKS_SLAVES).split("\r?\n|\r"); // split on newlines
    }

}
