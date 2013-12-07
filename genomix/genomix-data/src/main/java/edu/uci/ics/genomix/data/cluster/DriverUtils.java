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

package edu.uci.ics.genomix.data.cluster;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;

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
    public static void loadClusterProperties(GenomixJobConf conf) throws FileNotFoundException, IOException,
            InterruptedException {
        Properties clusterProperties = new Properties();
        clusterProperties.load(new FileInputStream(System.getProperty("app.home", ".") + File.separator + "conf"
                + File.separator + "cluster.properties"));

        for (String prop : clusterProperties.stringPropertyNames()) {
            conf.set(prop, clusterProperties.getProperty(prop));
        }
        if (conf.get(GenomixJobConf.MASTER) == null) {
            String master = FileUtils.readFileToString(new File(System.getProperty("app.home", ".") + File.separator
                    + "conf" + File.separator + "master"));
            conf.set(GenomixJobConf.MASTER, master);
        }
        if (conf.get(GenomixJobConf.SLAVES) == null) {
            String slaves = FileUtils.readFileToString(new File(System.getProperty("app.home", ".") + File.separator
                    + "conf" + File.separator + "slaves"));
            conf.set(GenomixJobConf.SLAVES, slaves);
        }
    }

    public static void dumpGraph(JobConf conf, String inputGraph, String outputDir) throws IOException {
        LOG.info("Dumping graph to fasta...");
        GenomixJobConf.tick("dumpGraph");
        FileSystem dfs = FileSystem.get(conf);

        dfs.delete(new Path(outputDir), true);
        dfs.mkdirs(new Path(outputDir));
        String outputFasta = outputDir + File.separator + "genomix-scaffolds.fasta";

        // stream in the graph, counting elements as you go... this would be better as a hadoop job which aggregated... maybe into counters?
        SequenceFile.Reader reader = null;
        VKmer key = null;
        Node value = null;
        FSDataOutputStream fastaOut = null;
        FileStatus[] files = dfs.globStatus(new Path(inputGraph + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {

                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmer) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (Node) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    if (fastaOut == null)
                        fastaOut = dfs.create(new Path(outputFasta));
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        fastaOut.writeBytes(">node_" + key.toString() + "\n");
                        fastaOut.writeBytes(value.getInternalKmer().getKmerLetterLength() > 0 ? value.getInternalKmer()
                                .toString() : key.toString());
                        fastaOut.writeBytes("\n");
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting stats for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }
        if (fastaOut != null)
            fastaOut.close();
        LOG.info("Dump graph to fasta took " + GenomixJobConf.tock("dumpGraph") + "ms");
    }

    public static String[] getSlaveList(GenomixJobConf conf) {
        return conf.get(GenomixJobConf.SLAVES).split("\r?\n|\r"); // split on newlines
    }

}
