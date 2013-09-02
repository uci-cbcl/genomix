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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class DriverUtils {

    public static final Log LOG = LogFactory.getLog(DriverUtils.class);

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
    }

    /**
     * set the CC's IP address and port from the cluster.properties and `getip.sh` script
     */
    public static void updateCCProperties(GenomixJobConf conf) throws FileNotFoundException, IOException, InterruptedException {
        Properties CCProperties = new Properties();
        CCProperties.load(new FileInputStream(System.getProperty("app.home", ".") + File.separator + "conf"
                + File.separator + "cluster.properties"));
        if (conf.get(GenomixJobConf.IP_ADDRESS) == null)
            conf.set(GenomixJobConf.IP_ADDRESS, getIP("localhost"));
        if (Integer.parseInt(conf.get(GenomixJobConf.PORT)) == -1) {
            conf.set(GenomixJobConf.PORT, CCProperties.getProperty("CC_CLIENTPORT"));
        }
        if (conf.get(GenomixJobConf.FRAME_SIZE) == null)
            conf.set(GenomixJobConf.FRAME_SIZE, CCProperties.getProperty("FRAME_SIZE"));
        if (conf.get(GenomixJobConf.FRAME_LIMIT) == null)
            conf.set(GenomixJobConf.FRAME_LIMIT, CCProperties.getProperty("FRAME_LIMIT"));
        if (conf.get(GenomixJobConf.HYRACKS_IO_DIRS) == null)
            conf.set(GenomixJobConf.HYRACKS_IO_DIRS, CCProperties.getProperty("IO_DIRS"));
    }
    
    public static void drawStatistics(JobConf conf, String inputStats, String outputChart) throws IOException {
        LOG.info("Getting coverage statistics...");
        GenomixJobConf.tick("drawStatistics");
        FileSystem dfs = FileSystem.get(conf);

        // stream in the graph, counting elements as you go... this would be better as a hadoop job which aggregated... maybe into counters?
        SequenceFile.Reader reader = null;
        VKmerBytesWritable key = null;
        NodeWritable value = null;
        TreeMap<Integer, Long> coverageCounts = new TreeMap<Integer, Long>();
        FileStatus[] files = dfs.globStatus(new Path(inputStats + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmerBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (NodeWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        if (key == null || value == null)
                            break;
                        Integer cov = java.lang.Math.round(value.getAverageCoverage());
                        Long count = coverageCounts.get(cov);
                        if (count == null)
                            coverageCounts.put(cov, new Long(1));
                        else
                            coverageCounts.put(cov, count + 1);
                    }
                } catch (Exception e) {
                    System.out.println("Encountered an error getting stats for " + f + ":\n" + e);
                } finally {
                    if (reader != null)
                        reader.close();
                }
            }
        }

        XYSeries series = new XYSeries("Kmer Coverage");
        for (Entry<Integer, Long> pair : coverageCounts.entrySet()) {
            series.add(pair.getKey().floatValue(), pair.getValue().longValue());
        }
        XYDataset xyDataset = new XYSeriesCollection(series);
        JFreeChart chart = ChartFactory.createXYLineChart("Coverage per kmer in " + new File(inputStats).getName(),
                "Coverage", "Count", xyDataset, PlotOrientation.VERTICAL, true, true, false);

        // Write the data to the output stream:
        FileOutputStream chartOut = new FileOutputStream(new File(outputChart));
        ChartUtilities.writeChartAsPNG(chartOut, chart, 800, 600);
        chartOut.flush();
        chartOut.close();
        LOG.info("Coverage took " + GenomixJobConf.tock("drawStatistics") + "ms");
    }

    public static void dumpGraph(JobConf conf, String inputGraph, String outputFasta, boolean followingBuild)
            throws IOException {
        LOG.info("Dumping graph to fasta...");
        GenomixJobConf.tick("dumpGraph");
        FileSystem dfs = FileSystem.get(conf);

        // stream in the graph, counting elements as you go... this would be better as a hadoop job which aggregated... maybe into counters?
        SequenceFile.Reader reader = null;
        VKmerBytesWritable key = null;
        NodeWritable value = null;
        BufferedWriter bw = null;
        FileStatus[] files = dfs.globStatus(new Path(inputGraph + File.separator + "*"));
        for (FileStatus f : files) {
            if (f.getLen() != 0) {
                try {
                    reader = new SequenceFile.Reader(dfs, f.getPath(), conf);
                    key = (VKmerBytesWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    value = (NodeWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
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

}
