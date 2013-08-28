package edu.uci.ics.genomix.driver;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
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

public class DriverUtils {
    
    enum NCTypes {
        HYRACKS,
        PREGELIX
    }

    private static final Log LOG = LogFactory.getLog(DriverUtils.class);
    
    static void startNCs(NCTypes type) throws IOException {
        LOG.info("Starting NC's");
        String startNCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "startAllNCs.sh " + type;
        Process p = Runtime.getRuntime().exec(startNCCmd);
        try {
            p.waitFor(); // wait for ssh 
            Thread.sleep(3000); // wait for NC -> CC registration
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (p.exitValue() != 0)
            throw new RuntimeException("Failed to start the" + type + " NC's! Script returned exit code: "
                    + p.exitValue() + "\nstdout: " + IOUtils.toString(p.getInputStream()) + "\nstderr: "
                    + IOUtils.toString(p.getInputStream()));
    }

    static void shutdownNCs() throws IOException {
        LOG.info("Shutting down any previous NC's");
        String stopNCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "stopAllNCs.sh";
        Process p = Runtime.getRuntime().exec(stopNCCmd);
        try {
            p.waitFor(); // wait for ssh 
//            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static void startCC() throws IOException {
        LOG.info("Starting CC");
        String startCCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator
                + "startcc.sh";
        Process p = Runtime.getRuntime().exec(startCCCmd);
        try {
            p.waitFor(); // wait for cmd execution
            Thread.sleep(5000); // wait for CC registration
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (p.exitValue() != 0)
            throw new RuntimeException("Failed to start the genomix CC! Script returned exit code: " + p.exitValue()
                    + "\nstdout: " + IOUtils.toString(p.getInputStream()) + "\nstderr: "
                    + IOUtils.toString(p.getInputStream()));
    }

    static void shutdownCC() throws IOException {
        LOG.info("Shutting down CC");
        String stopCCCmd = System.getProperty("app.home", ".") + File.separator + "bin" + File.separator + "stopcc.sh";
        Process p = Runtime.getRuntime().exec(stopCCCmd);
        try {
            p.waitFor(); // wait for cmd execution
//            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        if (p.exitValue() != 0)
//            throw new RuntimeException("Failed to stop the genomix CC! Script returned exit code: " + p.exitValue()
//                    + "\nstdout: " + IOUtils.toString(p.getInputStream()) + "\nstderr: "
//                    + IOUtils.toString(p.getInputStream()));
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

    static void drawStatistics(JobConf conf, String inputStats, String outputChart) throws IOException {
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
    
    
    static void dumpGraph(JobConf conf, String inputGraph, String outputFasta, boolean followingBuild) throws IOException {
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
