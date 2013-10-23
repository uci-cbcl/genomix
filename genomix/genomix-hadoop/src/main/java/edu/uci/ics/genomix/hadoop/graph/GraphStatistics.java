package edu.uci.ics.genomix.hadoop.graph;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;

/**
 * Generate graph statistics, storing them in the reporter's counters
 * 
 * @author wbiesing
 */
@SuppressWarnings("deprecation")
public class GraphStatistics extends MapReduceBase implements Mapper<VKmer, Node, Text, LongWritable> {

    public static final Logger LOG = Logger.getLogger(GraphStatistics.class.getName());
    private Reporter reporter;

    @Override
    public void map(VKmer key, Node value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {
        this.reporter = reporter;
        
        reporter.incrCounter("totals", "nodes", 1);
        updateStats("degree", value.inDegree() + value.outDegree());
        updateStats("kmerLength", value.getKmerLength() == 0 ? key.getKmerLetterLength() : value.getKmerLength());
        updateStats("coverage", Math.round(value.getAverageCoverage()));
        updateStats("startRead", value.getStartReads().size());
        updateStats("endRead", value.getEndReads().size());
        

        long totalEdgeReads = 0;
        long totalSelf = 0;
        for (EDGETYPE et : EDGETYPE.values()) {
            for (Entry<VKmer, ReadIdSet> e : value.getEdgeMap(et).entrySet()) {
                totalEdgeReads += e.getValue().size();
                if (e.getKey().equals(key)) {
                    reporter.incrCounter("totals", "selfEdge-" + et, 1);
                    totalSelf += 1;
                }
            }
        }
        updateStats("edgeRead", totalEdgeReads);
        
        if (value.isPathNode())
            reporter.incrCounter("totals", "pathNode", 1);

        for (DIR d : DIR.values())
            if (value.degree(d) == 0)
                reporter.incrCounter("totals", "tips-" + d, 1);

        if (value.inDegree() == 0 && value.outDegree() == 0)
            reporter.incrCounter("totals", "tips-BOTH", 1);
        
        if ((value.inDegree() == 0 && value.outDegree() != 0)
                || (value.inDegree() != 0 && value.outDegree() == 0))
            reporter.incrCounter("totals", "tips-ONE", 1);
    }
    
    private void updateStats(String valueName, long value) {
        reporter.incrCounter(valueName + "-bins", Long.toString(value), 1);
        reporter.incrCounter("totals", valueName, value);
        
        long prevMax = reporter.getCounter("maximum", valueName).getValue();
        if (prevMax < value) {
            reporter.incrCounter("maximum", valueName, value - prevMax); // increment by difference to get to new value (no set function!)
        }
    }

    /**
     * Run a map-reduce aggregator to get statistics on the graph (stored in returned job's counters)
     */
    public static Counters run(String inputPath, String outputPath, GenomixJobConf baseConf) throws IOException {
        GenomixJobConf conf = new GenomixJobConf(baseConf);
        conf.setJobName("Graph Statistics");
        conf.setMapperClass(GraphStatistics.class);
        conf.setNumReduceTasks(0); // no reducer

        conf.setInputFormat(SequenceFileInputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(outputPath), true);
        RunningJob job = JobClient.runJob(conf);
        dfs.delete(new Path(outputPath), true); // we have NO output (just counters)

        return job.getCounters();
    }

    /**
     * run a map-reduce job on the given input graph and save a simple text file of the relevant counters
     */
    public static void saveGraphStats(String outputDir, Counters jobCounters, GenomixJobConf conf) throws IOException {
        // get relevant counters

        TreeMap<String, Long> sortedCounters = new TreeMap<String, Long>();
        for (Group g : jobCounters) {
            if (!g.getName().endsWith("-bins")) {
                for (Counter c : g) {
                    sortedCounters.put(g.getName() + "." + c.getName(), c.getCounter());
                }
            }
        }

        FileSystem dfs = FileSystem.get(conf);
        dfs.mkdirs(new Path(outputDir));
        FSDataOutputStream outstream = dfs.create(new Path(outputDir + File.separator + "stats.txt"), true);
        PrintWriter writer = new PrintWriter(outstream);
        for (Entry<String, Long> e : sortedCounters.entrySet()) {
            writer.println(e.getKey() + " = " + e.getValue());
        }
        writer.close();
    }

    /**
     * generate a histogram from the *-bins values
     * for example, the coverage counters have the group "coverage-bins", the counter name "5" and the count 10
     * meaning the coverage chart has a bar at X=5 with height Y=10
     */
    public static void drawStatistics(String outputDir, Counters jobCounters, GenomixJobConf conf) throws IOException {
        HashMap<String, TreeMap<Integer, Long>> allHists = new HashMap<String, TreeMap<Integer, Long>>();
        TreeMap<Integer, Long> curCounts;

        // build up allHists to be {coverage : {1: 50, 2: 20, 3:5}, kmerLength : {55: 100}, ...}
        for (Group g : jobCounters) {
            if (g.getName().endsWith("-bins")) {
                String baseName = g.getName().replace("-bins", "");
                if (allHists.containsKey(baseName)) {
                    curCounts = allHists.get(baseName);
                } else {
                    curCounts = new TreeMap<Integer, Long>();
                    allHists.put(baseName, curCounts);
                }
                for (Counter c : g) { // counter name is the X value of the histogram; its count is the Y value
                    Integer X = Integer.parseInt(c.getName());
                    if (curCounts.get(X) != null) {
                        curCounts.put(X, curCounts.get(X) + c.getCounter());
                    } else {
                        curCounts.put(X, c.getCounter());
                    }
                }
            }
        }

        for (String graphType : allHists.keySet()) {
            curCounts = allHists.get(graphType);
            XYSeries series = new XYSeries(graphType);
            for (Entry<Integer, Long> pair : curCounts.entrySet()) {
                series.add(pair.getKey().floatValue(), pair.getValue().longValue());
            }
            XYSeriesCollection xyDataset = new XYSeriesCollection(series);
            JFreeChart chart = ChartFactory.createXYBarChart(graphType, graphType, false, "Count", xyDataset,
                    PlotOrientation.VERTICAL, true, true, false);
            // Write the data to the output stream:
            FileSystem dfs = FileSystem.get(conf);
            FSDataOutputStream outstream = dfs.create(new Path(outputDir + File.separator + graphType + "-hist.png"),
                    true);
            ChartUtilities.writeChartAsPNG(outstream, chart, 800, 600);
            outstream.close();
        }
    }
}
