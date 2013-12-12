package edu.uci.ics.genomix.hadoop.utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapred.JobConf;
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

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;

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
        updateStats("kmerLength", value.getInternalKmer().getKmerLetterLength() == 0 ? key.getKmerLetterLength()
                : value.getKmerLength());
        updateStats("coverage", Math.round(value.getAverageCoverage()));
        updateStats("unflippedReadIds", value.getUnflippedReadIds().size());
        updateStats("flippedReadIds", value.getFlippedReadIds().size());

        //        long totalEdgeReads = 0;
        long totalSelf = 0;
        for (EDGETYPE et : EDGETYPE.values) {
            for (VKmer e : value.getEdges(et)) {
                //                totalEdgeReads += e.getValue().size();
                if (e.equals(key)) {
                    reporter.incrCounter("totals", "selfEdge-" + et, 1);
                    totalSelf += 1;
                }
            }
        }
        //        updateStats("edgeRead", totalEdgeReads);

        if (value.isPathNode())
            reporter.incrCounter("totals", "pathNode", 1);

        for (DIR d : DIR.values())
            if (value.degree(d) == 0)
                reporter.incrCounter("totals", "tips-" + d, 1);

        if (value.inDegree() == 0 && value.outDegree() == 0)
            reporter.incrCounter("totals", "tips-BOTH", 1);

        if ((value.inDegree() == 0 && value.outDegree() != 0) || (value.inDegree() != 0 && value.outDegree() == 0))
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
    public static Counters run(String inputPath, String outputPath, JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
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
     * run a map-reduce job on the given input graph and return an array of coverage
     */
    public static double getCoverageStats(Counters jobCounters, ArrayList<Double> result) {
        double maxCoverage = 0;
        // get Coverage counter
        for (Group g : jobCounters) {
            if (g.getName().equals("coverage-bins")) {
                for (Counter c : g) {
                    double cov = Double.parseDouble(c.getName());
                    if (maxCoverage < cov)
                        maxCoverage = cov;
                    for (long i = 0; i < c.getValue(); i++) {
                        result.add((double)cov);
                    }
                }
            }
        }
        return maxCoverage;
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

        FileSystem fileSys = FileSystem.get(conf);
        fileSys.mkdirs(new Path(outputDir));
        FSDataOutputStream outstream = fileSys.create(new Path(outputDir + File.separator + "stats.txt"), true);
        PrintWriter writer = new PrintWriter(outstream);
        for (Entry<String, Long> e : sortedCounters.entrySet()) {
            writer.println(e.getKey() + " = " + e.getValue());
        }
        writer.close();
    }
    
    public static void drawCoverageStatistics(String outputDir, Counters jobCounters, GenomixJobConf conf) throws IOException {
        HashMap<String, TreeMap<Integer, Long>> allHists = new HashMap<String, TreeMap<Integer, Long>>();
        TreeMap<Integer, Long> curCounts;

        // build up allHists to be {coverage : {1: 50, 2: 20, 3:5}, kmerLength : {55: 100}, ...}
        for (Group g : jobCounters) {
            if (g.getName().equals("coverage-bins")) {
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
            FileSystem fileSys = FileSystem.get(conf);
            FSDataOutputStream outstream = fileSys.create(
                    new Path(outputDir + File.separator + graphType + "-hist.png"), true);
            ChartUtilities.writeChartAsPNG(outstream, chart, 800, 600);
            outstream.close();
        }
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
            FileSystem fileSys = FileSystem.get(conf);
            FSDataOutputStream outstream = fileSys.create(
                    new Path(outputDir + File.separator + graphType + "-hist.png"), true);
            ChartUtilities.writeChartAsPNG(outstream, chart, 800, 600);
            outstream.close();
        }
    }

    public static HashMap<Integer, Long> loadDataFromCounters(HashMap<Integer, Long> map, Counters jobCounters) {
        for (Group g : jobCounters) {
            if (g.getName().equals("kmerLength-bins")) {
                for (Counter c : g) {
                    Integer X = Integer.parseInt(c.getName());
                    if (map.get(X) != null) {
                        map.put(X, map.get(X) + c.getCounter());
                    } else {
                        map.put(X, c.getCounter());
                    }
                }
            }
        }
        return map;
    }

    private static ArrayList<Integer> contigLengthList = new ArrayList<Integer>();
    //    static boolean OLD_STYLE = true;
    private static int MIN_CONTIG_LENGTH;
    private static int EXPECTED_GENOME_SIZE;
    private static int maxContig = Integer.MIN_VALUE;
    private static int minContig = Integer.MAX_VALUE;
    private static long total = 0;
    private static int count = 0;
    private static int totalOverLength = 0;
    private static long totalBPOverLength = 0;
    private static double eSize;

    private static final int CONTIG_AT_INITIAL_STEP = 1000000;
    private static final NumberFormat nf = new DecimalFormat("############.##");

    private static class ContigAt {
        ContigAt(long currentBases) {
            this.count = this.len = 0;
            this.totalBP = 0;
            this.goal = currentBases;
        }

        public int count;
        public long totalBP;
        public int len;
        public long goal;
    }

    private static class NXValueAndCountPair {
        private int nxValue;
        private int nxCount;

        public NXValueAndCountPair() {
            nxValue = 0;
            nxCount = 0;
        }
    }

    private static void initializeNxMap(HashMap<Double, NXValueAndCountPair> nxMap) {
        double[] nxThresholds = { 0.1, 0.25, 0.5, 0.75, 0.95 };
        for (int i = 0; i < 5; i++) {
            nxMap.put(nxThresholds[i], new NXValueAndCountPair());
        }
    }

    public static void getFastaStatsForGage(String outputDir, Counters jobCounters, JobConf job) throws IOException {
        HashMap<Integer, Long> ctgSizeCounts = new HashMap<Integer, Long>();
        ctgSizeCounts = loadDataFromCounters(ctgSizeCounts, jobCounters);
        HashMap<Double, NXValueAndCountPair> nxMap = new HashMap<Double, NXValueAndCountPair>();
        initializeNxMap(nxMap);

        MIN_CONTIG_LENGTH = Integer.parseInt(job.get(GenomixJobConf.STATS_MIN_CONTIGLENGTH));
        EXPECTED_GENOME_SIZE = Integer.parseInt(job.get(GenomixJobConf.STATS_EXPECTED_GENOMESIZE));

        for (Integer curLength : ctgSizeCounts.keySet()) {
            for (int i = 0; i < ctgSizeCounts.get(curLength); i++) {
                contigLengthList.add(curLength);
                if (curLength <= MIN_CONTIG_LENGTH) {
                    continue;
                }
                if (curLength > maxContig) {
                    maxContig = curLength;
                }
                if (curLength < minContig) {
                    minContig = curLength;
                }
                if (EXPECTED_GENOME_SIZE == 0) {
                    total += curLength;
                } else {
                    total = EXPECTED_GENOME_SIZE;
                }
                count++;
                eSize += Math.pow(curLength, 2);
                totalOverLength++;
                totalBPOverLength += curLength;
            }
        }
        eSize /= EXPECTED_GENOME_SIZE;

        /*----------------------------------------------------------------*/
        // get the goal contig at X bases (1MBp, 2MBp)
        ArrayList<ContigAt> contigAtArray = new ArrayList<ContigAt>();
        long step = CONTIG_AT_INITIAL_STEP;
        long currentBases = 0;
        while (currentBases <= total) {
            if ((currentBases / step) >= 10) {
                step *= 10;
            }
            currentBases += step;
            contigAtArray.add(new ContigAt(currentBases));//
        }
        ContigAt[] contigAtVals = contigAtArray.toArray(new ContigAt[0]);
        /*----------------------------------------------------------------*/
        Collections.sort(contigLengthList);
        long sum = 0;
        double median = contigLengthList.get(contigLengthList.size() / 2);
        int numberContigsSeen = 1;
        int currentValPoint = 0;

        for (int i = contigLengthList.size() - 1; i >= 0; i--) {
            sum += contigLengthList.get(i);
            // calculate the bases at
            /*----------------------------------------------------------------*/
            while (currentValPoint < contigAtVals.length && sum >= contigAtVals[currentValPoint].goal
                    && contigAtVals[currentValPoint].count == 0) {
                System.err.println("Calculating point at " + currentValPoint + " and the sum is " + sum + " and i is"
                        + i + " and lens is " + contigLengthList.size() + " and length is " + contigLengthList.get(i));
                contigAtVals[currentValPoint].count = numberContigsSeen;
                contigAtVals[currentValPoint].len = contigLengthList.get(i);
                contigAtVals[currentValPoint].totalBP = sum;
                currentValPoint++;
            }
            /*----------------------------------------------------------------*/
            for (Map.Entry<Double, NXValueAndCountPair> entry : nxMap.entrySet()) {
                if (sum / (double) total >= entry.getKey() && entry.getValue().nxCount == 0) {
                    entry.getValue().nxValue = contigLengthList.get(i);
                    entry.getValue().nxCount = contigLengthList.size() - i;
                }
            }
            numberContigsSeen++;
        }

        StringBuilder outputStr = new StringBuilder();
        outputStr.append("Total units: " + count + "\n");
        outputStr.append("Reference: " + total + "\n");
        outputStr.append("BasesInFasta: " + totalBPOverLength + "\n");
        outputStr.append("Min: " + minContig + "\n");
        outputStr.append("Max: " + maxContig + "\n");
        outputStr.append("N10: " + nxMap.get(Double.valueOf(0.1)).nxValue + " COUNT: "
                + nxMap.get(Double.valueOf(0.1)).nxCount + "\n");
        outputStr.append("N25: " + nxMap.get(Double.valueOf(0.25)).nxValue + " COUNT: "
                + nxMap.get(Double.valueOf(0.25)).nxCount + "\n");
        outputStr.append("N50: " + nxMap.get(Double.valueOf(0.5)).nxValue + " COUNT: "
                + nxMap.get(Double.valueOf(0.5)).nxCount + "\n");
        outputStr.append("N75: " + nxMap.get(Double.valueOf(0.75)).nxValue + " COUNT: "
                + nxMap.get(Double.valueOf(0.75)).nxCount + "\n");
        outputStr.append("E-size:" + nf.format(eSize) + "\n");

        FileSystem dfs = FileSystem.getLocal(new Configuration());
        dfs.mkdirs(new Path(outputDir));
        FSDataOutputStream outstream = dfs.create(new Path("actual" + File.separator + outputDir
                + "/gagestatsFasta.txt"), true);
        PrintWriter writer = new PrintWriter(outstream);
        writer.println(outputStr.toString());
        writer.close();

    }

    public static void saveJobCounters(String outputDir, PregelixJob lastJob, GenomixJobConf conf) throws IOException {
        org.apache.hadoop.mapreduce.Counters newC;
        try {
            newC = BspUtils.getCounters(lastJob);
        } catch (HyracksDataException e) {
            e.printStackTrace();
            LOG.info("No counters available for job" + lastJob);
            return;
        }
        FileSystem dfs = FileSystem.get(conf);
        dfs.mkdirs(new Path(outputDir));
        org.apache.hadoop.mapred.Counters oldC = new org.apache.hadoop.mapred.Counters();
        for (String g : newC.getGroupNames()) {
            for (org.apache.hadoop.mapreduce.Counter c : newC.getGroup(g)) {
                oldC.findCounter(g, c.getName()).increment(c.getValue());
            }
        }
        GraphStatistics.saveGraphStats(outputDir + File.separator + "counters", oldC, conf);
        GraphStatistics.drawStatistics(outputDir + File.separator + "counters", oldC, conf);
    }
}
