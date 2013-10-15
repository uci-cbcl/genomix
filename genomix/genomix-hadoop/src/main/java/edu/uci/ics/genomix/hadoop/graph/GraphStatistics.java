package edu.uci.ics.genomix.hadoop.graph;

import java.io.File;
import java.io.FileOutputStream;
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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
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

    @Override
    public void map(VKmer key, Node value, OutputCollector<Text, LongWritable> output, Reporter reporter)
            throws IOException {

        reporter.getCounter("totals", "nodes").increment(1);

        reporter.getCounter("degree-bins", Integer.toString(value.inDegree() + value.outDegree())).increment(1);
        reporter.getCounter("totals", "degree").increment(value.inDegree() + value.outDegree());

        int kmerLength = value.getKmerLength() == 0 ? key.getKmerLetterLength() : value.getKmerLength();
        reporter.getCounter("kmerLength-bins", Integer.toString(kmerLength)).increment(1);
        reporter.getCounter("totals", "kmerLength").increment(kmerLength);

        reporter.getCounter("coverage-bins", Integer.toString(Math.round(value.getAverageCoverage()))).increment(1);
        reporter.getCounter("totals", "coverage").increment(Math.round(value.getAverageCoverage()));

        reporter.getCounter("startRead-bins", Integer.toString(Math.round(value.getStartReads().size()))).increment(1);
        reporter.getCounter("totals", "startRead").increment(Math.round(value.getStartReads().size()));

        reporter.getCounter("endRead-bins", Integer.toString(Math.round(value.getEndReads().size()))).increment(1);
        reporter.getCounter("totals", "endRead").increment(Math.round(value.getEndReads().size()));

        long totalEdgeReads = 0;
        long totalSelf = 0;
        for (EDGETYPE et : EDGETYPE.values()) {
            for (Entry<VKmer, ReadIdSet> e : value.getEdgeMap(et).entrySet()) {
                totalEdgeReads += e.getValue().size();
                if (e.getKey().equals(key)) {
                    reporter.getCounter("totals", "selfEdge-" + et).increment(1);
                    totalSelf += 1;
                }
            }
        }
        reporter.getCounter("edgeRead-bins", Long.toString(totalEdgeReads)).increment(1);
        reporter.getCounter("totals", "edgeRead").increment(totalEdgeReads);
        reporter.getCounter("selfEdge-bins", Long.toString(totalSelf)).increment(1);

        if (value.isPathNode())
            reporter.getCounter("totals", "pathNode").increment(1);

        for (DIR d : DIR.values())
            if (value.degree(d) == 0)
                reporter.getCounter("totals", "tips-" + d).increment(1);

        if (value.inDegree() == 0 && value.outDegree() == 0)
            reporter.getCounter("totals", "tips-BOTH").increment(1);
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
    public static void drawStatistics(String outputDir, Counters jobCounters) throws IOException {
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
            FileOutputStream chartOut = new FileOutputStream(new File(outputDir + File.separator + graphType
                    + "-hist.png"));
            ChartUtilities.writeChartAsPNG(chartOut, chart, 800, 600);
            chartOut.close();
        }
    }

    public static void loadDataFromCounters(HashMap<Integer, Long> map, Counters jobCounters) {
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
    }

    private static ArrayList<Integer> contigLengthList = new ArrayList<Integer>();
    static boolean OLD_STYLE = true;
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
    private static double[] nxThresholds = { 0.1, 0.25, 0.5, 0.75, 0.95 };
    private static int[] nxValueAndCount = new int[10];

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


    public static void getFastaStatsForGage(String outputDir, Counters jobCounters, JobConf job) throws IOException {
        HashMap<Integer, Long> ctgSizeCounts = new HashMap<Integer, Long>();
        loadDataFromCounters(ctgSizeCounts, jobCounters);
        MIN_CONTIG_LENGTH = Integer.parseInt(job.get(GenomixJobConf.STATS_MIN_CONTIGLENGTH));
        EXPECTED_GENOME_SIZE = Integer.parseInt(job.get(GenomixJobConf.STATS_EXPECTED_GENOMESIZE));
        OLD_STYLE = Boolean.parseBoolean(job.get(GenomixJobConf.STATS_GAGE_OLDSTYLE));
        for (Integer s : ctgSizeCounts.keySet()) {
            for (int i = 0; i < ctgSizeCounts.get(s); i++)
                contigLengthList.add(s);
        }
        for (Integer curLength : contigLengthList) {
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
        double median = 0;
        int medianCount = 0;
        int numberContigsSeen = 1;
        int currentValPoint = 0;
        for (int i = contigLengthList.size() - 1; i >= 0; i--) {
            if (((int) (count / 2)) == i) {
                median += contigLengthList.get(i);
                medianCount++;
            } else if (count % 2 == 0 && ((((int) (count / 2)) + 1) == i)) {
                median += contigLengthList.get(i);
                medianCount++;
            }

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
            for (int j = 0; j < 5; j++) {
                if (sum / (double) total >= nxThresholds[j] && nxValueAndCount[j + 5] == 0) {
                    nxValueAndCount[j] = contigLengthList.get(i);
                    nxValueAndCount[j + 5] = contigLengthList.size() - i;
                }
            }
            numberContigsSeen++;
        }

        StringBuffer outputStr = new StringBuffer();
        if (OLD_STYLE == true) {
            outputStr.append("Total units: " + count + "\n");
            outputStr.append("Reference: " + total + "\n");
            outputStr.append("BasesInFasta: " + totalBPOverLength + "\n");
            outputStr.append("Min: " + minContig + "\n");
            outputStr.append("Max: " + maxContig + "\n");
            outputStr.append("N10: " + nxValueAndCount[0] + " COUNT: " + nxValueAndCount[0 + 5] + "\n");
            outputStr.append("N25: " + nxValueAndCount[1] + " COUNT: " + nxValueAndCount[1 + 5] + "\n");
            outputStr.append("N50: " + nxValueAndCount[2] + " COUNT: " + nxValueAndCount[2 + 5] + "\n");
            outputStr.append("N75: " + nxValueAndCount[3] + " COUNT: " + nxValueAndCount[3 + 5] + "\n");
            outputStr.append("E-size:" + nf.format(eSize) + "\n");
        } else {
            //                if (outputHeader) {
            outputStr.append("Assembly");
            outputStr.append(",Unit Number");
            outputStr.append(",Unit Total BP");
            outputStr.append(",Number Units > " + MIN_CONTIG_LENGTH);
            outputStr.append(",Total BP in Units > " + MIN_CONTIG_LENGTH);
            outputStr.append(",Min");
            outputStr.append(",Max");
            outputStr.append(",Average");
            outputStr.append(",Median");

            for (int i = 0; i < contigAtVals.length; i++) {
                if (contigAtVals[i].count != 0) {
                    outputStr.append(",Unit At " + nf.format(contigAtVals[i].goal) + " Unit Count,"
                            + /*"Total Length," + */" Actual Unit Length");
                }
            }
            outputStr.append("\n");
            //            }
            //            outputStr.append(title);
            outputStr.append("," + nf.format(count));
            outputStr.append("," + nf.format(total));
            outputStr.append("," + nf.format(totalOverLength));
            outputStr.append("," + nf.format(totalBPOverLength));
            outputStr.append("," + nf.format(minContig));
            outputStr.append("," + nf.format(maxContig));
            outputStr.append("," + nf.format((double) total / count));
            outputStr.append("," + nf.format((double) median / medianCount));

            for (int i = 0; i < contigAtVals.length; i++) {
                if (contigAtVals[i].count != 0) {
                    outputStr.append("," + contigAtVals[i].count + ","
                            + /*contigAtVals[i].totalBP + "," +*/contigAtVals[i].len);
                }
            }
        }
        FileSystem dfs = FileSystem.get(job);
        dfs.mkdirs(new Path(outputDir));
        FSDataOutputStream outstream = dfs.create(new Path(outputDir + File.separator + "gagestatsFasta.txt"), true);
        PrintWriter writer = new PrintWriter(outstream);
        writer.println(outputStr.toString());
        writer.close();

    }
}
