package edu.uci.ics.genomix.hadoop.graph;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.dfshealth_jsp;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/**
 * Generate graph statistics, storing them in the reporter's counters
 * 
 * @author wbiesing
 */
@SuppressWarnings("deprecation")
public class GraphStatistics extends MapReduceBase implements
        Mapper<VKmerBytesWritable, NodeWritable, Text, LongWritable> {

    @Override
    public void map(VKmerBytesWritable key, NodeWritable value, OutputCollector<Text, LongWritable> output,
            Reporter reporter) throws IOException {

        reporter.getCounter("totals", "nodes").increment(1);

        reporter.getCounter("degree-bins", Integer.toString(value.inDegree() + value.outDegree())).increment(1);
        reporter.getCounter("totals", "degree").increment(value.inDegree() + value.outDegree());

        reporter.getCounter("kmerLength-bins", Integer.toString(value.getKmerLength())).increment(1);
        reporter.getCounter("totals", "kmerLength").increment(value.getKmerLength());

        reporter.getCounter("coverage-bins", Integer.toString(Math.round(value.getAverageCoverage()))).increment(1);
        reporter.getCounter("totals", "coverage").increment(Math.round(value.getAverageCoverage()));

        reporter.getCounter("startRead-bins", Integer.toString(Math.round(value.getStartReads().getCountOfPosition())))
                .increment(1);
        reporter.getCounter("totals", "startRead").increment(Math.round(value.getStartReads().getCountOfPosition()));

        reporter.getCounter("endRead-bins", Integer.toString(Math.round(value.getEndReads().getCountOfPosition())))
                .increment(1);
        reporter.getCounter("totals", "endRead").increment(Math.round(value.getEndReads().getCountOfPosition()));

        long totalEdgeReads = 0;
        long totalSelf = 0;
        for (EDGETYPE et : EDGETYPE.values()) {
            for (EdgeWritable e : value.getEdgeList(et)) {
                totalEdgeReads += e.getReadIDs().getCountOfPosition();
                if (e.getKey().equals(key)) {
                    reporter.getCounter("totals", et + "-selfEdge").increment(1);
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
            if (value.getDegree(d) == 0)
                reporter.getCounter("totals", d + "-tips").increment(1);

        if (value.inDegree() == 0 && value.outDegree() == 0)
            reporter.getCounter("totals", "BOTH-tips").increment(1);
    }

    public static RunningJob run(String inputPath, String outputPath, GenomixJobConf baseConf) throws IOException {
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
        return job;
    }

    /**
     * run a map-reduce job on the given input graph and save a simple text file of the relevant counters
     */
    public static void saveGraphStats(String inputPath, String outputPath, GenomixJobConf conf) throws IOException {
        FileSystem dfs = FileSystem.get(conf);
        dfs.mkdirs(new Path(outputPath));
        PrintWriter writer = new PrintWriter(outputPath + File.separator + "stats.txt", "UTF-8");
        RunningJob finishedJob = run(inputPath, outputPath + ".tmpout", conf);
        for (Group g : finishedJob.getCounters()) {
            if (!g.getName().endsWith("-bins")) {  // skip "*-bins" (there are tons of these; they are for for graphing)
                for (Counter c : g) {
                    writer.println(g.getName() + "." + c.getName() + " = " + c.getCounter());
                }
            }
        }
        writer.close();
    }
}
