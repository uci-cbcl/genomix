package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class GraphBuildingDriver {

    private static class Options {
        @Option(name = "-inputpath", usage = "the input path", required = true)
        public String inputPath;

        @Option(name = "-outputpath", usage = "the output path", required = true)
        public String outputPath;

        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;

        @Option(name = "-kmer-size", usage = "the size of kmer", required = true)
        public int sizeKmer;

        @Option(name = "-read-length", usage = "the length of read", required = true)
        public int readLength;

        @Option(name = "-onlytest1stjob", usage = "test", required = true)
        public String onlyTest1stJob;

        @Option(name = "-seq-output", usage = "sequence ouput format", required = true)
        public String seqOutput;
    }

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int readLength,
            boolean onlyTest1stJob, boolean seqOutput, String defaultConfPath) throws IOException {
        if (onlyTest1stJob == true) {
            
            runfirstjob(inputPath, outputPath, numReducers, sizeKmer, readLength, seqOutput, defaultConfPath);
        } else {
            runfirstjob(inputPath, inputPath + "-tmp", numReducers, sizeKmer, readLength, true, defaultConfPath);
            runsecondjob(inputPath + "-tmp", outputPath, numReducers, sizeKmer, readLength, seqOutput, defaultConfPath);
        }
    }

    public void runfirstjob(String inputPath, String outputPath, int numReducers, int sizeKmer, int readLength, boolean seqOutput,
            String defaultConfPath) throws IOException {
        JobConf conf = new JobConf(GraphBuildingDriver.class);
        conf.setInt("sizeKmer", sizeKmer);
        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }

        conf.setJobName("graph building");
        conf.setMapperClass(GraphInvertedIndexBuildingMapper.class);
        conf.setReducerClass(GraphInvertedIndexBuildingReducer.class);

        conf.setMapOutputKeyClass(KmerBytesWritable.class);
        conf.setMapOutputValueClass(PositionWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        if (seqOutput == true)
            conf.setOutputFormat(SequenceFileOutputFormat.class);
        else
            conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(KmerBytesWritable.class);
        conf.setOutputValueClass(PositionListWritable.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        if (numReducers == 0)
            conf.setNumReduceTasks(numReducers + 2);
        else
            conf.setNumReduceTasks(numReducers);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(outputPath), true);
        JobClient.runJob(conf);
    }

    public void runsecondjob(String inputPath, String outputPath, int numReducers, int sizeKmer, int readLength,
            boolean seqOutput, String defaultConfPath) throws IOException {
        JobConf conf = new JobConf(GraphBuildingDriver.class);
        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }
        conf.setJobName("deep build");
        conf.setInt("sizeKmer", sizeKmer);
        conf.setInt("readLength", readLength);

        conf.setMapperClass(DeepGraphBuildingMapper.class);
        conf.setReducerClass(DeepGraphBuildingReducer.class);

        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(PositionListAndKmerWritable.class);

        conf.setPartitionerClass(ReadIDPartitioner.class);

        // grouping is done on the readID only; sorting is based on the (readID, abs(posn)) 
        conf.setOutputKeyComparatorClass(PositionWritable.Comparator.class);
        conf.setOutputValueGroupingComparator(PositionWritable.FirstComparator.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        if (seqOutput == true)
            conf.setOutputFormat(SequenceFileOutputFormat.class);
        else
            conf.setOutputFormat(TextOutputFormat.class);

        if (numReducers != 0) {
            conf.setOutputKeyClass(NodeWritable.class);
            conf.setOutputValueClass(NullWritable.class);
        } else {
            conf.setOutputKeyClass(PositionWritable.class);
            conf.setOutputValueClass(PositionListAndKmerWritable.class);
        }

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(numReducers);
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(outputPath), true);
        JobClient.runJob(conf);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        GraphBuildingDriver driver = new GraphBuildingDriver();
        boolean onlyTest1stJob = true;
        boolean seqOutput = true;
        if (options.onlyTest1stJob.equals("true"))
            onlyTest1stJob = true;
        else
            onlyTest1stJob = false;
        if (options.seqOutput.equals("true"))
            seqOutput = true;
        else
            seqOutput = false;
        driver.run(options.inputPath, options.outputPath, options.numReducers, options.sizeKmer, options.readLength,
                onlyTest1stJob, seqOutput, null);
    }
}