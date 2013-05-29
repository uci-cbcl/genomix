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

    }
   
    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, String defaultConfPath)
            throws IOException {

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
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        conf.setOutputKeyClass(KmerBytesWritable.class);
        conf.setOutputValueClass(PositionListWritable.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(inputPath + "-step1"));
        conf.setNumReduceTasks(numReducers);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(inputPath + "-step1"), true);
        JobClient.runJob(conf);
        
        //-------------
        conf = new JobConf(GraphBuildingDriver.class);
        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }
        conf.setJobName("deep build");
        
        conf.setMapperClass(DeepGraphBuildingMapper.class);
        conf.setReducerClass(DeepGraphBuildingReducer.class);
        
        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(PositionListAndKmerWritable.class);
        
        conf.setPartitionerClass(ReadIDPartitioner.class);
        
        conf.setOutputKeyComparatorClass(PositionWritable.Comparator.class);
        conf.setOutputValueGroupingComparator(PositionWritable.FirstComparator.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        conf.setOutputKeyClass(NodeWritable.class);
        conf.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath + "-step1"));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(1);
        dfs.delete(new Path(outputPath), true);
        JobClient.runJob(conf);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        GraphBuildingDriver driver = new GraphBuildingDriver();
        driver.run(options.inputPath, options.outputPath, options.numReducers, options.sizeKmer, null);
    }
}