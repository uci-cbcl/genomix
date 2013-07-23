package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.hadoop.graphbuilding.GenomixCombiner;
import edu.uci.ics.genomix.hadoop.graphbuilding.GenomixMapper;
import edu.uci.ics.genomix.hadoop.graphbuilding.GenomixReducer;
import edu.uci.ics.genomix.hadoop.oldtype.KmerBytesWritable;
import edu.uci.ics.genomix.hadoop.oldtype.KmerCountValue;


@SuppressWarnings("deprecation")
public class GenomixDriver {
    
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
    }
    
    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int readLength) throws IOException{
        JobConf conf = new JobConf(GenomixDriver.class);
        conf.setInt("sizeKmer", sizeKmer);
        conf.setInt("readLength", readLength);

        conf.setJobName("Genomix Graph Building");
        conf.setMapperClass(GenomixMapper.class);
        conf.setReducerClass(GenomixReducer.class);
        conf.setCombinerClass(GenomixCombiner.class);

        conf.setMapOutputKeyClass(KmerBytesWritable.class);
        conf.setMapOutputValueClass(KmerCountValue.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(KmerBytesWritable.class);
        conf.setOutputValueClass(KmerCountValue.class);
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
        GenomixDriver driver = new GenomixDriver();
        driver.run(options.inputPath, options.outputPath, options.numReducers, options.sizeKmer, options.readLength);
    }
}
