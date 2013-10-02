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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

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

        @Option(name = "-num-lines-per-map", usage = "the number of lines per map", required = false)
        public int linesPerMap;
    }

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int linesPerMap,
            boolean seqOutput, String defaultConfPath) throws IOException {
        JobConf conf = new JobConf(GenomixDriver.class);
        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }
        run(inputPath, outputPath, numReducers, sizeKmer, linesPerMap, seqOutput, conf);
    }

    public void run(String inputPath, String outputPath, int numReducers, int sizeKmer, int linesPerMap,
            boolean seqOutput, JobConf conf) throws IOException {
        conf.setInt(GenomixJobConf.KMER_LENGTH, sizeKmer);
        conf.setJobName("Genomix Graph Building");
        conf.setMapperClass(GenomixMapper.class);
        conf.setReducerClass(GenomixReducer.class);

        conf.setMapOutputKeyClass(VKmer.class);
        conf.setMapOutputValueClass(Node.class);

        //InputFormat and OutputFormat for Reducer
        //        conf.setInputFormat(NLineInputFormat.class);
        //        conf.setInt("mapred.line.input.format.linespermap", linesPerMap);
        //        conf.setInt("io.sort.mb", 150);
        conf.setInputFormat(TextInputFormat.class);
        if (seqOutput == true)
            conf.setOutputFormat(SequenceFileOutputFormat.class);
        else
            conf.setOutputFormat(TextOutputFormat.class);

        //Output Key/Value Class
        conf.setOutputKeyClass(VKmer.class);
        conf.setOutputValueClass(Node.class);

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
        driver.run(options.inputPath, options.outputPath, options.numReducers, options.sizeKmer, options.linesPerMap,
                true, new JobConf());
    }
}
