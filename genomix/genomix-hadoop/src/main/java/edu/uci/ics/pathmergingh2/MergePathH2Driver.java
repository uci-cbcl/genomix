package edu.uci.ics.pathmergingh2;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;


@SuppressWarnings("deprecation")
public class MergePathH2Driver {
    
    private static class Options {
        @Option(name = "-inputpath", usage = "the input path", required = true)
        public String inputPath;

        @Option(name = "-outputpath", usage = "the output path", required = true)
        public String outputPath;

        @Option(name = "-mergeresultpath", usage = "the merging results path", required = true)
        public String mergeResultPath;
        
        @Option(name = "-num-reducers", usage = "the number of reducers", required = true)
        public int numReducers;

        @Option(name = "-kmer-size", usage = "the size of kmer", required = true)
        public int sizeKmer;
        
        @Option(name = "-merge-rounds", usage = "the while rounds of merging", required = true)
        public int mergeRound;

    }


    public void run(String inputPath, String outputPath, String mergeResultPath, int numReducers, int sizeKmer, int mergeRound, String defaultConfPath)
            throws IOException{

        JobConf conf = new JobConf(MergePathH2Driver.class);
        conf.setInt("sizeKmer", sizeKmer);
        
        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }
        conf.setJobName("Initial Path-Starting-Points Table");
        conf.setMapperClass(SNodeInitialMapper.class); 
        conf.setReducerClass(SNodeInitialReducer.class);

        conf.setMapOutputKeyClass(KmerBytesWritable.class);
        conf.setMapOutputValueClass(MergePathValueWritable.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        
        conf.setOutputKeyClass(VKmerBytesWritable.class);
        conf.setOutputValueClass(MergePathValueWritable.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(inputPath + "-step1"));
        conf.setNumReduceTasks(numReducers);
        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(inputPath + "-step1"), true);
        JobClient.runJob(conf);
        int iMerge = 0;
/*----------------------------------------------------------------------*/
        for(iMerge = 0; iMerge < mergeRound; iMerge ++){
            conf = new JobConf(MergePathH2Driver.class);
            conf.setInt("sizeKmer", sizeKmer);
            conf.setInt("iMerge", iMerge);
            
            if (defaultConfPath != null) {
                conf.addResource(new Path(defaultConfPath));
            }
            conf.setJobName("Path Merge");
            
            conf.setMapperClass(MergePathH2Mapper.class);
            conf.setReducerClass(MergePathH2Reducer.class);
            
            conf.setMapOutputKeyClass(VKmerBytesWritable.class);
            conf.setMapOutputValueClass(MergePathValueWritable.class);
            
            conf.setInputFormat(SequenceFileInputFormat.class);
            
            String uncomplete = "uncomplete" + iMerge;
            String complete = "complete" + iMerge;
           
            MultipleOutputs.addNamedOutput(conf, uncomplete,
                    MergePathMultiSeqOutputFormat.class, VKmerBytesWritable.class,
                    MergePathValueWritable.class);

            MultipleOutputs.addNamedOutput(conf, complete,
                    MergePathMultiTextOutputFormat.class, VKmerBytesWritable.class,
                    MergePathValueWritable.class);
            
            conf.setOutputKeyClass(VKmerBytesWritable.class);
            conf.setOutputValueClass(MergePathValueWritable.class);
            
            FileInputFormat.setInputPaths(conf, new Path(inputPath + "-step1"));
            FileOutputFormat.setOutputPath(conf, new Path(outputPath));
            conf.setNumReduceTasks(numReducers);
            dfs.delete(new Path(outputPath), true);
            JobClient.runJob(conf);
            dfs.delete(new Path(inputPath + "-step1"), true);
            dfs.rename(new Path(outputPath + "/" + uncomplete), new Path(inputPath + "-step1"));
            dfs.rename(new Path(outputPath + "/" + complete), new Path(mergeResultPath + "/" + complete));
        }
        conf = new JobConf(MergePathH2Driver.class);
        conf.setInt("sizeKmer", sizeKmer);
        conf.setInt("iMerge", iMerge);
        
        if (defaultConfPath != null) {
            conf.addResource(new Path(defaultConfPath));
        }
        conf.setJobName("Path Merge");
        
        conf.setMapperClass(MergePathH2Mapper.class);
        conf.setReducerClass(MergePathH2Reducer.class);
        
        conf.setMapOutputKeyClass(VKmerBytesWritable.class);
        conf.setMapOutputValueClass(MergePathValueWritable.class);
        
        conf.setInputFormat(SequenceFileInputFormat.class);
        
        String uncomplete = "uncomplete" + iMerge;
        String complete = "complete" + iMerge;
       
        MultipleOutputs.addNamedOutput(conf, uncomplete,
                MergePathMultiTextOutputFormat.class, VKmerBytesWritable.class,
                MergePathValueWritable.class);

        MultipleOutputs.addNamedOutput(conf, complete,
                MergePathMultiTextOutputFormat.class, VKmerBytesWritable.class,
                MergePathValueWritable.class);
        
        conf.setOutputKeyClass(VKmerBytesWritable.class);
        conf.setOutputValueClass(MergePathValueWritable.class);
        
        FileInputFormat.setInputPaths(conf, new Path(inputPath + "-step1"));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(numReducers);
        dfs.delete(new Path(outputPath), true);
        JobClient.runJob(conf);
        dfs.delete(new Path(inputPath + "-step1"), true);
        dfs.rename(new Path(outputPath + "/" + uncomplete), new Path(inputPath + "-step1"));
        dfs.rename(new Path(outputPath + "/" + complete), new Path(mergeResultPath + "/" + complete));
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
        MergePathH2Driver driver = new MergePathH2Driver();
        driver.run(options.inputPath, options.outputPath, options.mergeResultPath, options.numReducers, options.sizeKmer, options.mergeRound, null);
    }
}
