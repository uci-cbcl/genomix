package edu.uci.ics.genomix.hadoop.converttofasta;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.kohsuke.args4j.Option;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hadoop.graph.GraphStatistics;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

@SuppressWarnings("deprecation")
public class ConvertToFasta extends MapReduceBase implements Mapper<VKmer, Node, Text, Text> {
    public static final Logger LOG = Logger.getLogger(ConvertToFasta.class.getName());
    private static class Options {
        @Option(name = "-inputpath1", usage = "the input path", required = false)
        public String inputPath1;

        @Option(name = "-outputpath", usage = "the output path", required = false)
        public String outputPath;

    }

    public static Text textKey = new Text();
    public static Text textValue = new Text();

    @Override
    public void map(VKmer key, Node value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        textKey.set(">node_" + key.toString() + "\t" + value.getInternalKmer().getKmerLetterLength() + "\t"
                + value.getAverageCoverage() + "\n");
        textValue.set(value.getInternalKmer().toString());
        output.collect(textKey, textValue);
    }

    public static void run(String outputPath, int numReducers, GenomixJobConf baseConf) throws IOException {

        GenomixJobConf conf = new GenomixJobConf(baseConf);

        conf.setJobName("convert to fasta!");
        conf.setMapperClass(ConvertToFasta.class);
        //        conf.setReducerClass(ResultsCheckingReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

//        FileInputFormat.setInputPaths(conf, new Path(inputPath));
//        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(numReducers);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(outputPath), true);
        JobClient.runJob(conf);
    }
}
