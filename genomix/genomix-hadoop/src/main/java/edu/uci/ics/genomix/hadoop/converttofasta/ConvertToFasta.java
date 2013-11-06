package edu.uci.ics.genomix.hadoop.converttofasta;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.VKmer;

@SuppressWarnings("deprecation")
public class ConvertToFasta extends MapReduceBase implements Mapper<VKmer, Node, Text, Text> {
    public static final Logger LOG = Logger.getLogger(ConvertToFasta.class.getName());

    public static Text textKey = new Text();
    public static Text textValue = new Text();

    @Override
    public void map(VKmer key, Node value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        textKey.set("node_" + key.toString() + "  length:" + value.getInternalKmer().getKmerLetterLength() + "  coverage:"
                + value.getAverageCoverage() + "x");
        textValue.set(value.getInternalKmer().toString());
        output.collect(textKey, textValue);
    }

    private static class FastaOutputFormat<K, V> extends FileOutputFormat<K, V> {
        
        private static final String OUTPUT_FILENAME = "genomix-scaffolds.fasta";

        protected static class FastaRecordWriter<K, V> implements RecordWriter<K, V> {
            private DataOutputStream out;

            public FastaRecordWriter(DataOutputStream out) throws IOException {
                this.out = out;
            }

            public synchronized void write(K key, V value) throws IOException {
                boolean nullKey = key == null || key instanceof NullWritable;
                boolean nullValue = value == null || value instanceof NullWritable;
                if (nullKey && nullValue) {
                    return;
                }
                out.writeBytes(">");
                if (!nullKey)
                    out.writeBytes(key.toString());
                else
                    out.writeBytes("null");
                out.writeBytes("\n");
                if (!nullValue)
                    out.writeBytes(value.toString());
                else
                    out.writeBytes("null");
                out.writeBytes("\n");
            }

            public synchronized void close(Reporter reporter) throws IOException {
                out.close();
            }
        }

        public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
                throws IOException {
            Path file = new Path(FileOutputFormat.getTaskOutputPath(job, name), OUTPUT_FILENAME);
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new FastaRecordWriter<K, V>(fileOut);
        }
    }

    public static void run(String inputPath, String outputPath, int numReducers, GenomixJobConf baseConf)
            throws IOException {

        GenomixJobConf conf = new GenomixJobConf(baseConf);

        conf.setJobName("convert to fasta!");
        conf.setMapperClass(ConvertToFasta.class);
        //        conf.setReducerClass(ResultsCheckingReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(FastaOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        conf.setNumReduceTasks(numReducers);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(new Path(outputPath), true);
        JobClient.runJob(conf);
    }
}
