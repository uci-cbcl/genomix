package edu.uci.ics.genomix.hadoop.graphclean.removetips;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3.MergePathsH3.MergeMessageFlag;
import edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h4.MergePathsH4;
import edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h4.MergePathsH4.MergePathsH4Mapper;
import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class RemoveTips extends Configured implements Tool {

    /*
     * Mapper class: removes any tips by not mapping them at all
     */
    private static class RemoveTipsMapper extends MapReduceBase implements
            Mapper<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {
        private int KMER_SIZE;
        private int removeTipsMinLength;

        private NodeWithFlagWritable outputValue;
        private NodeWritable curNode;

        public void configure(JobConf conf) {
            removeTipsMinLength = conf.getInt("removeTipsMinLength", 0);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            curNode = new NodeWritable(KMER_SIZE);
        }

        @Override
        public void map(PositionWritable key, NodeWithFlagWritable value,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter)
                throws IOException {
            curNode.set(value.getNode());
            if ((curNode.inDegree() == 0 || curNode.outDegree() == 0)
                    && curNode.getKmer().getKmerLength() < removeTipsMinLength) {
                // kill this node by NOT mapping it.  Update my neighbors with a suicide note
                //TODO: update neighbors by removing me from its list
            } else {
                outputValue.set(MergeMessageFlag.MSG_SELF, curNode);
                output.collect(key, value);
            }
        }
    }

    /*
     * Reducer class: keeps mapped nodes 
     */
    private static class MergePathsH4Reducer extends MapReduceBase implements
            Reducer<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {

        private int KMER_SIZE;
        private NodeWithFlagWritable inputValue;
        private NodeWithFlagWritable outputValue;
        private NodeWritable curNode;
        private NodeWritable prevNode;
        private NodeWritable nextNode;
        private boolean sawCurNode;
        private boolean sawPrevNode;
        private boolean sawNextNode;
        private int count;
        private byte outFlag;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            curNode = new NodeWritable(KMER_SIZE);
            prevNode = new NodeWritable(KMER_SIZE);
            nextNode = new NodeWritable(KMER_SIZE);
        }

        @Override
        public void reduce(PositionWritable key, Iterator<NodeWithFlagWritable> values,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter)
                throws IOException {

            inputValue.set(values.next());
            if (!values.hasNext()) {
                if ((inputValue.getFlag() & MergeMessageFlag.MSG_SELF) > 0) {
                    // FROM_SELF => keep self
                    output.collect(key, inputValue);
                } else {
                    throw new IOException("Only one value recieved in merge, but it wasn't from self!");
                }
            } else {
                throw new IOException("Expected only one node during reduce... saw more");
            }
        }
    }

    /*
     * Run one iteration of the mergePaths algorithm
     */
    public RunningJob run(String inputPath, String outputPath, JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
        conf.setJarByClass(MergePathsH4.class);
        conf.setJobName("MergePathsH4 " + inputPath);

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(NodeWithFlagWritable.class);
        conf.setOutputKeyClass(PositionWritable.class);
        conf.setOutputValueClass(NodeWithFlagWritable.class);

        conf.setMapperClass(MergePathsH4Mapper.class);
        conf.setReducerClass(MergePathsH4Reducer.class);

        FileSystem.get(conf).delete(new Path(outputPath), true);

        return JobClient.runJob(conf);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MergePathsH4(), args);
        System.out.println("Ran the job fine!");
        System.exit(res);
    }
}