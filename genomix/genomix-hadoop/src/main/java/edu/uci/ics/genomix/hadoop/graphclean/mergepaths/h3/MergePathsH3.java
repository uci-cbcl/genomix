package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3;

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

import edu.uci.ics.genomix.hadoop.pmcommon.MessageWritableNodeWithFlag;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial.PathNodeFlag;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class MergePathsH3 extends Configured implements Tool {
    /*
     * Flags used when sending messages
     */
    public static class MergeMessageFlag extends PathNodeFlag {
        public static final byte FROM_SUCCESSOR = 1 << 5;
        public static final byte FROM_PREDECESSOR = 1 << 6;
        public static final byte IS_PSEUDOHEAD = ((byte) 1 << 6); //TODO FIXME        
    }

    /*
     * Mapper class: Partition the graph using random pseudoheads.
     * Heads send themselves to their successors, and all others map themselves.
     */
    private static class MergePathsH3Mapper extends MapReduceBase implements
            Mapper<PositionWritable, MessageWritableNodeWithFlag, PositionWritable, MessageWritableNodeWithFlag> {
        private static long randSeed;
        private Random randGenerator;
        private float probBeingRandomHead;

        private int KMER_SIZE;
        private PositionWritable outputKey;
        private MessageWritableNodeWithFlag outputValue;
        private NodeWritable curNode;
        private byte headFlag;
        private byte outFlag;
        private boolean finalMerge;

        public void configure(JobConf conf) {
            randSeed = conf.getLong("randomSeed", 0);
            randGenerator = new Random(randSeed);
            probBeingRandomHead = conf.getFloat("probBeingRandomHead", 0.5f);
            finalMerge = conf.getBoolean("finalMerge", false);

            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            outputKey = new PositionWritable();
            curNode = new NodeWritable(KMER_SIZE);
        }

        protected boolean isNodeRandomHead(PositionWritable nodeID) {
            // "deterministically random", based on node id
            randGenerator.setSeed(randSeed ^ nodeID.hashCode());
            return randGenerator.nextFloat() < probBeingRandomHead;
        }

        @Override
        public void map(PositionWritable key, MessageWritableNodeWithFlag value,
                OutputCollector<PositionWritable, MessageWritableNodeWithFlag> output, Reporter reporter)
                throws IOException {
            curNode = value.getNode();
            // Map all path vertices; Heads and pseudoheads are sent to their successors
            // NOTE: all mapping nodes are already simple paths

            // Node may be marked as head b/c it's a real head, it's a previously merged head, or the node appears as a random head
            headFlag = (byte) (MergeMessageFlag.IS_HEAD & value.getFlag());
            // remove all pseudoheads on the last iteration
            if (!finalMerge) {
                headFlag |= (MergeMessageFlag.IS_PSEUDOHEAD & value.getFlag());
            }

            outFlag = (byte) (headFlag | (MergeMessageFlag.IS_TAIL & value.getFlag()));
            if (headFlag != 0 || isNodeRandomHead(curNode.getNodeID())) {
                // head nodes send themselves to their successor
                //outputKey.set(curNode.getOutgoingList().getPosition(0));
                if (!finalMerge) {
                    headFlag |= (MergeMessageFlag.IS_PSEUDOHEAD & value.getFlag());
                }
                outFlag |= MergeMessageFlag.FROM_PREDECESSOR;

                outputValue.set(outFlag, curNode);
                output.collect(outputKey, outputValue);
            } else {
                // tail nodes map themselves
                outFlag |= MergeMessageFlag.FROM_SELF;
                outputValue.set(outFlag, curNode);
                output.collect(key, outputValue);
            }
        }
    }

    /*
     * Reducer class: merge nodes that co-occur; for singletons, remap the original nodes 
     */
    private static class MergePathsH3Reducer extends MapReduceBase implements
            Reducer<PositionWritable, MessageWritableNodeWithFlag, PositionWritable, MessageWritableNodeWithFlag> {

        private int KMER_SIZE;
        private MessageWritableNodeWithFlag inputValue;
        private MessageWritableNodeWithFlag outputValue;
        private NodeWritable headNode;
        private NodeWritable tailNode;
        private int count;
        private byte outFlag;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            headNode = new NodeWritable(KMER_SIZE);
            tailNode = new NodeWritable(KMER_SIZE);
        }

        @Override
        public void reduce(PositionWritable key, Iterator<MessageWritableNodeWithFlag> values,
                OutputCollector<PositionWritable, MessageWritableNodeWithFlag> output, Reporter reporter)
                throws IOException {

            inputValue = values.next();
            if (!values.hasNext()) {
                // all single nodes must be remapped
                if ((inputValue.getFlag() & MergeMessageFlag.FROM_SELF) == MergeMessageFlag.FROM_SELF) {
                    // FROM_SELF => remap self
                    output.collect(key, inputValue);
                } else {
                    // FROM_PREDECESSOR => remap predecessor
                    output.collect(inputValue.getNode().getNodeID(), inputValue);
                }
            } else {
                // multiple inputs => a merge will take place. Aggregate both, then collect the merged path
                count = 0;
                outFlag = MergeMessageFlag.EMPTY_MESSAGE;
                while (true) { // process values; break when no more
                    count++;
                    outFlag |= (inputValue.getFlag() & (MergeMessageFlag.IS_HEAD | MergeMessageFlag.IS_PSEUDOHEAD | MergeMessageFlag.IS_TAIL));
                    if ((inputValue.getFlag() & MergeMessageFlag.FROM_PREDECESSOR) == MergeMessageFlag.FROM_PREDECESSOR) {
                        headNode.set(inputValue.getNode());
                    } else {
                        tailNode.set(inputValue.getNode());
                    }
                    if (!values.hasNext()) {
                        break;
                    } else {
                        inputValue = values.next();
                    }
                }
                if (count != 2) {
                    throw new IOException("Expected two nodes in MergePathsH3 reduce; saw " + String.valueOf(count));
                }
                // merge the head and tail as saved output, this merged node is now a head
                //headNode.mergeNext(tailNode, KMER_SIZE);
                outputValue.set(outFlag, headNode);

                if ((outFlag & MergeMessageFlag.IS_TAIL) == MergeMessageFlag.IS_TAIL) {
                    // Pseudoheads merging with tails don't become heads.
                    // Reset the IS_PSEUDOHEAD flag
                    outFlag &= ~MergeMessageFlag.IS_PSEUDOHEAD;

                    if ((outFlag & MergeMessageFlag.IS_HEAD) == MergeMessageFlag.IS_HEAD) {
                        // True heads meeting tails => merge is complete for this node
                        // TODO: send to the "complete" collector
                    }
                }
                reporter.incrCounter("genomix", "num_merged", 1);
                output.collect(key, outputValue);
            }
        }
    }

    /*
     * Run one iteration of the mergePaths algorithm
     */
    public RunningJob run(String inputPath, String outputPath, JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
        conf.setJarByClass(MergePathsH3.class);
        conf.setJobName("MergePathsH3 " + inputPath);

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(MessageWritableNodeWithFlag.class);
        conf.setOutputKeyClass(PositionWritable.class);
        conf.setOutputValueClass(MessageWritableNodeWithFlag.class);

        conf.setMapperClass(MergePathsH3Mapper.class);
        conf.setReducerClass(MergePathsH3Reducer.class);

        FileSystem.get(conf).delete(new Path(outputPath), true);

        return JobClient.runJob(conf);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MergePathsH3(), args);
        System.out.println("Ran the job fine!");
        System.exit(res);
    }
}
