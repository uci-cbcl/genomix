/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.genomix.hadoop.pmcommon;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3.MergePathsH3.MessageFlag;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

/*
 * A map-reduce job to find all nodes that are part of a simple path and the mark the nodes that
 * form their heads and tails.
 */
@SuppressWarnings("deprecation")
public class PathNodeInitial extends Configured implements Tool {

    public static class PathNodeInitialMapper extends MapReduceBase implements
            Mapper<NodeWritable, NullWritable, PositionWritable, MessageWritableNodeWithFlag> {

        private int KMER_SIZE;
        private PositionWritable outputKey;
        private MessageWritableNodeWithFlag outputValue;
        private int inDegree;
        private int outDegree;
        private NodeWritable emptyNode;
        private Iterator<PositionWritable> posIterator;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            outputKey = new PositionWritable();
            emptyNode = new NodeWritable();
        }

        @Override
        public void map(NodeWritable key, NullWritable value,
                OutputCollector<PositionWritable, MessageWritableNodeWithFlag> output, Reporter reporter)
                throws IOException {
            inDegree = key.inDegree();
            outDegree = key.outDegree();
            if (inDegree == 1 && outDegree == 1) {
                // simple path nodes map themselves
                outputValue.set(MessageFlag.FROM_SELF, key);
                output.collect(key.getNodeID(), outputValue);
                reporter.incrCounter("genomix", "path_nodes", 1);
            } else if (inDegree == 0 && outDegree == 1) {
                // start of a tip.  needs to merge & be marked as head
                outputValue.set(MessageFlag.FROM_SELF, key);
                output.collect(key.getNodeID(), outputValue);
                reporter.incrCounter("genomix", "path_nodes", 1);

                outputValue.set(MessageFlag.FROM_PREDECESSOR, emptyNode);
                output.collect(key.getNodeID(), outputValue);
            } else if (inDegree == 1 && outDegree == 0) {
                // end of a tip.  needs to merge & be marked as tail
                outputValue.set(MessageFlag.FROM_SELF, key);
                output.collect(key.getNodeID(), outputValue);
                reporter.incrCounter("genomix", "path_nodes", 1);

                outputValue.set(MessageFlag.FROM_SUCCESSOR, emptyNode);
                output.collect(key.getNodeID(), outputValue);
            } else {
                if (outDegree > 0) {
                    // Not a path myself, but my successor might be one. Map forward successor to find heads
                    outputValue.set(MessageFlag.FROM_PREDECESSOR, emptyNode);
                    posIterator = key.getFFList().iterator();
                    while (posIterator.hasNext()) {
                        outputKey.set(posIterator.next());
                        output.collect(outputKey, outputValue);
                    }
                    posIterator = key.getFRList().iterator();
                    while (posIterator.hasNext()) {
                        outputKey.set(posIterator.next());
                        output.collect(outputKey, outputValue);
                    }
                }
                if (inDegree > 0) {
                    // Not a path myself, but my predecessor might be one. map predecessor to find tails 
                    outputValue.set(MessageFlag.FROM_SUCCESSOR, emptyNode);
                    posIterator = key.getRRList().iterator();
                    while (posIterator.hasNext()) {
                        outputKey.set(posIterator.next());
                        output.collect(outputKey, outputValue);
                    }
                    posIterator = key.getRFList().iterator();
                    while (posIterator.hasNext()) {
                        outputKey.set(posIterator.next());
                        output.collect(outputKey, outputValue);
                    }
                }
                // push this non-path node to the "complete" output
                outputValue.set((byte) (MessageFlag.FROM_SELF | MessageFlag.IS_COMPLETE), key);
                output.collect(key.getNodeID(), outputValue);
            }
        }
    }

    public static class PathNodeInitialReducer extends MapReduceBase implements
            Reducer<PositionWritable, MessageWritableNodeWithFlag, PositionWritable, MessageWritableNodeWithFlag> {
        private MultipleOutputs mos;
        private static final String COMPLETE_OUTPUT = "complete";
        private int KMER_SIZE;
        private MessageWritableNodeWithFlag inputValue;
        private MessageWritableNodeWithFlag outputValue;
        private NodeWritable nodeToKeep;
        private int count;
        private byte flag;
        private boolean isComplete;

        public void configure(JobConf conf) {
            mos = new MultipleOutputs(conf);
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            outputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            nodeToKeep = new NodeWritable(KMER_SIZE);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void reduce(PositionWritable key, Iterator<MessageWritableNodeWithFlag> values,
                OutputCollector<PositionWritable, MessageWritableNodeWithFlag> output, Reporter reporter)
                throws IOException {

            inputValue.set(values.next());
            if (!values.hasNext()) {
                if ((inputValue.getFlag() & MessageFlag.FROM_SELF) > 0) {
                    if ((inputValue.getFlag() & MessageFlag.IS_COMPLETE) > 0) {
                        // non-path node.  Store in "complete" output
                        mos.getCollector(COMPLETE_OUTPUT, reporter).collect(key, inputValue);
                    } else {
                        // FROM_SELF => need to keep this PATH node
                        output.collect(key, inputValue);
                    }
                }
            } else {
                // multiple inputs => possible HEAD or TAIL to a path node. note if HEAD or TAIL node 
                count = 0;
                flag = MessageFlag.EMPTY_MESSAGE;
                isComplete = false;
                while (true) { // process values; break when no more
                    count++;
                    if ((inputValue.getFlag() & MessageFlag.FROM_SELF) > 0) {
                        // SELF -> keep this node
                        flag |= MessageFlag.FROM_SELF;
                        nodeToKeep.set(inputValue.getNode());
                        if ((inputValue.getFlag() & MessageFlag.IS_COMPLETE) > 0) {
                            isComplete = true;
                        }
                    } else if ((inputValue.getFlag() & MessageFlag.FROM_SUCCESSOR) > 0) {
                        flag |= MessageFlag.IS_TAIL;
                    } else if ((inputValue.getFlag() & MessageFlag.FROM_PREDECESSOR) > 0) {
                        flag |= MessageFlag.IS_HEAD;
                    }
                    if (!values.hasNext()) {
                        break;
                    } else {
                        inputValue.set(values.next());
                    }
                }
                if (count < 2) {
                    throw new IOException("Expected at least two nodes in PathNodeInitial reduce; saw "
                            + String.valueOf(count));
                }
                if ((flag & MessageFlag.FROM_SELF) > 0) {
                    if ((flag & MessageFlag.IS_COMPLETE) > 0) {
                        // non-path node.  Store in "complete" output
                        mos.getCollector(COMPLETE_OUTPUT, reporter).collect(key, inputValue);
                    } else {
                        // only keep simple path nodes
                        outputValue.set(flag, nodeToKeep);
                        output.collect(key, outputValue);

                        reporter.incrCounter("genomix", "path_nodes", 1);
                        if ((flag & MessageFlag.IS_HEAD) > 0) {
                            reporter.incrCounter("genomix", "path_nodes_heads", 1);
                        }
                        if ((flag & MessageFlag.IS_TAIL) > 0) {
                            reporter.incrCounter("genomix", "path_nodes_tails", 1);
                        }
                    }
                } else {
                    throw new IOException("No SELF node recieved in reduce! key=" + key.toString() + " flag=" + flag);
                }
            }
        }
    }

    /*
     * Mark the head, tail, and simple path nodes in one map-reduce job.
     */
    public RunningJob run(String inputPath, String outputPath, JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
        conf.setJarByClass(PathNodeInitial.class);
        conf.setJobName("PathNodeInitial " + inputPath);

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);

        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(MessageWritableNodeWithFlag.class);
        conf.setOutputKeyClass(PositionWritable.class);
        conf.setOutputValueClass(MessageWritableNodeWithFlag.class);

        conf.setMapperClass(PathNodeInitialMapper.class);
        conf.setReducerClass(PathNodeInitialReducer.class);

        FileSystem.get(conf).delete(new Path(outputPath), true);

        return JobClient.runJob(conf);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PathNodeInitial(), args);
        System.exit(res);
    }
}
