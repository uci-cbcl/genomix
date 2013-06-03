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
        
        public PathNodeInitialMapper() {
            
        }

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
            } else if (outDegree == 1) {
                // Not a path myself, but my successor might be one. Map forward successor
                outputValue.set(MessageFlag.FROM_PREDECESSOR, emptyNode);
                if (key.getFFList().getCountOfPosition() > 0) {
                    outputKey.set(key.getFFList().getPosition(0));
                } else {
                    outputKey.set(key.getFRList().getPosition(0));
                }
                output.collect(outputKey, outputValue);
            } else if (inDegree == 1) {
                // Not a path myself, but my predecessor might be one.
                outputValue.set(MessageFlag.FROM_SUCCESSOR, emptyNode);
                if (key.getRRList().getCountOfPosition() > 0) {
                    outputKey.set(key.getRRList().getPosition(0));
                } else {
                    outputKey.set(key.getRFList().getPosition(0));
                }
                output.collect(outputKey, outputValue);
            }
            else {
                // TODO: all other nodes will not participate-- should they be collected in a "complete" output?
            }
        }
    }

    public static class PathNodeInitialReducer extends MapReduceBase implements
            Reducer<PositionWritable, MessageWritableNodeWithFlag, PositionWritable, MessageWritableNodeWithFlag> {

        private int KMER_SIZE;
        private MessageWritableNodeWithFlag inputValue;
        private MessageWritableNodeWithFlag outputValue;
        private NodeWritable nodeToKeep;
        private int count;
        private byte flag;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            outputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            nodeToKeep = new NodeWritable(KMER_SIZE);
        }

        @Override
        public void reduce(PositionWritable key, Iterator<MessageWritableNodeWithFlag> values,
                OutputCollector<PositionWritable, MessageWritableNodeWithFlag> output, Reporter reporter)
                throws IOException {

            inputValue.set(values.next());
            if (!values.hasNext()) {
                if ((inputValue.getFlag() & MessageFlag.FROM_SELF) == MessageFlag.FROM_SELF) {
                    // FROM_SELF => need to keep this PATH node
                    output.collect(key, inputValue);
                }
            } else {
                // multiple inputs => possible HEAD or TAIL to a path node. note if HEAD or TAIL node 
                count = 0;
                flag = MessageFlag.EMPTY_MESSAGE;
                while (true) { // process values; break when no more
                    count++;
                    if ((inputValue.getFlag() & MessageFlag.FROM_SELF) == MessageFlag.FROM_SELF) {
                        // SELF -> keep this node
                        nodeToKeep.set(inputValue.getNode());
                    } else if ((inputValue.getFlag() & MessageFlag.FROM_SUCCESSOR) == MessageFlag.FROM_SUCCESSOR) {
                        flag |= MessageFlag.IS_TAIL;
                        reporter.incrCounter("genomix", "path_nodes_tails", 1);
                    } else if ((inputValue.getFlag() & MessageFlag.FROM_PREDECESSOR) == MessageFlag.FROM_PREDECESSOR) {
                        flag |= MessageFlag.IS_HEAD;
                        reporter.incrCounter("genomix", "path_nodes_heads", 1);
                    }
                    if (!values.hasNext()) {
                        break;
                    } else {
                        inputValue = values.next();
                    }
                }
                if (count < 2) {
                    throw new IOException("Expected at least two nodes in PathNodeInitial reduce; saw "
                            + String.valueOf(count));
                }
                if ((flag & MessageFlag.FROM_SELF) == MessageFlag.FROM_SELF) {
                    // only map simple path nodes
                    outputValue.set(flag, nodeToKeep);
                    output.collect(key, outputValue);
                    reporter.incrCounter("genomix", "path_nodes", 1);
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
