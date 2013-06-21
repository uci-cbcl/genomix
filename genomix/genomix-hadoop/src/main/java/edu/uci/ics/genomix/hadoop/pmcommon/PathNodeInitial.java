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

import java.io.File;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

/*
 * A map-reduce job to find all nodes that are part of a simple path and the mark the nodes that
 * form their heads and tails, also identifies parts of the graph that will participate in a path merge.
 * 
 * This MR job uses MultipleOutputs rather than remapping the entire graph each iteration:
 *   1. simple path nodes (indegree = outdegree = 1) (TO_MERGE_OUTPUT collector)
 *   2. non-path, "complete" nodes, which will not be affected by the path merging (COMPLETE_OUTPUT collector)
 *   3. non-path, "possibly updated" nodes, whose edges need to be updated after the merge (TO_UPDATE_OUTPUT collector)
 */
@SuppressWarnings("deprecation")
public class PathNodeInitial extends Configured implements Tool {

    public static final String COMPLETE_OUTPUT = "complete";
    public static final String TO_MERGE_OUTPUT = "toMerge";
    public static final String TO_UPDATE_OUTPUT = "toUpdate";

    public static class PathNodeFlag {
        public static final byte EMPTY_MESSAGE = 0;
        public static final byte FROM_SELF = 1 << 0;
        public static final byte IS_HEAD = 1 << 1;
        public static final byte IS_TAIL = 1 << 2;
        public static final byte IS_COMPLETE = 1 << 3;
        public static final byte NEAR_PATH = 1 << 4;
    }

    private static void sendOutputToNextNeighbors(NodeWritable node, MessageWritableNodeWithFlag outputValue,
            OutputCollector<PositionWritable, MessageWritableNodeWithFlag> collector) throws IOException {
        Iterator<PositionWritable> posIterator = node.getFFList().iterator(); // FFList
        while (posIterator.hasNext()) {
            collector.collect(posIterator.next(), outputValue);
        }
        posIterator = node.getFRList().iterator(); // FRList
        while (posIterator.hasNext()) {
            collector.collect(posIterator.next(), outputValue);
        }
    }

    private static void sendOutputToPreviousNeighbors(NodeWritable node, MessageWritableNodeWithFlag outputValue,
            OutputCollector<PositionWritable, MessageWritableNodeWithFlag> collector) throws IOException {
        Iterator<PositionWritable> posIterator = node.getRRList().iterator(); // RRList
        while (posIterator.hasNext()) {
            collector.collect(posIterator.next(), outputValue);
        }
        posIterator = node.getRFList().iterator(); // RFList
        while (posIterator.hasNext()) {
            collector.collect(posIterator.next(), outputValue);
        }
    }

    public static class PathNodeInitialMapper extends MapReduceBase implements
            Mapper<NodeWritable, NullWritable, PositionWritable, MessageWritableNodeWithFlag> {

        private int KMER_SIZE;
        private PositionWritable outputKey;
        private MessageWritableNodeWithFlag outputValue;
        private int inDegree;
        private int outDegree;
        private boolean pathNode;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            outputKey = new PositionWritable();
        }

        /*
         * Identify the heads and tails of simple path nodes and their neighbors
         * 
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        @Override
        public void map(NodeWritable key, NullWritable value,
                OutputCollector<PositionWritable, MessageWritableNodeWithFlag> output, Reporter reporter)
                throws IOException {
            inDegree = key.inDegree();
            outDegree = key.outDegree();
            if (inDegree == 1 && outDegree == 1) {
                pathNode = true;
            } else if (inDegree == 0 && outDegree == 1) {
                pathNode = true;
                // start of a tip.  needs to merge & be marked as head
                outputValue.set(PathNodeFlag.IS_HEAD, NodeWritable.EMPTY_NODE);
                output.collect(key.getNodeID(), outputValue);
            } else if (inDegree == 1 && outDegree == 0) {
                pathNode = true;
                // end of a tip.  needs to merge & be marked as tail
                outputValue.set(PathNodeFlag.IS_TAIL, NodeWritable.EMPTY_NODE);
                output.collect(key.getNodeID(), outputValue);
            } else {
                pathNode = false;
                if (outDegree > 0) {
                    // Not a path myself, but my successor might be one. Map forward successor to find heads
                    outputValue.set(PathNodeFlag.IS_HEAD, NodeWritable.EMPTY_NODE);
                    sendOutputToNextNeighbors(key, outputValue, output);
                }
                if (inDegree > 0) {
                    // Not a path myself, but my predecessor might be one. map predecessor to find tails 
                    outputValue.set(PathNodeFlag.IS_TAIL, NodeWritable.EMPTY_NODE);
                    sendOutputToPreviousNeighbors(key, outputValue, output);
                }
                // this non-path node won't participate in the merge
                outputValue.set((byte) (PathNodeFlag.FROM_SELF | PathNodeFlag.IS_COMPLETE), key);
                output.collect(key.getNodeID(), outputValue);
            }

            if (pathNode) {
                // simple path nodes map themselves
                outputValue.set(PathNodeFlag.FROM_SELF, key);
                output.collect(key.getNodeID(), outputValue);
                reporter.incrCounter("genomix", "path_nodes", 1);

                // also mark neighbors of paths (they are candidates for updates)
                outputValue.set(PathNodeFlag.NEAR_PATH, NodeWritable.EMPTY_NODE);
                sendOutputToNextNeighbors(key, outputValue, output);
                sendOutputToPreviousNeighbors(key, outputValue, output);
            }
        }
    }

    public static class PathNodeInitialReducer extends MapReduceBase implements
            Reducer<PositionWritable, MessageWritableNodeWithFlag, PositionWritable, MessageWritableNodeWithFlag> {
        private MultipleOutputs mos;
        private OutputCollector<PositionWritable, MessageWritableNodeWithFlag> toMergeCollector;
        private OutputCollector<PositionWritable, MessageWritableNodeWithFlag> completeCollector;
        private OutputCollector<PositionWritable, MessageWritableNodeWithFlag> toUpdateCollector;
        private int KMER_SIZE;

        private MessageWritableNodeWithFlag inputValue;
        private MessageWritableNodeWithFlag outputValue;
        private NodeWritable nodeToKeep;
        private byte outputFlag;
        private byte inputFlag;

        public void configure(JobConf conf) {
            mos = new MultipleOutputs(conf);
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            outputValue = new MessageWritableNodeWithFlag(KMER_SIZE);
            nodeToKeep = new NodeWritable(KMER_SIZE);
        }

        /*
         * Segregate nodes into three bins:
         *   1. mergeable nodes (marked as H/T)
         *   2. non-mergeable nodes that are candidates for updates
         *   3. non-mergeable nodes that are not path neighbors and won't be updated
         * 
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        @SuppressWarnings("unchecked")
        @Override
        public void reduce(PositionWritable key, Iterator<MessageWritableNodeWithFlag> values,
                OutputCollector<PositionWritable, MessageWritableNodeWithFlag> output, Reporter reporter)
                throws IOException {
            completeCollector = mos.getCollector(COMPLETE_OUTPUT, reporter);
            toMergeCollector = mos.getCollector(TO_MERGE_OUTPUT, reporter);
            toUpdateCollector = mos.getCollector(TO_UPDATE_OUTPUT, reporter);

            outputFlag = PathNodeFlag.EMPTY_MESSAGE;
            while (values.hasNext()) {
                inputValue.set(values.next());
                inputFlag = inputValue.getFlag();
                outputFlag |= inputFlag;

                if ((inputFlag & PathNodeFlag.FROM_SELF) > 0) {
                    // SELF -> keep this node
                    nodeToKeep.set(inputValue.getNode());
                }
            }

            if ((outputFlag & PathNodeFlag.FROM_SELF) > 0) {
                if ((outputFlag & PathNodeFlag.IS_COMPLETE) > 0) {
                    if ((outputFlag & PathNodeFlag.NEAR_PATH) > 0) {
                        // non-path, but update candidate
                        outputValue.set(PathNodeFlag.NEAR_PATH, nodeToKeep);
                        toUpdateCollector.collect(key, outputValue);
                    } else {
                        // non-path node.  Store in "complete" output
                        outputValue.set(PathNodeFlag.EMPTY_MESSAGE, nodeToKeep);
                        completeCollector.collect(key, outputValue);
                    }
                } else {
                    if ((outputFlag & PathNodeFlag.IS_HEAD) > 0 && (outputFlag & PathNodeFlag.IS_TAIL) > 0) {
                        // path nodes marked as H & T are single-node paths (not mergeable, not updateable)
                        outputValue.set(PathNodeFlag.EMPTY_MESSAGE, nodeToKeep);
                        completeCollector.collect(key, outputValue);
                    } else {
                        // path nodes that are mergeable
                        outputFlag &= (PathNodeFlag.IS_HEAD | PathNodeFlag.IS_TAIL); // clear flags except H/T 
                        outputValue.set(outputFlag, nodeToKeep);
                        toMergeCollector.collect(key, outputValue);

                        reporter.incrCounter("genomix", "path_nodes", 1);
                        if ((outputFlag & PathNodeFlag.IS_HEAD) > 0) {
                            reporter.incrCounter("genomix", "path_nodes_heads", 1);
                        }
                        if ((outputFlag & PathNodeFlag.IS_TAIL) > 0) {
                            reporter.incrCounter("genomix", "path_nodes_tails", 1);
                        }
                    }
                }
            } else {
                throw new IOException("No SELF node recieved in reduce! key=" + key.toString() + " flag=" + outputFlag);
            }
        }

        public void close() throws IOException {
            mos.close();
        }
    }

    /*
     * Mark the head, tail, and simple path nodes in one map-reduce job.
     */
    public RunningJob run(String inputPath, String toMergeOutput, String completeOutput, String toUpdateOutput,
            JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
        conf.setJarByClass(PathNodeInitial.class);
        conf.setJobName("PathNodeInitial " + inputPath);

        FileInputFormat.addInputPaths(conf, inputPath);
        Path outputPath = new Path(inputPath.replaceAll("/$", "") + ".initialMerge.tmp");
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);

        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(MessageWritableNodeWithFlag.class);
        conf.setOutputKeyClass(PositionWritable.class);
        conf.setOutputValueClass(MessageWritableNodeWithFlag.class);

        conf.setMapperClass(PathNodeInitialMapper.class);
        conf.setReducerClass(PathNodeInitialReducer.class);

        MultipleOutputs.addNamedOutput(conf, TO_MERGE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, MessageWritableNodeWithFlag.class);
        MultipleOutputs.addNamedOutput(conf, COMPLETE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, MessageWritableNodeWithFlag.class);
        MultipleOutputs.addNamedOutput(conf, TO_UPDATE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, MessageWritableNodeWithFlag.class);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(outputPath, true); // clean output dir
        RunningJob job = JobClient.runJob(conf);

        // move the tmp outputs to the arg-spec'ed dirs
        dfs.rename(new Path(outputPath + File.separator + TO_MERGE_OUTPUT), new Path(toMergeOutput));
        dfs.rename(new Path(outputPath + File.separator + COMPLETE_OUTPUT), new Path(completeOutput));
        dfs.rename(new Path(outputPath + File.separator + TO_UPDATE_OUTPUT), new Path(toUpdateOutput));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PathNodeInitial(), args);
        return res;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PathNodeInitial(), args);
        System.exit(res);
    }
}
