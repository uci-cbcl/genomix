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

import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable.MessageFlag;
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

    private static byte NEAR_PATH = MessageFlag.EXTRA_FLAG; // special-case extra flag for us

    public static void sendOutputToNextNeighbors(NodeWritable node, NodeWithFlagWritable outputValue,
            OutputCollector<PositionWritable, NodeWithFlagWritable> collector) throws IOException {
        Iterator<PositionWritable> posIterator = node.getFFList().iterator(); // FFList
        while (posIterator.hasNext()) {
            collector.collect(posIterator.next(), outputValue);
        }
        posIterator = node.getFRList().iterator(); // FRList
        while (posIterator.hasNext()) {
            collector.collect(posIterator.next(), outputValue);
        }
    }

    public static void sendOutputToPreviousNeighbors(NodeWritable node, NodeWithFlagWritable outputValue,
            OutputCollector<PositionWritable, NodeWithFlagWritable> collector) throws IOException {
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
            Mapper<NodeWritable, NullWritable, PositionWritable, NodeWithFlagWritable> {

        private int KMER_SIZE;
        private PositionWritable outputKey;
        private NodeWithFlagWritable outputValue;
        private int inDegree;
        private int outDegree;
        private boolean pathNode;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
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
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter) throws IOException {
            inDegree = key.inDegree();
            outDegree = key.outDegree();
            if (inDegree == 1 && outDegree == 1) {
                pathNode = true;
            } else if (inDegree == 0 && outDegree == 1) {
                pathNode = true;
                // start of a tip.  needs to merge & be marked as head
                outputValue.set(MessageFlag.IS_HEAD, NodeWritable.EMPTY_NODE);
                output.collect(key.getNodeID(), outputValue);
            } else if (inDegree == 1 && outDegree == 0) {
                pathNode = true;
                // end of a tip.  needs to merge & be marked as tail
                outputValue.set(MessageFlag.IS_TAIL, NodeWritable.EMPTY_NODE);
                output.collect(key.getNodeID(), outputValue);
            } else {
                pathNode = false;
                if (outDegree > 0) {
                    // Not a path myself, but my successor might be one. Map forward successor to find heads
                    outputValue.set(MessageFlag.IS_HEAD, NodeWritable.EMPTY_NODE);
                    sendOutputToNextNeighbors(key, outputValue, output);
                }
                if (inDegree > 0) {
                    // Not a path myself, but my predecessor might be one. map predecessor to find tails 
                    outputValue.set(MessageFlag.IS_TAIL, NodeWritable.EMPTY_NODE);
                    sendOutputToPreviousNeighbors(key, outputValue, output);
                }
                // this non-path node won't participate in the merge. Mark as "complete" (H + T)
                outputValue.set((byte) (MessageFlag.MSG_SELF | MessageFlag.IS_HEAD | MessageFlag.IS_TAIL), key);
                output.collect(key.getNodeID(), outputValue);
            }

            if (pathNode) {
                // simple path nodes map themselves
                outputValue.set(MessageFlag.MSG_SELF, key);
                output.collect(key.getNodeID(), outputValue);
                reporter.incrCounter("genomix", "path_nodes", 1);

                // also mark neighbors of paths (they are candidates for updates)
                outputValue.set(NEAR_PATH, NodeWritable.EMPTY_NODE);
                sendOutputToNextNeighbors(key, outputValue, output);
                sendOutputToPreviousNeighbors(key, outputValue, output);
            }
        }
    }

    public static class PathNodeInitialReducer extends MapReduceBase implements
            Reducer<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {
        private MultipleOutputs mos;
        private OutputCollector<PositionWritable, NodeWithFlagWritable> completeCollector;
        private OutputCollector<PositionWritable, NodeWithFlagWritable> toUpdateCollector;
        private int KMER_SIZE;

        private NodeWithFlagWritable inputValue;
        private NodeWithFlagWritable outputValue;
        private NodeWritable nodeToKeep;
        private byte outputFlag;
        private byte inputFlag;
        private boolean sawSelf;

        public void configure(JobConf conf) {
            mos = new MultipleOutputs(conf);
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new NodeWithFlagWritable(KMER_SIZE);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            nodeToKeep = new NodeWritable(KMER_SIZE);
        }

        /*
         * Segregate nodes into three bins:
         *   1. mergeable nodes (maybe marked H or T)
         *   2. non-mergeable nodes that are candidates for updates
         *   3. non-mergeable nodes that are not path neighbors and won't be updated
         * 
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        @SuppressWarnings("unchecked")
        @Override
        public void reduce(PositionWritable key, Iterator<NodeWithFlagWritable> values,
                OutputCollector<PositionWritable, NodeWithFlagWritable> toMergeCollector, Reporter reporter)
                throws IOException {
            completeCollector = mos.getCollector(COMPLETE_OUTPUT, reporter);
            toUpdateCollector = mos.getCollector(TO_UPDATE_OUTPUT, reporter);

            outputFlag = MessageFlag.EMPTY_MESSAGE;
            sawSelf = false;
            while (values.hasNext()) {
                inputValue.set(values.next());
                inputFlag = inputValue.getFlag();
                outputFlag |= inputFlag;

                if ((inputFlag & MessageFlag.MSG_SELF) > 0) {
                    // SELF -> keep this node
                    if (sawSelf) {
                        throw new IOException("Already saw SELF node in PathNodeInitialReducer! previous self: "
                                + nodeToKeep.toString() + ". current self: " + inputValue.getNode().toString());
                    }
                    sawSelf = true;
                    nodeToKeep.set(inputValue.getNode());
                }
            }

            if ((outputFlag & MessageFlag.MSG_SELF) > 0) {
                if ((outputFlag & MessageFlag.IS_HEAD) > 0 && (outputFlag & MessageFlag.IS_TAIL) > 0) {
                    // non-path or single path nodes
                    if ((outputFlag & NEAR_PATH) > 0) {
                        // non-path, but an update candidate
                        outputValue.set(MessageFlag.EMPTY_MESSAGE, nodeToKeep);
                        toUpdateCollector.collect(key, outputValue);
                    } else {
                        // non-path or single-node path.  Store in "complete" output
                        outputValue.set(MessageFlag.EMPTY_MESSAGE, nodeToKeep);
                        completeCollector.collect(key, outputValue);
                    }
                } else {
                    // path nodes that are mergeable
                    outputFlag &= (MessageFlag.IS_HEAD | MessageFlag.IS_TAIL); // clear flags except H/T
                    outputValue.set(outputFlag, nodeToKeep);
                    toMergeCollector.collect(key, outputValue);

                    reporter.incrCounter("genomix", "path_nodes", 1);
                    if ((outputFlag & MessageFlag.IS_HEAD) > 0) {
                        reporter.incrCounter("genomix", "path_nodes_heads", 1);
                    }
                    if ((outputFlag & MessageFlag.IS_TAIL) > 0) {
                        reporter.incrCounter("genomix", "path_nodes_tails", 1);
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
        //        Path outputPath = new Path(inputPath.replaceAll("/$", "") + ".initialMerge.tmp");
        Path outputPath = new Path(toMergeOutput);
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);

        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(NodeWithFlagWritable.class);
        conf.setOutputKeyClass(PositionWritable.class);
        conf.setOutputValueClass(NodeWithFlagWritable.class);

        conf.setMapperClass(PathNodeInitialMapper.class);
        conf.setReducerClass(PathNodeInitialReducer.class);

        MultipleOutputs.addNamedOutput(conf, COMPLETE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, NodeWithFlagWritable.class);
        MultipleOutputs.addNamedOutput(conf, TO_UPDATE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, NodeWithFlagWritable.class);

        FileSystem dfs = FileSystem.get(conf);
        dfs.delete(outputPath, true); // clean output dir
        RunningJob job = JobClient.runJob(conf);

        // move the tmp outputs to the arg-spec'ed dirs
        dfs.rename(new Path(outputPath + File.separator + COMPLETE_OUTPUT), new Path(completeOutput));
        dfs.rename(new Path(outputPath + File.separator + TO_UPDATE_OUTPUT), new Path(toUpdateOutput));
        //        dfs.rename(outputPath, new Path(toMergeOutput));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PathNodeInitial(), args);
        return res;
    }

    public static void main(String[] args) throws Exception {
        int res = new PathNodeInitial().run(args);
        System.exit(res);
    }
}
