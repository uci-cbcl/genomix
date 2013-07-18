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

package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h4;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

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
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.genomix.hadoop.pmcommon.MergePathMultiSeqOutputFormat;
import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable.MessageFlag;
import edu.uci.ics.genomix.oldtype.NodeWritable;
import edu.uci.ics.genomix.oldtype.PositionWritable;

/*
 * a probabilistic merge algorithm for merging long single paths (chains without only 1 incoming and outgoing edge)
 * The merge is guaranteed to succeed, but not all nodes that could be merged in an iteration will be.
 * 
 * There are two steps to the merge: 
 *    1. (H4UpdatesMapper & H4UpdatesReducer): the direction of the merge is chosen and all 
 *       neighbor's edges are updated with the merge intent 
 *    2. H4MergeMapper & H4MergeReducer): the nodes initiating the merge are "sent" to their neighbors, kmers are combined, and edges 
 *       are again updated (since the merge-initiator may be neighbor to another merging node).  
 */
@SuppressWarnings("deprecation")
public class MergePathsH4 extends Configured implements Tool {

    private enum MergeDir {
        NO_MERGE,
        FORWARD,
        BACKWARD
    }

    /*
     * Mapper class: randomly chooses a direction to merge s.t. if a merge takes place, it will be successful.
     *      Sends update messages to all of this node's neighbors who their new neighbor will be
     */
    public static class H4UpdatesMapper extends MapReduceBase implements
            Mapper<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {
        private static long randSeed;
        private Random randGenerator;
        private float probBeingRandomHead;

        private int KMER_SIZE;
        private NodeWithFlagWritable outputValue;
        private NodeWithFlagWritable mergeMsgValue;
        private NodeWithFlagWritable updateMsgValue;

        private NodeWritable curNode;
        private PositionWritable curID;
        private PositionWritable nextID;
        private PositionWritable prevID;
        private boolean mergeableNext;
        private boolean mergeablePrev;
        private boolean curHead;
        private boolean nextHead;
        private boolean prevHead;
        private MergeDir mergeDir;
        private byte inFlag;
        private byte headFlag;
        private byte tailFlag;
        private byte mergeMsgFlag;
        private byte nextDir;
        private byte prevDir;

        public void configure(JobConf conf) {

            randSeed = conf.getLong("randomSeed", 0);
            randGenerator = new Random(randSeed);
            probBeingRandomHead = conf.getFloat("probBeingRandomHead", 0.5f);

            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);

            mergeMsgValue = new NodeWithFlagWritable(KMER_SIZE);
            updateMsgValue = new NodeWithFlagWritable(KMER_SIZE);

            curNode = new NodeWritable(KMER_SIZE);
            curID = new PositionWritable();
            nextID = new PositionWritable();
            prevID = new PositionWritable();
        }

        protected boolean isNodeRandomHead(PositionWritable nodeID) {
            // "deterministically random", based on node id
            randGenerator.setSeed(randSeed ^ nodeID.hashCode());

            // similar hashcodes will produce similar initial random values.  Burn through a few to increase spread
            for (int i = 0; i < 100; i++) {
                randGenerator.nextFloat();
            }
            return randGenerator.nextFloat() < probBeingRandomHead;
        }

        /*
         * set nextID to the element that's next (in the node's FF or FR list), returning true when there is a next neighbor
         */
        protected boolean setNextInfo(NodeWritable node) {
            if (node.getFFList().getCountOfPosition() > 0) {
                nextDir = MessageFlag.DIR_FF;
                nextID.set(node.getFFList().getPosition(0));
                nextHead = isNodeRandomHead(nextID);
                return true;
            }
            if (node.getFRList().getCountOfPosition() > 0) {
                nextDir = MessageFlag.DIR_FR;
                nextID.set(node.getFRList().getPosition(0));
                nextHead = isNodeRandomHead(nextID);
                return true;
            }
            return false;
        }

        /*
         * set prevID to the element that's previous (in the node's RR or RF list), returning true when there is a previous neighbor
         */
        protected boolean setPrevInfo(NodeWritable node) {
            if (node.getRRList().getCountOfPosition() > 0) {
                prevDir = MessageFlag.DIR_RR;
                prevID.set(node.getRRList().getPosition(0));
                prevHead = isNodeRandomHead(prevID);
                return true;
            }
            if (node.getRFList().getCountOfPosition() > 0) {
                prevDir = MessageFlag.DIR_RF;
                prevID.set(node.getRFList().getPosition(0));
                prevHead = isNodeRandomHead(prevID);
                return true;
            }
            return false;
        }

        @Override
        public void map(PositionWritable key, NodeWithFlagWritable value,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter) throws IOException {
            inFlag = value.getFlag();
            curNode.set(value.getNode());
            curID.set(curNode.getNodeID());
            mergeDir = MergeDir.NO_MERGE; // no merge to happen
            headFlag = (byte) (MessageFlag.IS_HEAD & inFlag);
            tailFlag = (byte) (MessageFlag.IS_TAIL & inFlag);
            mergeMsgFlag = (byte) (headFlag | tailFlag);

            curHead = isNodeRandomHead(curID);
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            boolean isPath = curNode.isSimpleOrTerminalPath();
            mergeableNext = setNextInfo(curNode) && tailFlag == 0;
            mergeablePrev = setPrevInfo(curNode) && headFlag == 0;

            // decide where we're going to merge to
            if (isPath && (mergeableNext || mergeablePrev)) {
                if (curHead) {
                    if (mergeableNext && !nextHead) {
                        // merge forward
                        mergeMsgFlag |= nextDir;
                        mergeDir = MergeDir.FORWARD;
                    } else if (mergeablePrev && !prevHead) {
                        // merge backwards
                        mergeMsgFlag |= prevDir;
                        mergeDir = MergeDir.BACKWARD;
                    }
                } else {
                    // I'm a tail
                    if (mergeableNext && mergeablePrev) {
                        if ((!nextHead && !prevHead) && (curID.compareTo(nextID) > 0 && curID.compareTo(prevID) > 0)) {
                            // tails on both sides, and I'm the "local minimum"
                            // compress me towards the tail in forward dir
                            mergeMsgFlag |= nextDir;
                            mergeDir = MergeDir.FORWARD;
                        }
                    } else if (!mergeablePrev) {
                        // no previous node
                        if (!nextHead && curID.compareTo(nextID) > 0) {
                            // merge towards tail in forward dir
                            mergeMsgFlag |= nextDir;
                            mergeDir = MergeDir.FORWARD;
                        }
                    } else if (!mergeableNext) {
                        // no next node
                        if (!prevHead && curID.compareTo(prevID) > 0) {
                            // merge towards tail in reverse dir
                            mergeMsgFlag |= prevDir;
                            mergeDir = MergeDir.BACKWARD;
                        }
                    }
                }
            }

            if (mergeDir == MergeDir.NO_MERGE) {
                mergeMsgFlag |= MessageFlag.MSG_SELF;
                mergeMsgValue.set(mergeMsgFlag, curNode);
                output.collect(curID, mergeMsgValue);
            } else {
                // this node will do a merge next round
                mergeMsgFlag |= MessageFlag.MSG_UPDATE_MERGE;
                mergeMsgValue.set(mergeMsgFlag, curNode);
                output.collect(curID, mergeMsgValue);

                sendUpdateToNeighbors(curNode, (byte) (mergeMsgFlag & MessageFlag.DIR_MASK), output);
            }
        }

        /*
         * when performing a merge, an update message needs to be sent to my neighbors
         */
        private void sendUpdateToNeighbors(NodeWritable node, byte mergeDir,
                OutputCollector<PositionWritable, NodeWithFlagWritable> collector) throws IOException {
            PositionWritable mergeSource = node.getNodeID();
            PositionWritable mergeTarget = node.getListFromDir(mergeDir).getPosition(0);

            // I need to notify in the opposite direction as I'm merging
            Iterator<PositionWritable> posIterator1;
            byte dir1;
            Iterator<PositionWritable> posIterator2;
            byte dir2;
            switch (mergeDir) {
                case MessageFlag.DIR_FF:
                case MessageFlag.DIR_FR:
                    // merging forward; tell my previous neighbors
                    posIterator1 = node.getRRList().iterator();
                    dir1 = MessageFlag.DIR_RR;
                    posIterator2 = node.getRFList().iterator();
                    dir2 = MessageFlag.DIR_RF;
                    break;
                case MessageFlag.DIR_RF:
                case MessageFlag.DIR_RR:
                    posIterator1 = node.getFFList().iterator();
                    dir1 = MessageFlag.DIR_FF;
                    posIterator2 = node.getFRList().iterator();
                    dir2 = MessageFlag.DIR_FR;
                    break;
                default:
                    throw new IOException("Unrecognized direction in sendUpdateToNeighbors: " + mergeDir);
            }
            while (posIterator1.hasNext()) {
                updateMsgValue.setAsUpdateMessage(mergeDir, dir1, mergeSource, mergeTarget);
                collector.collect(posIterator1.next(), updateMsgValue);
            }
            while (posIterator2.hasNext()) {
                updateMsgValue.setAsUpdateMessage(mergeDir, dir2, mergeSource, mergeTarget);
                collector.collect(posIterator2.next(), outputValue);
            }
        }
    }

    /*
     * Reducer class: processes the update messages from updateMapper
     */
    private static class H4UpdatesReducer extends MapReduceBase implements
            Reducer<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {
        private int KMER_SIZE;
        private NodeWithFlagWritable inputValue;
        private NodeWithFlagWritable outputValue;
        private PositionWritable outPosn;
        private boolean sawCurNode;
        private byte inFlag;

        // to prevent GC on update messages, we keep them all in one list and use the Node set method rather than creating new Node's
        private ArrayList<NodeWithFlagWritable> updateMsgs;
        private int updateMsgsSize;
        private int updateMsgsCount;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new NodeWithFlagWritable(KMER_SIZE);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            outPosn = new PositionWritable();
            updateMsgs = new ArrayList<NodeWithFlagWritable>();
            updateMsgsSize = updateMsgs.size();
        }

        private void addUpdateMessage(NodeWithFlagWritable myInputValue) {
            updateMsgsCount++;
            if (updateMsgsCount >= updateMsgsSize) {
                updateMsgs.add(new NodeWithFlagWritable(myInputValue)); // make a copy of inputValue-- not a reference!
            } else {
                updateMsgs.get(updateMsgsCount - 1).set(myInputValue); // update existing reference
            }
        }

        /*
         * Process updates from mapper
         * 
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        @Override
        public void reduce(PositionWritable key, Iterator<NodeWithFlagWritable> values,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter) throws IOException {
            sawCurNode = false;
            updateMsgsCount = 0;

            byte inMsg;
            while (values.hasNext()) {
                inputValue.set(values.next());
                inFlag = inputValue.getFlag();
                inMsg = (byte) (inFlag & MessageFlag.MSG_MASK);

                switch (inMsg) {
                    case MessageFlag.MSG_UPDATE_MERGE:
                    case MessageFlag.MSG_SELF:
                        if (sawCurNode)
                            throw new IOException("Saw more than one MSG_SELF! previously seen self: "
                                    + outputValue.getNode() + "  current self: " + inputValue.getNode());
                        if (inMsg == MessageFlag.MSG_SELF) {
                            outPosn.set(outputValue.getNode().getNodeID());
                        } else if (inMsg == MessageFlag.MSG_UPDATE_MERGE) {
                            // merge messages are sent to their merge recipient
                            outPosn.set(outputValue.getNode().getListFromDir(inMsg).getPosition(0));
                        } else {
                            throw new IOException("Unrecongized MessageFlag MSG: " + inMsg);
                        }
                        outputValue.set(inFlag, inputValue.getNode());
                        sawCurNode = true;
                        break;
                    case MessageFlag.MSG_UPDATE_EDGE:
                        addUpdateMessage(inputValue);
                        break;
                    default:
                        throw new IOException("Unrecognized message type: " + (inFlag & MessageFlag.MSG_MASK));
                }
            }
            if (!sawCurNode) {
                throw new IOException("Never saw self in recieve update messages!");
            }

            // process all the update messages for this node
            for (int i = 0; i < updateMsgsCount; i++) {
                outputValue.processUpdates(updateMsgs.get(i), KMER_SIZE);
            }
            output.collect(outPosn, outputValue);
        }
    }

    /*
     * Reducer class: processes merge messages 
     */
    private static class H4MergeReducer extends MapReduceBase implements
            Reducer<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {
        private MultipleOutputs mos;
        public static final String TO_UPDATE_OUTPUT = "toUpdate";
        public static final String COMPLETE_OUTPUT = "complete";
        private OutputCollector<PositionWritable, NodeWithFlagWritable> toUpdateCollector;
        private OutputCollector<NodeWritable, NullWritable> completeCollector;

        private int KMER_SIZE;
        private NodeWithFlagWritable inputValue;
        private NodeWithFlagWritable outputValue;
        private PositionWritable outputKey;
        private boolean sawCurNode;
        private byte inFlag;

        // to prevent GC on update messages, we keep them all in one list and use the Node set method rather than creating new Node's
        private ArrayList<NodeWithFlagWritable> mergeMsgs;
        private int updateMsgsSize;
        private int mergeMsgsCount;

        public void configure(JobConf conf) {
            mos = new MultipleOutputs(conf);
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new NodeWithFlagWritable(KMER_SIZE);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            outputKey = new PositionWritable();
            mergeMsgs = new ArrayList<NodeWithFlagWritable>();
            updateMsgsSize = mergeMsgs.size();
        }

        private void addMergeMessage(NodeWithFlagWritable myInputValue) {
            mergeMsgsCount++;
            if (mergeMsgsCount >= updateMsgsSize) {
                mergeMsgs.add(new NodeWithFlagWritable(myInputValue)); // make a copy of inputValue-- not a reference!
            } else {
                mergeMsgs.get(mergeMsgsCount - 1).set(myInputValue); // update existing reference
            }
        }

        /*
         * Process merges
         * 
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        @SuppressWarnings("unchecked")
        @Override
        public void reduce(PositionWritable key, Iterator<NodeWithFlagWritable> values,
                OutputCollector<PositionWritable, NodeWithFlagWritable> toMergeCollector, Reporter reporter)
                throws IOException {
            toUpdateCollector = mos.getCollector(TO_UPDATE_OUTPUT, reporter);
            completeCollector = mos.getCollector(COMPLETE_OUTPUT, reporter);
            sawCurNode = false;
            mergeMsgsCount = 0;

            while (values.hasNext()) {
                inputValue.set(values.next());
                inFlag = inputValue.getFlag();
                switch (inFlag & MessageFlag.MSG_MASK) {
                    case MessageFlag.MSG_SELF:
                        if (sawCurNode)
                            throw new IOException("Saw more than one MSG_SELF! previously seen self: "
                                    + outputValue.getNode() + "  current self: " + inputValue.getNode());
                        outputKey.set(outputValue.getNode().getNodeID());
                        outputValue.set(inFlag, inputValue.getNode());
                        sawCurNode = true;
                        break;
                    case MessageFlag.MSG_UPDATE_MERGE:
                        addMergeMessage(inputValue);
                        break;
                    case MessageFlag.MSG_UPDATE_EDGE:
                        throw new IOException("Error: update message recieved during merge phase!" + inputValue);
                    default:
                        throw new IOException("Unrecognized message type: " + (inFlag & MessageFlag.MSG_MASK));
                }
            }
            if (!sawCurNode) {
                throw new IOException("Never saw self in recieve update messages!");
            }

            // process all the merge messages for this node
            for (int i = 0; i < mergeMsgsCount; i++) {
                outputValue.processUpdates(mergeMsgs.get(i), KMER_SIZE);
            }

            if (!outputValue.getNode().isSimpleOrTerminalPath()) {
                // not a mergeable path, can't tell if it still needs updates!
                toUpdateCollector.collect(outputKey, outputValue);
            } else if ((outputValue.getFlag() & MessageFlag.IS_HEAD) > 0
                    && ((outputValue.getFlag() & MessageFlag.IS_TAIL) > 0)) {
                // H + T indicates a complete path
                completeCollector.collect(outputValue.getNode(), NullWritable.get());
            } else {
                // not finished merging yet
                toMergeCollector.collect(outputKey, outputValue);
            }
        }

        public void close() throws IOException {
            mos.close();
        }
    }

    /*
     * Run one iteration of the mergePaths algorithm
     */
    public RunningJob run(String inputPath, String toMergeOutput, String toUpdateOutput, String completeOutput, JobConf baseConf)
            throws IOException {
        JobConf conf = new JobConf(baseConf);
        FileSystem dfs = FileSystem.get(conf);
        conf.setJarByClass(MergePathsH4.class);
        conf.setJobName("MergePathsH4 " + inputPath);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(NodeWithFlagWritable.class);
        conf.setOutputKeyClass(PositionWritable.class);
        conf.setOutputValueClass(NodeWithFlagWritable.class);

        // step 1: decide merge dir and send updates
        FileInputFormat.addInputPaths(conf, inputPath);
        String outputUpdatesTmp = "h4.updatesProcessed." + new Random().nextDouble() + ".tmp"; // random filename
        FileOutputFormat.setOutputPath(conf, new Path(outputUpdatesTmp));
        dfs.delete(new Path(outputUpdatesTmp), true);
        conf.setMapperClass(H4UpdatesMapper.class);
        conf.setReducerClass(H4UpdatesReducer.class);
        RunningJob job = JobClient.runJob(conf);

        // step 2: process merges
        FileInputFormat.addInputPaths(conf, outputUpdatesTmp);
        for (Path out : FileInputFormat.getInputPaths(conf)) {
            System.out.println(out);
        }
        Path outputMergeTmp = new Path("h4.mergeProcessed." + new Random().nextDouble() + ".tmp"); // random filename
        FileOutputFormat.setOutputPath(conf, outputMergeTmp);
        MultipleOutputs.addNamedOutput(conf, H4MergeReducer.TO_UPDATE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, NodeWithFlagWritable.class);
        MultipleOutputs.addNamedOutput(conf, H4MergeReducer.COMPLETE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                NodeWritable.class, NullWritable.class);
        dfs.delete(outputMergeTmp, true);
        conf.setMapperClass(IdentityMapper.class);
        conf.setReducerClass(H4MergeReducer.class);
        job = JobClient.runJob(conf);

        // move the tmp outputs to the arg-spec'ed dirs. If there is no such dir, create an empty one to simplify downstream processing
        if (!dfs.rename(new Path(outputMergeTmp + File.separator + H4MergeReducer.TO_UPDATE_OUTPUT), new Path(
                toUpdateOutput))) {
            dfs.mkdirs(new Path(toUpdateOutput));
        }
        if (!dfs.rename(new Path(outputMergeTmp + File.separator + H4MergeReducer.COMPLETE_OUTPUT), new Path(
                completeOutput))) {
            dfs.mkdirs(new Path(completeOutput));
        }
        if (!dfs.rename(outputMergeTmp, new Path(toMergeOutput))) {
            dfs.mkdirs(new Path(toMergeOutput));
        }

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MergePathsH4(), args);
        return res;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MergePathsH4(), args);
        System.exit(res);
    }
}
