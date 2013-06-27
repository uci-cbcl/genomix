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
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.genomix.hadoop.oldtype.VKmerBytesWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.MergePathMultiSeqOutputFormat;
import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable.MessageFlag;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial.PathNodeInitialReducer;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

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
        private boolean hasNext;
        private boolean hasPrev;
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

            headFlag = (byte) (MessageFlag.IS_HEAD & inFlag);
            tailFlag = (byte) (MessageFlag.IS_TAIL & inFlag);
            mergeMsgFlag = (byte) (headFlag | tailFlag);

            curHead = isNodeRandomHead(curID);
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            hasNext = setNextInfo(curNode) && tailFlag == 0;
            hasPrev = setPrevInfo(curNode) && headFlag == 0;
            mergeDir = MergeDir.NO_MERGE; // no merge to happen

            // decide where we're going to merge to
            if (hasNext || hasPrev) {
                if (curHead) {
                    if (hasNext && !nextHead) {
                        // merge forward
                        mergeMsgFlag |= nextDir;
                        mergeDir = MergeDir.FORWARD;
                    } else if (hasPrev && !prevHead) {
                        // merge backwards
                        mergeMsgFlag |= prevDir;
                        mergeDir = MergeDir.BACKWARD;
                    }
                } else {
                    // I'm a tail
                    if (hasNext && hasPrev) {
                        if ((!nextHead && !prevHead) && (curID.compareTo(nextID) < 0 && curID.compareTo(prevID) < 0)) {
                            // tails on both sides, and I'm the "local minimum"
                            // compress me towards the tail in forward dir
                            mergeMsgFlag |= nextDir;
                            mergeDir = MergeDir.FORWARD;
                        }
                    } else if (!hasPrev) {
                        // no previous node
                        if (!nextHead && curID.compareTo(nextID) < 0) {
                            // merge towards tail in forward dir
                            mergeMsgFlag |= nextDir;
                            mergeDir = MergeDir.FORWARD;
                        }
                    } else if (!hasNext) {
                        // no next node
                        if (!prevHead && curID.compareTo(prevID) < 0) {
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
        private NodeWritable curNode;
        private PositionWritable outPosn;
        private ArrayList<NodeWithFlagWritable> updateMsgs;
        private boolean sawCurNode;
        private byte outFlag;
        private byte inFlag;

        public void configure(JobConf conf) {
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new NodeWithFlagWritable(KMER_SIZE);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            curNode = new NodeWritable(KMER_SIZE);
            outPosn = new PositionWritable();
            updateMsgs = new ArrayList<NodeWithFlagWritable>();
        }

        /*
         * Process updates from mapper
         * 
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        @SuppressWarnings("unchecked")
        @Override
        public void reduce(PositionWritable key, Iterator<NodeWithFlagWritable> values,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter) throws IOException {
            sawCurNode = false;
            updateMsgs.clear();
            
            byte inDir;
            while (values.hasNext()) {
                inputValue.set(values.next());
                inFlag = inputValue.getFlag();
                inDir = (byte) (inFlag & MessageFlag.MSG_MASK);
                
                switch (inDir) {
                    case MessageFlag.MSG_UPDATE_MERGE:
                    case MessageFlag.MSG_SELF:
                        if (sawCurNode)
                            throw new IOException("Saw more than one MSG_SELF! previously seen self: " + curNode
                                    + "  current self: " + inputValue.getNode());
                        curNode.set(inputValue.getNode());
                        outFlag = inFlag;
                        sawCurNode = true;
                        if (inDir == MessageFlag.MSG_SELF) {
                            outPosn.set(curNode.getNodeID());
                        } else {  // MSG_UPDATE_MERGE
                            // merge messages are sent to their merge recipient
                            outPosn.set(curNode.getListFromDir(inDir).getPosition(0));
                        }
                        break;
                    case MessageFlag.MSG_UPDATE_EDGE:
                        updateMsgs.add(new NodeWithFlagWritable(inputValue)); // make a copy of inputValue-- not a reference!
                        break;
                    default:
                        throw new IOException("Unrecognized message type: " + (inFlag & MessageFlag.MSG_MASK));
                }
            }

            // process all the update messages for this node
            // I have no idea how to make this more efficient...
            for (NodeWithFlagWritable updateMsg : updateMsgs) {
                NodeWithFlagWritable.processUpdates(curNode, updateMsg, KMER_SIZE);
            }
            outputValue.set(outFlag, curNode);
            output.collect(outPosn, outputValue);
        }
    }
    
    
    /*
     * Mapper class: sends the update messages to their (already decided) destination
     */
    public static class H4MergeMapper extends MapReduceBase implements
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
        private boolean hasNext;
        private boolean hasPrev;
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

        @Override
        public void map(PositionWritable key, NodeWithFlagWritable value,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter) throws IOException {
            inFlag = value.getFlag();
            curNode.set(value.getNode());
            curID.set(curNode.getNodeID());
            
        }

    }

    
    
    

    /*
     * Reducer class: processes the update messages from updateMapper
     */
    private static class H4MergeReducer2 extends MapReduceBase implements
            Reducer<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {
        private MultipleOutputs mos;
        private static final String TO_MERGE_OUTPUT = "toMerge";
        private static final String COMPLETE_OUTPUT = "complete";
        private static final String UPDATES_OUTPUT = "update";
        private OutputCollector<PositionWritable, NodeWithFlagWritable> toMergeCollector;
        private OutputCollector<PositionWritable, NodeWithFlagWritable> completeCollector;
        private OutputCollector<PositionWritable, NodeWithFlagWritable> updatesCollector;

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
            mos = new MultipleOutputs(conf);
            KMER_SIZE = conf.getInt("sizeKmer", 0);
            inputValue = new NodeWithFlagWritable(KMER_SIZE);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            curNode = new NodeWritable(KMER_SIZE);
            prevNode = new NodeWritable(KMER_SIZE);
            nextNode = new NodeWritable(KMER_SIZE);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void reduce(PositionWritable key, Iterator<NodeWithFlagWritable> values,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter) throws IOException {
            toMergeCollector = mos.getCollector(TO_MERGE_OUTPUT, reporter);
            completeCollector = mos.getCollector(COMPLETE_OUTPUT, reporter);
            updatesCollector = mos.getCollector(UPDATES_OUTPUT, reporter);

            inputValue.set(values.next());
            if (!values.hasNext()) {
                if ((inputValue.getFlag() & MessageFlag.MSG_SELF) > 0) {
                    if ((inputValue.getFlag() & MessageFlag.IS_HEAD) > 0
                            && (inputValue.getFlag() & MessageFlag.IS_TAIL) > 0) {
                        // complete path (H & T meet in this node)
                        completeCollector.collect(key, inputValue);
                    } else {
                        // FROM_SELF => no merging this round. remap self
                        toMergeCollector.collect(key, inputValue);
                    }
                } else if ((inputValue.getFlag() & (MessageFlag.FROM_PREDECESSOR | MessageFlag.FROM_SUCCESSOR)) > 0) {
                    // FROM_PREDECESSOR | FROM_SUCCESSOR, but singleton?  error here!
                    throw new IOException("Only one value recieved in merge, but it wasn't from self!");
                }
            } else {
                // multiple inputs => a merge will take place. Aggregate all, then collect the merged path
                count = 0;
                outFlag = MessageFlag.EMPTY_MESSAGE;
                sawCurNode = false;
                sawPrevNode = false;
                sawNextNode = false;
                while (true) { // process values; break when no more
                    count++;
                    outFlag |= (inputValue.getFlag() & (MessageFlag.IS_HEAD | MessageFlag.IS_TAIL)); // merged node may become HEAD or TAIL
                    if ((inputValue.getFlag() & MessageFlag.FROM_PREDECESSOR) > 0) {
                        prevNode.set(inputValue.getNode());
                        sawPrevNode = true;
                    } else if ((inputValue.getFlag() & MessageFlag.FROM_SUCCESSOR) > 0) {
                        nextNode.set(inputValue.getNode());
                        sawNextNode = true;
                    } else if ((inputValue.getFlag() & MessageFlag.MSG_SELF) > 0) {
                        curNode.set(inputValue.getNode());
                        sawCurNode = true;
                    } else {
                        throw new IOException("Unknown origin for merging node");
                    }
                    if (!values.hasNext()) {
                        break;
                    } else {
                        inputValue.set(values.next());
                    }
                }
                if (count != 2 && count != 3) {
                    throw new IOException("Expected two or three nodes in MergePathsH4 reduce; saw "
                            + String.valueOf(count));
                }
                if (!sawCurNode) {
                    throw new IOException("Didn't see node from self in MergePathsH4 reduce!");
                }

                // merge any received nodes
                if (sawNextNode) {
                    curNode.mergeForwardNext(nextNode, KMER_SIZE);
                    reporter.incrCounter("genomix", "num_merged", 1);
                }
                if (sawPrevNode) {
                    // TODO: fix this merge command!  which one is the right one?
                    curNode.mergeForwardPre(prevNode, KMER_SIZE);
                    reporter.incrCounter("genomix", "num_merged", 1);
                }

                outputValue.set(outFlag, curNode);
                if ((outFlag & MessageFlag.IS_HEAD) > 0 && (outFlag & MessageFlag.IS_TAIL) > 0) {
                    // True heads meeting tails => merge is complete for this node
                    completeCollector.collect(key, outputValue);
                } else {
                    toMergeCollector.collect(key, outputValue);
                }
            }
        }

        public void close() throws IOException {
            mos.close();
        }
    }

    /*
     * Run one iteration of the mergePaths algorithm
     */
    public RunningJob run(String inputPath, String toMergeOutput, String completeOutput, String updatesOutput,
            JobConf baseConf) throws IOException {
        JobConf conf = new JobConf(baseConf);
        conf.setJarByClass(MergePathsH4.class);
        conf.setJobName("MergePathsH4 " + inputPath);

        FileInputFormat.addInputPaths(conf, inputPath);
        Path outputPath = new Path(inputPath + ".h4merge.tmp");
        FileOutputFormat.setOutputPath(conf, outputPath);

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);

        conf.setMapOutputKeyClass(PositionWritable.class);
        conf.setMapOutputValueClass(NodeWithFlagWritable.class);
        conf.setOutputKeyClass(PositionWritable.class);
        conf.setOutputValueClass(NodeWithFlagWritable.class);

        conf.setMapperClass(H4UpdatesMapper.class);
        conf.setReducerClass(H4UpdatesReducer.class);

        MultipleOutputs.addNamedOutput(conf, H4UpdatesReducer.TO_MERGE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, NodeWithFlagWritable.class);
        MultipleOutputs.addNamedOutput(conf, H4UpdatesReducer.COMPLETE_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, NodeWithFlagWritable.class);
        MultipleOutputs.addNamedOutput(conf, H4UpdatesReducer.UPDATES_OUTPUT, MergePathMultiSeqOutputFormat.class,
                PositionWritable.class, NodeWithFlagWritable.class);

        FileSystem dfs = FileSystem.get(conf);
        // clean output dirs
        dfs.delete(outputPath, true);
        dfs.delete(new Path(toMergeOutput), true);
        dfs.delete(new Path(completeOutput), true);
        dfs.delete(new Path(updatesOutput), true);

        RunningJob job = JobClient.runJob(conf);

        // move the tmp outputs to the arg-spec'ed dirs. If there is no such dir, create an empty one to simplify downstream processing
        if (!dfs.rename(new Path(outputPath + File.separator + H4UpdatesReducer.TO_MERGE_OUTPUT), new Path(
                toMergeOutput))) {
            dfs.mkdirs(new Path(toMergeOutput));
        }
        if (!dfs.rename(new Path(outputPath + File.separator + H4UpdatesReducer.COMPLETE_OUTPUT), new Path(
                completeOutput))) {
            dfs.mkdirs(new Path(completeOutput));
        }
        if (!dfs.rename(new Path(outputPath + File.separator + H4UpdatesReducer.UPDATES_OUTPUT),
                new Path(updatesOutput))) {
            dfs.mkdirs(new Path(updatesOutput));
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
