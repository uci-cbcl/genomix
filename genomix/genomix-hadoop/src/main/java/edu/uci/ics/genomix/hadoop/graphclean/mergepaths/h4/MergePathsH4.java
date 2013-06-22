package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h4;

import java.io.File;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.genomix.hadoop.oldtype.VKmerBytesWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.MergePathMultiSeqOutputFormat;
import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable;
import edu.uci.ics.genomix.hadoop.pmcommon.NodeWithFlagWritable.MessageFlag;
import edu.uci.ics.genomix.hadoop.pmcommon.PathNodeInitial.PathNodeInitialReducer;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class MergePathsH4 extends Configured implements Tool {

    /*
     * Mapper class: Partition the graph using random pseudoheads.
     * Heads send themselves to their successors, and all others map themselves.
     */
    public static class MergePathsH4Mapper extends MapReduceBase implements
            Mapper<PositionWritable, NodeWithFlagWritable, PositionWritable, NodeWithFlagWritable> {
        private static long randSeed;
        private Random randGenerator;
        private float probBeingRandomHead;

        private int KMER_SIZE;
        private PositionWritable outputKey;
        private NodeWithFlagWritable outputValue;
        private NodeWritable curNode;
        private PositionWritable curID;
        private PositionWritable nextID;
        private PositionWritable prevID;
        private boolean hasNext;
        private boolean hasPrev;
        private boolean curHead;
        private boolean nextHead;
        private boolean prevHead;
        private boolean willMerge;
        private byte headFlag;
        private byte tailFlag;
        private byte outFlag;

        public void configure(JobConf conf) {
            
            randSeed = conf.getLong("randomSeed", 0);
            randGenerator = new Random(randSeed);
            probBeingRandomHead = conf.getFloat("probBeingRandomHead", 0.5f);

            KMER_SIZE = conf.getInt("sizeKmer", 0);
            outputValue = new NodeWithFlagWritable(KMER_SIZE);
            outputKey = new PositionWritable();
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
                nextID.set(node.getFFList().getPosition(0));
                nextHead = isNodeRandomHead(nextID);
                return true;
            }
            if (node.getFRList().getCountOfPosition() > 0) {
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
                prevID.set(node.getRRList().getPosition(0));
                prevHead = isNodeRandomHead(prevID);
                return true;
            }
            if (node.getRFList().getCountOfPosition() > 0) {
                prevID.set(node.getRFList().getPosition(0));
                prevHead = isNodeRandomHead(prevID);
                return true;
            }
            return false;
        }

        @Override
        public void map(PositionWritable key, NodeWithFlagWritable value,
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter)
                throws IOException {
            // Node may be marked as head b/c it's a real head or a real tail
            headFlag = (byte) (MessageFlag.IS_HEAD & value.getFlag());
            tailFlag = (byte) (MessageFlag.IS_TAIL & value.getFlag());
            outFlag = (byte) (headFlag | tailFlag);
            
            // only PATH vertices are present. Find the ID's for my neighbors
            curNode.set(value.getNode());
            curID.set(curNode.getNodeID());
            
            curHead = isNodeRandomHead(curID);
            // the headFlag and tailFlag's indicate if the node is at the beginning or end of a simple path. 
            // We prevent merging towards non-path nodes
            hasNext = setNextInfo(curNode) && tailFlag == 0;
            hasPrev = setPrevInfo(curNode) && headFlag == 0;
            willMerge = false;
            
            // TODO: need to update edges in neighboring nodes
            
            if ((outFlag & MessageFlag.IS_HEAD) > 0 && (outFlag & MessageFlag.IS_TAIL) > 0) {
                // true HEAD met true TAIL. this path is complete
                outFlag |= MessageFlag.MSG_SELF;
                outputValue.set(outFlag, curNode);
                output.collect(curID, outputValue);
                return;
            }
            if (hasNext || hasPrev) {
                if (curHead) {
                    if (hasNext && !nextHead) {
                        // compress this head to the forward tail
                        outFlag |= MessageFlag.FROM_PREDECESSOR;
                        outputValue.set(outFlag, curNode);
                        output.collect(nextID, outputValue);
                        willMerge = true;
                    } else if (hasPrev && !prevHead) {
                        // compress this head to the reverse tail
                        outFlag |= MessageFlag.FROM_SUCCESSOR;
                        outputValue.set(outFlag, curNode);
                        output.collect(prevID, outputValue);
                        willMerge = true;
                    }
                } else {
                    // I'm a tail
                    if (hasNext && hasPrev) {
                        if ((!nextHead && !prevHead) && (curID.compareTo(nextID) < 0 && curID.compareTo(prevID) < 0)) {
                            // tails on both sides, and I'm the "local minimum"
                            // compress me towards the tail in forward dir
                            outFlag |= MessageFlag.FROM_PREDECESSOR;
                            outputValue.set(outFlag, curNode);
                            output.collect(nextID, outputValue);
                            willMerge = true;
                        }
                    } else if (!hasPrev) {
                        // no previous node
                        if (!nextHead && curID.compareTo(nextID) < 0) {
                            // merge towards tail in forward dir
                            outFlag |= MessageFlag.FROM_PREDECESSOR;
                            outputValue.set(outFlag, curNode);
                            output.collect(nextID, outputValue);
                            willMerge = true;
                        }
                    } else if (!hasNext) {
                        // no next node
                        if (!prevHead && curID.compareTo(prevID) < 0) {
                            // merge towards tail in reverse dir
                            outFlag |= MessageFlag.FROM_SUCCESSOR;
                            outputValue.set(outFlag, curNode);
                            output.collect(prevID, outputValue);
                            willMerge = true;
                        }
                    }
                }
            }

            // if we didn't send ourselves to some other node, remap ourselves for the next round
            if (!willMerge) {
                outFlag |= MessageFlag.MSG_SELF;
                outputValue.set(outFlag, curNode);
                output.collect(curID, outputValue);
            }
            else {
                // TODO send update to this node's neighbors
                //mos.getCollector(UPDATES_OUTPUT, reporter).collect(key, outputValue);
            }
        }
    }

    /*
     * Reducer class: merge nodes that co-occur; for singletons, remap the original nodes 
     */
    private static class MergePathsH4Reducer extends MapReduceBase implements
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
                OutputCollector<PositionWritable, NodeWithFlagWritable> output, Reporter reporter)
                throws IOException {
        	toMergeCollector = mos.getCollector(TO_MERGE_OUTPUT, reporter);
        	completeCollector = mos.getCollector(COMPLETE_OUTPUT, reporter);
        	updatesCollector = mos.getCollector(UPDATES_OUTPUT, reporter);

            inputValue.set(values.next());
            if (!values.hasNext()) {
                if ((inputValue.getFlag() & MessageFlag.MSG_SELF) > 0) {
                    if ((inputValue.getFlag() & MessageFlag.IS_HEAD) > 0 && (inputValue.getFlag() & MessageFlag.IS_TAIL) > 0) {
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
    public RunningJob run(String inputPath, String toMergeOutput, String completeOutput, String updatesOutput, JobConf baseConf) throws IOException {
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

        conf.setMapperClass(MergePathsH4Mapper.class);
        conf.setReducerClass(MergePathsH4Reducer.class);
        
        MultipleOutputs.addNamedOutput(conf, MergePathsH4Reducer.TO_MERGE_OUTPUT, MergePathMultiSeqOutputFormat.class,
        		PositionWritable.class, NodeWithFlagWritable.class);
        MultipleOutputs.addNamedOutput(conf, MergePathsH4Reducer.COMPLETE_OUTPUT, MergePathMultiSeqOutputFormat.class,
        		PositionWritable.class, NodeWithFlagWritable.class);
        MultipleOutputs.addNamedOutput(conf, MergePathsH4Reducer.UPDATES_OUTPUT, MergePathMultiSeqOutputFormat.class,
        		PositionWritable.class, NodeWithFlagWritable.class);
        
        FileSystem dfs = FileSystem.get(conf); 
        // clean output dirs
        dfs.delete(outputPath, true);
        dfs.delete(new Path(toMergeOutput), true);
        dfs.delete(new Path(completeOutput), true);
        dfs.delete(new Path(updatesOutput), true);

        RunningJob job = JobClient.runJob(conf);
        
        // move the tmp outputs to the arg-spec'ed dirs. If there is no such dir, create an empty one to simplify downstream processing
        if (!dfs.rename(new Path(outputPath + File.separator +  MergePathsH4Reducer.TO_MERGE_OUTPUT), new Path(toMergeOutput))) {
            dfs.mkdirs(new Path(toMergeOutput));
        }
        if (!dfs.rename(new Path(outputPath + File.separator +  MergePathsH4Reducer.COMPLETE_OUTPUT), new Path(completeOutput))) {
            dfs.mkdirs(new Path(completeOutput));
        }
        if (!dfs.rename(new Path(outputPath + File.separator +  MergePathsH4Reducer.UPDATES_OUTPUT), new Path(updatesOutput))) {
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
