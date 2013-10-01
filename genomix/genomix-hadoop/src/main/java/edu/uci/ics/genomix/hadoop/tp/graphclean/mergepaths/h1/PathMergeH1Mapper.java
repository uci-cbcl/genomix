package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanNode;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.MsgListWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanNode.DIR;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.MessageFlag;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.VertexUtil;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.WholeStateInformation.State;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings({ "deprecation", "rawtypes" })
public class PathMergeH1Mapper extends MapReduceBase implements
        Mapper<NodeWritable, NullWritable, GraphCleanDestId, GraphCleanGenericValue> {
    PathMergeNode curNode;
    GraphCleanDestId destVertexId;
    GraphCleanNode tempNode;
    MsgListWritable msgList;

    PathMergeMsgWritable msg;
    GraphCleanGenericValue outputValue;
    int KMER_SIZE;

    @SuppressWarnings("unchecked")
    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", KMER_SIZE);
        destVertexId = new GraphCleanDestId(KMER_SIZE);
        curNode = new PathMergeNode();
        tempNode = new GraphCleanNode();
        msgList = new MsgListWritable();
        msg = new PathMergeMsgWritable();
        msgList.add(msg);
    }

    @Override
    public void map(NodeWritable key, NullWritable value,
            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output, Reporter reporter) throws IOException {
        tempNode.setAsCopy(key);
        curNode.setAsCopy(key);
        byte curVertexState = 0b00 << 0;

        if (tempNode.isTandemRepeat()) {
            tempNode.processTandemRepeatForPathMerge();
            curNode.sendBasicMsgToNeighbors(tempNode, DIR.ALL, MessageFlag.IS_HEAD, destVertexId, msgList, outputValue,
                    output);
        } else {
            if (VertexUtil.isVertexWithOnlyOneIncoming(curNode)) {
                curVertexState = State.IS_HEAD | State.SHOULD_MERGEWITHPREV;
                curNode.setState(curVertexState);
            } else if (VertexUtil.isVertexWithOnlyOneOutgoing(curNode)) {
                curVertexState = State.IS_HEAD | State.SHOULD_MERGEWITHNEXT;
                curNode.setState(curVertexState);
            }

            if (VertexUtil.isVertexWithManyIncoming(curNode)) {
                curNode.sendBasicMsgToNeighbors(curNode, DIR.PREVIOUS, MessageFlag.IS_HEAD, destVertexId, msgList,
                        outputValue, output);
            } else if (VertexUtil.isVertexWithManyOutgoing(curNode)) {
                curNode.sendBasicMsgToNeighbors(curNode, DIR.NEXT, MessageFlag.IS_HEAD, destVertexId, msgList,
                        outputValue, output);
            }
        }
        msgList.get(0).reset();
        outputValue.set(curNode);
        destVertexId.setAsCopy(curNode.getInternalKmer());
        output.collect(destVertexId, outputValue);
    }
}
