package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h4;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode.P4State;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeMsgWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.MsgListWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

@SuppressWarnings("deprecation")
public class MergePathH4ThirdMapper extends MapReduceBase implements
        Mapper<NodeWritable, NullWritable, GraphCleanDestId, GraphCleanGenericValue> {
    
    PathMergeNode curNode;
    PathMergeMsgWritable outgoingMsg;
    MsgListWritable outgoMsgList;
    
    public void configure(JobConf job) {
        curNode = new PathMergeNode();
        outgoMsgList = new MsgListWritable();
        outgoingMsg = new PathMergeMsgWritable();
        outgoMsgList.add(outgoingMsg);
    }
    
    public void broadcastMerge(GraphCleanDestId curNodeId, PathMergeNode curNode) {
//        VertexValueWritable vertex = getVertexValue();
        byte state = curNode.getState();
        if ((state & P4State.MERGE) != 0) {
            outgoingMsg.reset();
            // tell neighbor where this is coming from (so they can merge kmers and delete)
            byte mergeDir = (byte)(curNode.getState() & DirectionFlag.DIR_MASK);
            byte neighborRestrictions = DIR.fromSet(DirectionFlag.causesFlip(mergeDir) ? DIR.flipSetFromByte(state) : DIR.enumSetFromByte(state));
            
            outgoingMsg.setFlag((byte) (DirectionFlag.mirrorEdge(mergeDir) | neighborRestrictions));
            outgoingMsg.setSourceVertexId(curNodeId);
            outgoingMsg.setNode(curNode.getNode());
            if (curNode.getDegree(DirectionFlag.dirFromEdgeType(mergeDir)) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeDir + " direction != 1!\n");
            VKmerBytesWritable dest = curNode.getEdgeList(mergeDir).get(0).getKey();
//          TODO  sendMsg(dest, outgoingMsg);
            
//            deleteVertex(getVertexId());
        }
    }
    
    /*****new methods*/
    public void sendMergeMsg() {
        VertexValueWritable vertex = getVertexValue();
        short state = vertex.getState();
        if ((state & P4State.MERGE) != 0) {
            outgoingMsg.reset();
            // tell neighbor where this is coming from (so they can merge kmers and delete)
            EDGETYPE mergeEdgetype = EDGETYPE.fromByte(vertex.getState());
            byte neighborRestrictions = DIR.fromSet(mergeEdgetype.causesFlip() ? DIR.flipSetFromByte(state) : DIR
                    .enumSetFromByte(state));

            outgoingMsg.setFlag((short) (mergeEdgetype.mirror().get() | neighborRestrictions));
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setNode(vertex.getNode());
            if (vertex.getDegree(mergeEdgetype.dir()) != 1)
                throw new IllegalStateException("Merge attempted in node with degree in " + mergeEdgetype
                        + " direction != 1!\n" + vertex);
            VKmerBytesWritable dest = vertex.getEdgeList(mergeEdgetype).get(0).getKey();
            sendMsg(dest, outgoingMsg);
            deleteVertex(getVertexId());

        }
    }
    
    @Override
    public void map(NodeWritable key, NullWritable value,
            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output, Reporter reporter) throws IOException {

    }
}
