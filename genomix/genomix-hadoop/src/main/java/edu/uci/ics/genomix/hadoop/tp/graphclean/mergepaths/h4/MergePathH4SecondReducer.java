package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h4;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeMsgWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.MsgListWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

@SuppressWarnings("deprecation")
public class MergePathH4SecondReducer extends MapReduceBase implements
        Reducer<GraphCleanDestId, GraphCleanGenericValue, GraphCleanDestId, PathMergeNode> {
    
    PathMergeNode curNode;
    PathMergeMsgWritable incomingMsg;
    MsgListWritable incomingMsgList;
    
    public void configure(JobConf job) {
        curNode = new PathMergeNode();
        incomingMsgList = new MsgListWritable();
        incomingMsg = new PathMergeMsgWritable();
        incomingMsgList.add(incomingMsg);
    }


    @Override
    public void reduce(GraphCleanDestId key, Iterator<GraphCleanGenericValue> values,
            OutputCollector<GraphCleanDestId, PathMergeNode> output, Reporter reporter) throws IOException {
        Writable tempValue = values.next().get();
        if(tempValue instanceof PathMergeNode) {
            curNode.setAsCopy((NodeWritable) tempValue);
        } else {
                throw new IOException("the first element should be node instance not messages! ");
        }
        
        boolean updated = false;
        while (values.hasNext()) {
            incomingMsgList.setAsCopy((MsgListWritable) values.next().get());
            // remove the edge to the node that will merge elsewhere
            curNode.getEdgeList(EDGETYPE.fromByte(incomingMsg.getFlag())).remove(incomingMsg.getSourceVertexId());
            // add the node this neighbor will merge into
            for (EDGETYPE edgeType : EnumSet.allOf(EDGETYPE.class)) {
                curNode.getEdgeList(edgeType).unionUpdate(incomingMsg.getEdgeList(edgeType));
            }
            updated = true;
        }
        if (updated) {
            //TODO output node
        }
    }
}
