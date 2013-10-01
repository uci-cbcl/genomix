package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h4;

import java.io.IOException;
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
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

@SuppressWarnings("deprecation")
public class MergePathH4ThirdReducer extends MapReduceBase implements
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
    
    public void receiveMerges(GraphCleanDestId curNodeId, PathMergeNode curNode, Iterator<GraphCleanGenericValue> values) {
//        VertexValueWritable vertex = getVertexValue();
        NodeWritable node = curNode.getNode();
        byte state = curNode.getState();
        boolean updated = false;
        byte senderDir;
        int numMerged = 0;
        while (values.hasNext()) {
            incomingMsgList.setAsCopy((MsgListWritable) values.next().get());
            senderDir = (byte) (incomingMsg.getFlag() & DirectionFlag.DIR_MASK);
            node.mergeWithNode(senderDir, incomingMsg.getNode());
            state |= (byte) (incomingMsg.getFlag() & DIR.MASK);  // update incoming restricted directions
            numMerged++;
            updated = true;
        }
        if(curNode.isTandemRepeat(curNodeId, curNode)) {
            // tandem repeats can't merge anymore; restrict all future merges
            state |= DIR.NEXT.get();
            state |= DIR.PREVIOUS.get();
            updated = true;
//          updateStatisticsCounter(StatisticsCounter.Num_Cycles); 
        }
//      updateStatisticsCounter(StatisticsCounter.Num_MergedNodes);
//      getVertexValue().setCounters(counters);
        if (updated) {
            curNode.setState(state);
//            if (DIR.enumSetFromByte(state).containsAll(EnumSet.allOf(DIR.class)))
//                voteToHalt();
//            else 
//                activate();
            //TODO
        }
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
    }
}
