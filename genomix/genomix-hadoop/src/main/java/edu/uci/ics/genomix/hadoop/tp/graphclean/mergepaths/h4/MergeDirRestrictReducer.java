package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeMsgWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.type.NodeWritable;


@SuppressWarnings("deprecation")
public class MergeDirRestrictReducer extends MapReduceBase implements
        Reducer<GraphCleanDestId, GraphCleanGenericValue, GraphCleanDestId, PathMergeNode> {
    
    PathMergeNode curNode;
    PathMergeMsgWritable incomingMsg;
    
    public void configure(JobConf job) {
        curNode = new PathMergeNode();
        incomingMsg = new PathMergeMsgWritable();
    }
    
    /**
     * initiate head, rear and path node
     */
    
    @Override
    public void reduce(GraphCleanDestId key, Iterator<GraphCleanGenericValue> values,
            OutputCollector<GraphCleanDestId, PathMergeNode> output, Reporter reporter) throws IOException {
        //correspond to recieveRestrictions()
        Writable tempValue = values.next().get();
        if(tempValue instanceof PathMergeNode) {
            curNode.setAsCopy((NodeWritable) tempValue);
        } else {
                throw new IOException("the first element should be node instance not messages! ");
        }
        byte restrictedDirs = 0;  // the directions (NEXT/PREVIOUS) that I'm not allowed to merge in
        boolean updated = false;
        while (values.hasNext()) {
            incomingMsg.setAsCopy((PathMergeMsgWritable) values.next().get()); // TODO maybe not right
            restrictedDirs |= incomingMsg.getFlag();
            updated = true;
        }
        if (updated) {
            curNode.setState(restrictedDirs);
            output.collect(key, curNode);
        }
    }
    
}
