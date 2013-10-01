package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h4;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeMsgWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

@SuppressWarnings("deprecation")
public class MergeDirRestrictMapper extends MapReduceBase implements
        Mapper<VKmerBytesWritable, NodeWritable, GraphCleanDestId, GraphCleanGenericValue> {
    
    PathMergeNode curNode;
    GraphCleanDestId curNodeVertexId;
    GraphCleanDestId destVertexId;
    PathMergeMsgWritable msg;
    GraphCleanGenericValue outputValue;
    int KMER_SIZE;

    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", KMER_SIZE);
        curNodeVertexId = new GraphCleanDestId(KMER_SIZE);
        destVertexId = new GraphCleanDestId(KMER_SIZE);
        curNode = new PathMergeNode();
        msg = new PathMergeMsgWritable();
        outputValue = new GraphCleanGenericValue();
    }
    
    public void sendMsg(GraphCleanDestId key, GraphCleanGenericValue outputValue,
            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output) throws IOException {
        output.collect(key, outputValue);
    }
    
    @Override
    public void map(VKmerBytesWritable key, NodeWritable value,
            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output, Reporter reporter) throws IOException {
        curNode.setAsCopy(value);
        curNodeVertexId.setAsCopy(key);
        curNodeVertexId.setSecSStampFirstOrder(); // for secondary sort before reduce phase
        //correspond to restrictNeighbors()
        //TODO genomix-pregelix, this method is put into BasicPathMergeVertex.java
        byte state = curNode.getState();
        EnumSet<DIR> dirsToRestrict;
        boolean updated = false;
        if (curNode.isTandemRepeat(curNodeVertexId, curNode)) {
            // tandem repeats are not allowed to merge at all
            dirsToRestrict = EnumSet.of(DIR.FORWARD, DIR.REVERSE);
            state |= DIR.FORWARD.get();
            state |= DIR.REVERSE.get();
            updated = true;
        } else {
            // degree > 1 can't merge in that direction; == 0 means we are a tip 
            dirsToRestrict = EnumSet.noneOf(DIR.class);
            for (DIR dir : DIR.values()) {
                if (curNode.getDegree(dir) > 1 || curNode.getDegree(dir) == 0) {
                    dirsToRestrict.add(dir);
                    state |= dir.get();
                    updated = true;
                }
            }
        }
        if (updated) {
            curNode.setState(state);
            outputValue.set(curNode);
            sendMsg(curNodeVertexId, outputValue, output);
        }
        // send a message to each neighbor indicating they can't merge towards me
        for (DIR dir : dirsToRestrict) {
            for (EDGETYPE et : dir.edgeType()) {
                for (VKmerBytesWritable destId : curNode.getEdgeList(et).getKeys()) {
                    destVertexId.setAsCopy(destId);
                    destVertexId.setSecSStampSecOrder();// for secondary sort before reduce phase
                    msg.setFlag(et.mirror().dir().get());
//                    msg.setSourceVertexId(key);//FIXME only for test which assign the sourcevertexId, after running normally, cancel this!
                    outputValue.set(msg);
                    sendMsg(destVertexId, outputValue, output);
                }
            }
        }
    }
}
