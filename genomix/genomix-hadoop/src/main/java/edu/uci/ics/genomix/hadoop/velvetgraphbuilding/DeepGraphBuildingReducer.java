package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.hadoop.oldtype.VKmerBytesWritable;
import edu.uci.ics.genomix.hadoop.oldtype.VKmerBytesWritableFactory;
import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class DeepGraphBuildingReducer extends MapReduceBase implements
        Reducer<PositionWritable, PositionListAndKmerWritable, NodeWritable, NullWritable> {

    private PositionListAndKmerWritable nodeListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable nodeSuccListAndKmer = new PositionListAndKmerWritable();
    private NodeWritable startNodeInRead = new NodeWritable();
    private NodeWritable nodeToMerge = new NodeWritable();
    private NodeWritable nodeToBeMerged = new NodeWritable();
    private PositionListWritable incomingList = new PositionListWritable();
    private PositionListWritable outgoingList = new PositionListWritable();
    private NullWritable nullWritable = NullWritable.get();
    private int KMER_SIZE;
    
    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", 0);
    }
    public enum nodeToMergeState {
        SRC_UPDATE_FROM_VALUES,
        SRC_NODE_NON_UPDATE,
        SRC_ASSIGNED_BY_RIGHTNODE;
    }

    @Override
    public void reduce(PositionWritable key, Iterator<PositionListAndKmerWritable> values,
            OutputCollector<NodeWritable, NullWritable> output, Reporter reporter) throws IOException {
        //initialize the Start point in Read
        int readID = key.getReadID();
        byte posInRead = 0;
        if (values.hasNext()) {
            nodeListAndKmer.set(values.next());
            incomingList.set(nodeSuccListAndKmer.getVertexIDList());
        }
        if (values.hasNext()) {
            nodeSuccListAndKmer.set(values.next());
            if (nodeSuccListAndKmer.getVertexIDList() != null)
                outgoingList.set(nodeSuccListAndKmer.getVertexIDList());
        }
        outgoingList.append(readID, (byte) (posInRead + 1));
        startNodeInRead.setNodeID(readID, posInRead);
        startNodeInRead.setIncomingList(incomingList);
        startNodeInRead.setOutgoingList(outgoingList);
        startNodeInRead.setKmer(nodeListAndKmer.getKmer());
        output.collect(startNodeInRead, nullWritable);
        posInRead++;
        //----initialize the nodeToMerge
        nodeListAndKmer.set(nodeSuccListAndKmer);
        incomingList.reset();
        incomingList.append(key.getReadID(), key.getPosInRead());
        if (values.hasNext()) {
            nodeSuccListAndKmer.set(values.next());
            if (nodeSuccListAndKmer.getVertexIDList() != null)
                outgoingList.set(nodeSuccListAndKmer.getVertexIDList());
        }
        outgoingList.append(readID, (byte) (posInRead + 1));
        nodeToMerge.setNodeID(readID, (byte) posInRead);
        nodeToMerge.setIncomingList(incomingList);
        nodeToMerge.setOutgoingList(outgoingList);
        nodeToMerge.setKmer(nodeListAndKmer.getKmer());
        posInRead++;
        //----LOOP
        nodeToMergeState srcState = nodeToMergeState.SRC_NODE_NON_UPDATE;
        boolean srcOrTarget = true;
        while (values.hasNext()) {
            switch (srcState.toString()) {
                case "SRC_UPDATE_FROM_VALUES":
                    srcOrTarget = true;
                    getNodeFromValues(readID, posInRead, values, srcOrTarget);
                    posInRead++;
                    srcOrTarget = false;
                    getNodeFromValues(readID, posInRead, values, srcOrTarget);
                    posInRead++;
                    break;
                case "SRC_NODE_NON_UPDATE":
                    srcOrTarget = false;
                    getNodeFromValues(readID, posInRead, values, srcOrTarget);
                    posInRead++;
                    break;
                case "SRC_ASSIGNED_BY_RIGHTNODE":
                    nodeToMerge.set(nodeToBeMerged);
                    srcOrTarget = false;
                    getNodeFromValues(readID, posInRead, values, srcOrTarget);
                    posInRead++;
                    break;
            }
            if(nodeToMerge.isPathNode() == true && nodeToBeMerged.isPathNode() == true){
                nodeToMerge.mergeNext(nodeToBeMerged, KMER_SIZE);
                srcState = nodeToMergeState.SRC_NODE_NON_UPDATE;
            }
            else{
                if(nodeToMerge.isPathNode() == false && nodeToBeMerged.isPathNode() == true){
                    
                    srcState = nodeToMergeState.SRC_ASSIGNED_BY_RIGHTNODE;
                    output.collect(nodeToBeMerged, nullWritable);
                }
                else {
                    srcState = nodeToMergeState.SRC_UPDATE_FROM_VALUES;
                    output.collect(nodeToMerge, nullWritable);
                    output.collect(nodeToBeMerged, nullWritable);
                }
            }

        }
    }

    public void getNodeFromValues(int readID, byte posInRead, Iterator<PositionListAndKmerWritable> values,
            boolean srcOrTarget) {
        if (values.hasNext()) {
            nodeListAndKmer.set(values.next());
            incomingList.reset();
            incomingList.append(readID, (byte) (posInRead - 1));
        }
        if (values.hasNext()) {
            nodeSuccListAndKmer.set(values.next());
            if (nodeSuccListAndKmer.getVertexIDList() != null)
                outgoingList.set(nodeSuccListAndKmer.getVertexIDList());
        }
        outgoingList.append(readID, (byte) (posInRead + 1));
        if (srcOrTarget == true) {
            nodeToMerge.setNodeID(readID, (byte) posInRead);
            nodeToMerge.setIncomingList(incomingList);
            nodeToMerge.setOutgoingList(outgoingList);
            nodeToMerge.setKmer(nodeListAndKmer.getKmer());
        } else {
            nodeToBeMerged.setNodeID(readID, (byte) posInRead);
            nodeToBeMerged.setIncomingList(incomingList);
            nodeToBeMerged.setOutgoingList(outgoingList);
            nodeToBeMerged.setKmer(nodeListAndKmer.getKmer());
        }
    }
}
