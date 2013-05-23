package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class DeepGraphBuildingReducer extends MapReduceBase implements
        Reducer<PositionWritable, PositionListAndKmerWritable, NodeWritable, NullWritable> {
    public class nodeToMergeState {
        public static final byte NOT_UPDATE = 0;
        public static final byte ASSIGNED_BY_RIGHTNODE = 1;
        private byte state;

        public nodeToMergeState() {
            state = NOT_UPDATE;
        }

        public void setToNotUpdate() {
            state = NOT_UPDATE;
        }

        public void setToAssignedByRightNode() {
            state = ASSIGNED_BY_RIGHTNODE;
        }

        public String getState() {
            switch (state) {
                case NOT_UPDATE:
                    return "NOT_UPDATE";
                case ASSIGNED_BY_RIGHTNODE:
                    return "ASSIGNED_BY_RIGHTNODE";
            }
            return "ERROR_STATE";
        }
    }

    private PositionListAndKmerWritable nodeListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable nodeSuccListAndKmer = new PositionListAndKmerWritable();
    private NodeWritable startNodeInRead = new NodeWritable();
    private NodeWritable nodeToMerge = new NodeWritable();
    private NodeWritable nodeToBeMerged = new NodeWritable();
    private PositionListWritable incomingList = new PositionListWritable();
    private PositionListWritable outgoingList = new PositionListWritable();
    private NullWritable nullWritable = NullWritable.get();
    private nodeToMergeState state = new nodeToMergeState();

    @Override
    public void reduce(PositionWritable key, Iterator<PositionListAndKmerWritable> values,
            OutputCollector<NodeWritable, NullWritable> output, Reporter reporter) throws IOException {
        int readID = key.getReadID();
        byte posInRead = 0;
        assembleNodeInitialization(readID, posInRead, values);
        posInRead = (byte) (posInRead + 2);
        //----LOOP
        while (values.hasNext()) {
            assembleNodeFromValues(readID, posInRead, values);
            posInRead = (byte) (posInRead + 1);
            if (nodeToMerge.existsInSinglePath() == true && nodeToBeMerged.existsInSinglePath() == true) {
                nodeToMerge.mergeNextWithinOneRead(nodeToBeMerged);
                state.setToNotUpdate();
            }
            else {
                state.setToAssignedByRightNode();
                output.collect(nodeToMerge, nullWritable);
            }
        }
    }

    public void assembleNodeFromValues(int readID, byte posInRead, Iterator<PositionListAndKmerWritable> values)
            throws IOException {
        if (values.hasNext()) {
            nodeListAndKmer.set(values.next());
        } else {
            throw new IOException("the size of values emerge bug!");
        }
        if (state.getState().equals("ASSIGNED_BY_RIGHTNODE")) {
            nodeToMerge.set(nodeToBeMerged);
        }
        incomingList.reset();
        incomingList.append(readID, (byte) (posInRead - 1));
        if (nodeSuccListAndKmer.getVertexIDList() != null)
            outgoingList.set(nodeSuccListAndKmer.getVertexIDList());
        outgoingList.append(readID, (byte) (posInRead + 1));
        nodeToBeMerged.setNodeID(readID, (byte) posInRead);
        nodeToBeMerged.setIncomingList(incomingList);
        nodeToBeMerged.setOutgoingList(outgoingList);
        nodeToBeMerged.setKmer(nodeListAndKmer.getKmer());
    }

    public void assembleNodeInitialization(int readID, byte posInRead, Iterator<PositionListAndKmerWritable> values)
            throws IOException {
        if (values.hasNext()) {
            nodeListAndKmer.set(values.next());
        } else {
            throw new IOException("the size of values emerge bug!");
        }
        if (values.hasNext()) {
            nodeSuccListAndKmer.set(values.next());
        }
        incomingList.reset();
        incomingList.set(nodeSuccListAndKmer.getVertexIDList());
        outgoingList.reset();
        if (nodeSuccListAndKmer.getVertexIDList() != null)
            outgoingList.set(nodeSuccListAndKmer.getVertexIDList());
        outgoingList.append(readID, (byte) (posInRead + 1));
        startNodeInRead.setNodeID(readID, posInRead);
        startNodeInRead.setIncomingList(incomingList);
        startNodeInRead.setOutgoingList(outgoingList);
        startNodeInRead.setKmer(nodeListAndKmer.getKmer());
        //---------
        nodeListAndKmer.set(nodeSuccListAndKmer);
        incomingList.reset();
        incomingList.append(readID, posInRead);
        if (values.hasNext()) {
            nodeSuccListAndKmer.set(values.next());
            if (nodeSuccListAndKmer.getVertexIDList() != null)
                outgoingList.set(nodeSuccListAndKmer.getVertexIDList());
        }
        outgoingList.append(readID, (byte) (posInRead + 2));
        nodeToMerge.setNodeID(readID, (byte) posInRead);
        nodeToMerge.setIncomingList(incomingList);
        nodeToMerge.setOutgoingList(outgoingList);
        nodeToMerge.setKmer(nodeListAndKmer.getKmer());
        state.setToNotUpdate();
    }
}
