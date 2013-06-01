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

    private PositionListAndKmerWritable curNodePosiListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable curNodeNegaListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable nextNodePosiListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable nextNodeNegaListAndKmer = new PositionListAndKmerWritable();

    private NodeWritable curNode = new NodeWritable();
    private NodeWritable nextNode = new NodeWritable();
    private NodeWritable nextNextNode = new NodeWritable();
    private PositionListWritable incomingList = new PositionListWritable();
    private PositionListWritable outgoingList = new PositionListWritable();
    private NullWritable nullWritable = NullWritable.get();
    private int KMER_SIZE;
    private int LAST_POSID;
    private int READ_LENGTH;

    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
        READ_LENGTH = Integer.parseInt(job.get("readLength"));
        LAST_POSID = READ_LENGTH - KMER_SIZE + 1;
    }

    @Override
    public void reduce(PositionWritable key, Iterator<PositionListAndKmerWritable> values,
            OutputCollector<NodeWritable, NullWritable> output, Reporter reporter) throws IOException {
        int readID = key.getReadID();
        byte posInRead = 0;
        assembleFirstTwoNodesInRead(readID, posInRead, curNode, nextNode, values);
        assembleNodeFromValues(readID, posInRead, curNode, nextNode, values);
        posInRead ++;
        while(values.hasNext()){
            assembleNodeFromValues(readID, posInRead, curNode, nextNextNode, values);

            if (curNode.inDegree() > 1 || curNode.outDegree() > 0 || nextNode.inDegree() > 0
                    || nextNode.outDegree() > 0 || nextNextNode.inDegree() > 0
                    || nextNextNode.outDegree() > 0) {
                connect(curNode, nextNextNode);
                output.collect(curNode, nullWritable);
                curNode.set(nextNode);
                nextNode.set(nextNode);
                continue;
            }
            curNode.mergeForwadNext(nextNode, KMER_SIZE);
            nextNode.set(nextNextNode);
        }
    }

    public void assembleNodeFromValues(int readID, byte posInRead, NodeWritable curNode, NodeWritable nextNode, Iterator<PositionListAndKmerWritable> values)
            throws IOException {
        curNodePosiListAndKmer.set(nextNodePosiListAndKmer);
        curNodeNegaListAndKmer.set(nextNodeNegaListAndKmer);
        if (values.hasNext()) {
            nextNodePosiListAndKmer.set(values.next());
            if(values.hasNext()) {
                nextNodeNegaListAndKmer.set(values.next());
            }
            else {
                throw new IOException("lose the paired kmer");
            }
        }
        
        outgoingList.reset();
        outgoingList.set(nextNodePosiListAndKmer.getVertexIDList());
        setForwardOutgoingList(curNode, outgoingList);
        
        nextNode.setNodeID(readID, (byte)(posInRead + 1));
        nextNode.setKmer(nextNodePosiListAndKmer.getKmer());
        
        outgoingList.reset();
        outgoingList.set(curNodeNegaListAndKmer.getVertexIDList());
        setReverseOutgoingList(nextNode, outgoingList);
        
        if (nextNode.getNodeID().getPosInRead() == LAST_POSID) {
            incomingList.reset();
            incomingList.set(nextNodeNegaListAndKmer.getVertexIDList());
            setReverseIncomingList(nextNode, incomingList);
        }
    }

    public void assembleFirstTwoNodesInRead(int readID, byte posInRead, NodeWritable curNode, NodeWritable nextNode, Iterator<PositionListAndKmerWritable> values)
            throws IOException {
        nextNodePosiListAndKmer.set(values.next());
        if (values.hasNext()) {
            nextNodeNegaListAndKmer.set(values.next());
        } else {
            throw new IOException("lose the paired kmer");
        }
        
        if (curNode.getNodeID().getPosInRead() == LAST_POSID) {
            incomingList.reset();
            incomingList.set(nextNodeNegaListAndKmer.getVertexIDList());
            setReverseIncomingList(curNode, incomingList);
        }
        incomingList.reset();
        incomingList.set(nextNodePosiListAndKmer.getVertexIDList());
        
        curNode.setNodeID(readID, posInRead);
        curNode.setKmer(curNodePosiListAndKmer.getKmer());
        setForwardIncomingList(curNode, incomingList);
    }

    private void setForwardOutgoingList(NodeWritable node, PositionListWritable posList) {
        for (PositionWritable pos : posList) {
            if (pos.getPosInRead() > 0) {
                node.getFFList().append(pos);
            } else {
                node.getFRList().append(pos.getReadID(), (byte) -pos.getPosInRead());
            }
        }
    }

    private void setForwardIncomingList(NodeWritable node, PositionListWritable posList) {
        for (PositionWritable pos : posList) {
            if (pos.getPosInRead() > 0) {
                if (pos.getPosInRead() > 1) {
                    node.getRRList().append(pos.getReadID(), (byte) (pos.getPosInRead() - 1));
                } else {
                    throw new IllegalArgumentException("position id is invalid");
                }
            } else {
                if (pos.getPosInRead() > -LAST_POSID) {
                    node.getRFList().append(pos.getReadID(), (byte) -(pos.getPosInRead() - 1));
                }
            }
        }
    }

    private void setReverseOutgoingList(NodeWritable node, PositionListWritable posList) {
        for (PositionWritable pos : posList) {
            if (pos.getPosInRead() > 0) {
                node.getRFList().append(pos);
            } else {
                node.getRRList().append(pos.getReadID(), (byte) -pos.getPosInRead());
            }
        }
    }

    private void setReverseIncomingList(NodeWritable node, PositionListWritable posList) {
        for (PositionWritable pos : posList) {
            if (pos.getPosInRead() > 0) {
                if (pos.getPosInRead() > 1) {
                    node.getFRList().append(pos.getReadID(), (byte) (pos.getPosInRead() - 1));
                } else {
                    throw new IllegalArgumentException("Invalid position");
                }
            } else {
                if (pos.getPosInRead() > -LAST_POSID) {
                    node.getFFList().append(pos.getReadID(), (byte) -(pos.getPosInRead() - 1));
                }
            }
        }
    }
    
    private void connect(NodeWritable curNode, NodeWritable nextNode) {
        curNode.getFFList().append(nextNode.getNodeID());
        nextNode.getRRList().append(curNode.getNodeID());
    }
}
