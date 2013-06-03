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
    private PositionListAndKmerWritable curNodePosiListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable curNodeNegaListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable nextNodePosiListAndKmer = new PositionListAndKmerWritable();
    private PositionListAndKmerWritable nextNodeNegaListAndKmer = new PositionListAndKmerWritable();

    private NodeWritable curNode;
    private NodeWritable nextNode;
    private NodeWritable nextNextNode;
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
        curNode = new NodeWritable(KMER_SIZE);
        nextNode = new NodeWritable(KMER_SIZE);
        nextNextNode = new NodeWritable(KMER_SIZE);
    }

    @Override
    public void reduce(PositionWritable key, Iterator<PositionListAndKmerWritable> values,
            OutputCollector<NodeWritable, NullWritable> output, Reporter reporter) throws IOException {
        int readID = key.getReadID();
        if(readID == 1) {
            int x = 4;
            int y =x ;
            System.out.println((int)key.getPosInRead());
        }
/*        while(values.hasNext()) {
            System.out.println(values.next().getKmer().toString());
        }*/
        byte posInRead = (byte) 1;
        resetNode(curNode, readID, posInRead);
        assembleFirstTwoNodesInRead(curNodePosiListAndKmer, nextNodePosiListAndKmer, nextNodeNegaListAndKmer,
                incomingList, outgoingList, curNode, nextNode, values);
        posInRead++;
        assembleNodeFromValues(readID, posInRead, curNodePosiListAndKmer, curNodeNegaListAndKmer,
                nextNodePosiListAndKmer, nextNodeNegaListAndKmer, incomingList, outgoingList, curNode, nextNode, values);
        posInRead++;
        boolean flag = true;
        while (flag) {
            flag = assembleNodeFromValues(readID, posInRead, curNodePosiListAndKmer, curNodeNegaListAndKmer,
                    nextNodePosiListAndKmer, nextNodeNegaListAndKmer, incomingList, outgoingList, nextNode,
                    nextNextNode, values);
            posInRead++;
            if (curNode.inDegree() > 1 || curNode.outDegree() > 0 || nextNode.inDegree() > 0
                    || nextNode.outDegree() > 0 || nextNextNode.inDegree() > 0 || nextNextNode.outDegree() > 0) {
                connect(curNode, nextNode);
                output.collect(curNode, nullWritable);
                curNode.set(nextNode);
                nextNode.set(nextNextNode);
                continue;
            }
            curNode.mergeForwadNext(nextNode, KMER_SIZE);
            nextNode.set(nextNextNode);
        }
        output.collect(curNode, nullWritable);
    }

    public boolean assembleNodeFromValues(int readID, byte posInRead, PositionListAndKmerWritable curNodePosiListAndKmer,
            PositionListAndKmerWritable curNodeNegaListAndKmer, PositionListAndKmerWritable nextNodePosiListAndKmer,
            PositionListAndKmerWritable nextNodeNegaListAndKmer, PositionListWritable outgoingList,
            PositionListWritable incomingList, NodeWritable curNode, NodeWritable nextNode,
            Iterator<PositionListAndKmerWritable> values) throws IOException {
        boolean flag = true;
        curNodePosiListAndKmer.set(nextNodePosiListAndKmer);
        curNodeNegaListAndKmer.set(nextNodeNegaListAndKmer);
        if (values.hasNext()) {
            nextNodeNegaListAndKmer.set(values.next());
            if (values.hasNext()) {
                nextNodePosiListAndKmer.set(values.next());
            } else {
                throw new IOException("lose the paired kmer from values");
            }
            outgoingList.reset();
            outgoingList.set(nextNodePosiListAndKmer.getVertexIDList());
            setForwardOutgoingList(curNode, outgoingList);

            resetNode(nextNode, readID, posInRead);
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
        else{
            flag = false;
            resetNode(nextNode, readID, (byte)0);
        }
        return flag;
    }

    public void assembleFirstTwoNodesInRead(PositionListAndKmerWritable curNodePosiListAndKmer,
            PositionListAndKmerWritable nextNodePosiListAndKmer, PositionListAndKmerWritable nextNodeNegaListAndKmer,
            PositionListWritable outgoingList, PositionListWritable incomingList, NodeWritable curNode,
            NodeWritable nextNode, Iterator<PositionListAndKmerWritable> values) throws IOException {
        nextNodeNegaListAndKmer.set(values.next());
        if (values.hasNext()) {
            nextNodePosiListAndKmer.set(values.next());
        } else {
            System.out.println(curNode.getNodeID().getReadID());
            throw new IOException("lose the paired kmer from first two nodes");
        }

        if (curNode.getNodeID().getPosInRead() == LAST_POSID) {
            incomingList.reset();
            incomingList.set(nextNodeNegaListAndKmer.getVertexIDList());
            setReverseIncomingList(curNode, incomingList);
        }
        incomingList.reset();
        incomingList.set(nextNodePosiListAndKmer.getVertexIDList());

        
        curNode.setKmer(nextNodePosiListAndKmer.getKmer());
        setForwardIncomingList(curNode, incomingList);
    }

    private void resetNode(NodeWritable node, int readID, byte posInRead) {
        node.reset(KMER_SIZE);
        node.setNodeID(readID, posInRead);
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

    private void connect(NodeWritable curNode, NodeWritable nextNode) {
        curNode.getFFList().append(nextNode.getNodeID());
        nextNode.getRRList().append(curNode.getNodeID());
    }
}
