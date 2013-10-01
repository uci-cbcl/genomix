package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h4;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeNode.P4State;
import edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1.PathMergeMsgWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanDestId;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanGenericValue;
import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.MsgListWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.MessageFlag;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

@SuppressWarnings("deprecation")
public class UpdateNeighborsAndMergeDirMapper extends MapReduceBase implements
        Mapper<GraphCleanDestId, PathMergeNode, GraphCleanDestId, GraphCleanGenericValue> {

    private Random randGenerator = null;
    private static long randSeed = 1; //static for save memory
    private float probBeingRandomHead = -1;
    private int numOfJobStep;

    private VKmerBytesWritable curKmer = new VKmerBytesWritable();
    private VKmerBytesWritable nextKmer = new VKmerBytesWritable();
    private VKmerBytesWritable prevKmer = new VKmerBytesWritable();
    private boolean hasNext;
    private boolean hasPrev;
    private boolean curHead;
    private boolean nextHead;
    private boolean prevHead;
    private byte nextDir;
    private byte prevDir;
    private EDGETYPE nextEdgetype;
    private EDGETYPE prevEdgetype;

    PathMergeNode curNode;
    PathMergeMsgWritable outgoingMsg;

    public void configure(JobConf job) {
        numOfJobStep = job.getInt("numOfJobStep", numOfJobStep);
        if (randGenerator == null)
            randSeed = Long.parseLong(job.get(GenomixJobConf.PATHMERGE_RANDOM_RANDSEED));
        if (probBeingRandomHead < 0)
            probBeingRandomHead = Float.parseFloat(job.get(GenomixJobConf.PATHMERGE_RANDOM_PROB_BEING_RANDOM_HEAD));
        curNode = new PathMergeNode();
        outgoingMsg = new PathMergeMsgWritable();
    }

    public void sendMsg(GraphCleanDestId key, GraphCleanGenericValue outputValue,
            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output) throws IOException {
        output.collect(key, outputValue);
    }

    protected void checkNeighbors(PathMergeNode node) {
        EnumSet<DIR> restrictedDirs = DIR.enumSetFromByte(node.getState());
        // NEXT restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.NEXT) || node.outDegree() != 1) { // TODO should I restrict based on degree in the first iteration?
            hasNext = false;
        } else {
            hasNext = true;
            nextEdgetype = node.getEdgetypeFromDir(DIR.NEXT);
            nextKmer = node.getEdgeList(nextEdgetype).get(0).getKey();
            nextHead = isNodeRandomHead(nextKmer);
        }

        // PREVIOUS restricted by neighbor or by my edges? 
        if (restrictedDirs.contains(DIR.PREVIOUS) || node.inDegree() != 1) {
            hasPrev = false;
        } else {
            hasPrev = true;
            prevEdgetype = node.getEdgetypeFromDir(DIR.PREVIOUS);;
            prevKmer = node.getEdgeList(prevEdgetype).get(0).getKey();
            prevHead = isNodeRandomHead(prevKmer);
        }
    }

    public void chooseMergeDir(GraphCleanDestId curNodeId, PathMergeNode curNode) {
        //initiate merge_dir
        curNode.setMerge(P4State.NO_MERGE);

        curKmer = curNodeId;
        curHead = isNodeRandomHead(curKmer);
        checkNeighbors(curNode);

        if (!hasNext && !hasPrev) { // TODO check if logic for previous updates is the same as here (just look at internal flags?)
        //            voteToHalt();  // this node can never merge (restricted by neighbors or my structure)
            //TODO emmit the id and nodewritable to output
        } else {
            if (curHead) {
                if (hasNext && !nextHead) {
                    // compress this head to the forward tail
                    curNode.setMerge((byte) (nextDir | P4State.MERGE));
                } else if (hasPrev && !prevHead) {
                    // compress this head to the reverse tail
                    curNode.setMerge((byte) (prevDir | P4State.MERGE));
                }
            } else {
                // I'm a tail
                if (hasNext && hasPrev) {
                    if ((!nextHead && !prevHead)
                            && (curKmer.compareTo(nextKmer) < 0 && curKmer.compareTo(prevKmer) < 0)) {
                        // tails on both sides, and I'm the "local minimum"
                        // compress me towards the tail in forward dir
                        curNode.setMerge((byte) (nextDir | P4State.MERGE));
                    }
                } else if (!hasPrev) {
                    // no previous node
                    if (!nextHead && curKmer.compareTo(nextKmer) < 0) {
                        // merge towards tail in forward dir
                        curNode.setMerge((byte) (nextDir | P4State.MERGE));
                    }
                } else if (!hasNext) {
                    // no next node
                    if (!prevHead && curKmer.compareTo(prevKmer) < 0) {
                        // merge towards tail in reverse dir
                        curNode.setMerge((byte) (prevDir | P4State.MERGE));
                    }
                }
            }
        }
    }

    public void updateNeighbors(GraphCleanDestId curNodeId, PathMergeNode curNode) {
        //        VertexValueWritable vertex = getVertexValue();
        byte state = curNode.getState();
        EDGETYPE edgeType = EDGETYPE.fromByte(state);
        if ((state & P4State.MERGE) == 0) {
            return; // no merge requested; don't have to update neighbors
            //TODO why no vote to halt?
        }

        DIR mergeDir = edgeType.dir();
        EnumSet<EDGETYPE> mergeEdges = mergeDir.edgeType();

        DIR updateDir = mergeDir.mirror();
        EnumSet<EDGETYPE> updateEdges = updateDir.edgeType();

        // prepare the update message s.t. the receiver can do a simple unionupdate
        // that means we figure out any hops and place our merge-dir edges in the appropriate list of the outgoing msg
        for (EDGETYPE updateEdge : updateEdges) {
            outgoingMsg.reset();
            outgoingMsg.setSourceVertexId(curNodeId);
            byte outFlag = 0;

            outFlag |= MessageFlag.TO_UPDATE | updateEdge.mirror().get(); // neighbor's edge to me (so he can remove me)
            for (EDGETYPE mergeEdge : mergeEdges) {
                EDGETYPE newEdgetype = EDGETYPE.resolveLinkThroughMiddleNode(updateEdge, mergeEdge);
                outgoingMsg.getNode().setEdgeList(newEdgetype, curNode.getEdgeList(mergeEdge));
            }

            // send the update to all kmers in this list // TODO perhaps we could skip all this if there are no neighbors here
            for (VKmerBytesWritable dest : curNode.getEdgeList(updateEdge).getKeys()) {
                //                LOG.info("send update message from " + getVertexId() + " to " + dest + ": " + outgoingMsg);
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    protected boolean isNodeRandomHead(VKmerBytesWritable nodeKmer) {
        // "deterministically random", based on node id
        randGenerator.setSeed((randSeed ^ nodeKmer.hashCode()) * 10000 * numOfJobStep);//randSeed + nodeID.hashCode()
        for (int i = 0; i < 500; i++)
            randGenerator.nextFloat();
        boolean isHead = randGenerator.nextFloat() < probBeingRandomHead;
        return isHead;
    }

    @Override
    public void map(GraphCleanDestId key, PathMergeNode value,
            OutputCollector<GraphCleanDestId, GraphCleanGenericValue> output, Reporter reporter) throws IOException {

    }
}
