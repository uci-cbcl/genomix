package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.BubbleMergeWithSearchVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.message.BubbleMergeWithSearchMessage;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

/**
 * Graph clean pattern: Bubble Merge With Search
 * Ex.      B - C - D - E - F - G  
 *      A - B - H ------------- G
 * From this example, CDEF and H are formed bubble and B and G are two end points of this bubble.
 * For this operator,
 *     1. it starts BFSearching from start point B, and then it will collect all the valid paths(BCDEFG and BHG) to B
 *     2. compare all the valid paths with its own startHeads and pick one path which is enough similar so that we 
 *     have confidence to say this path is the correct path and others are noice
 *     3. keep this similar path and merge the path nodes in B
 *     4. delete all path nodes and the corresponding edges with these path nodes 
 */
public class BubbleMergeWithSearchVertex extends
        DeBruijnGraphCleanVertex<BubbleMergeWithSearchVertexValueWritable, BubbleMergeWithSearchMessage> {

    //    private static final Logger LOG = Logger.getLogger(BubbleMergeWithSearchVertex.class.getName());

    private Integer MAX_BFS_LENGTH = -1;

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (MAX_BFS_LENGTH < 0)
            MAX_BFS_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.BUBBLE_MERGE_WITH_SEARCH_MAX_LENGTH));
        if (outgoingMsg == null)
            outgoingMsg = new BubbleMergeWithSearchMessage();
        StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    public void beginBFS() {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();
        int internalKmerLength = vertex.getInternalKmer().getKmerLetterLength();
        if (internalKmerLength > MAX_BFS_LENGTH)
            return;

        outgoingMsg.reset();

        // update numBranches
        vertex.setNumBranches(vertex.outDegree());

        // update pathList
        VKmerList pathList = new VKmerList();
        pathList.append(getVertexId());
        outgoingMsg.setPathList(pathList);

        // update internalKmer
        VKmer internalKmer = new VKmer();
        internalKmer.setAsCopy(vertex.getInternalKmer());
        outgoingMsg.setInternalKmer(internalKmer);

        // update preKmerLength
        outgoingMsg.setPreKmerLength(internalKmer.getKmerLetterLength());

        outgoingMsg.setFlag(State.UPDATE_PATH_IN_NEXT);
        for (EDGETYPE et : EDGETYPE.OUTGOING) {
            for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                EdgeTypeList edgeTypeList = new EdgeTypeList();
                edgeTypeList.add(et);
                outgoingMsg.setEdgeTypeList(edgeTypeList);
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    public void continueBFS(Iterator<BubbleMergeWithSearchMessage> msgIterator) {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();
        while (msgIterator.hasNext()) {
            BubbleMergeWithSearchMessage incomingMsg = msgIterator.next();

            // get msg flag
            byte flag = (byte) (incomingMsg.getFlag() & State.BUBBLE_WITH_SEARCH_FLAG_MASK);
            
            // different operators for different message flags
            if (flag == State.UPDATE_PATH_IN_NEXT) {
                updatePathInNextNode(incomingMsg);
            } else if (flag == State.UPDATE_BRANCH_IN_SRC) {
                // update numBranches in src
                vertex.setNumBranches(vertex.getNumBranches() + incomingMsg.getNumBranches() - 1);
            } else if (flag == State.END_NOTICE_IN_SRC) {
                receiveEndInSource(incomingMsg);
            } else if (flag == State.KILL_MESSAGE_FROM_SOURCE) {
                sendKillMsgToPathNodes(incomingMsg);
            } else if (flag == State.PRUNE_DEAD_EDGE) {
                EDGETYPE meToNeighborEdgetype = EDGETYPE.fromByte(incomingMsg.getFlag());
                vertex.getEdgeMap(meToNeighborEdgetype).remove(incomingMsg.getSourceVertexId());
            }
        }
    }

    public void updatePathInNextNode(BubbleMergeWithSearchMessage incomingMsg) {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();
        int internalKmerLength = vertex.getInternalKmer().getKmerLetterLength();
        VKmer source = incomingMsg.getPathList().getPosition(0);
        int newLength = internalKmerLength + incomingMsg.getPreKmerLength() - kmerSize + 1;
        if (newLength > MAX_BFS_LENGTH) {
            // send back to source vertex (pathList, internalKmer and setEdgeTypeList)
            outgoingMsg.reset();
            outgoingMsg.setPathList(incomingMsg.getPathList());
            outgoingMsg.setInternalKmer(incomingMsg.getInternalKmer());
            outgoingMsg.setEdgeTypeList(incomingMsg.getEdgeTypeList());
            sendMsg(source, outgoingMsg);
        } else {
            outgoingMsg.reset();

            // update pathList
            VKmerList pathList = incomingMsg.getPathList();
            pathList.append(getVertexId());
            outgoingMsg.setPathList(pathList);

            // update internalKmer
            EdgeTypeList edgeTypes = incomingMsg.getEdgeTypeList();
            int size = edgeTypes.size();
            VKmer internalKmer = incomingMsg.getInternalKmer();
            internalKmer.mergeWithKmerInDir(edgeTypes.get(size - 1), kmerSize, vertex.getInternalKmer());
            outgoingMsg.setInternalKmer(internalKmer);

            // update preKmerLength
            outgoingMsg.setPreKmerLength(newLength);

            // if numBranches == 0, send end to source
            if (vertex.outDegree() == 0) {
                outgoingMsg.setEdgeTypeList(incomingMsg.getEdgeTypeList());
                outgoingMsg.setFlag(State.END_NOTICE_IN_SRC);
                sendMsg(source, outgoingMsg);
                return;
            }
            // if numBranches > 1, update numBranches
            if (vertex.outDegree() > 1) {
                outgoingMsg.setFlag(State.UPDATE_BRANCH_IN_SRC);
                outgoingMsg.setNumBranches(vertex.outDegree());
                sendMsg(source, outgoingMsg);
            }

            // send to next 
            EDGETYPE preToMe = incomingMsg.getEdgeTypeList().get(incomingMsg.getEdgeTypeList().size() - 1);
            for (EDGETYPE et : preToMe.mirror().dir().mirror().edgeTypes()) {
                for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                    // set flag and source vertex
                    outFlag |= State.UPDATE_PATH_IN_NEXT;
                    outFlag &= EDGETYPE.CLEAR;
                    outFlag |= et.mirror().get();
                    outgoingMsg.setFlag(outFlag);
                    outgoingMsg.setSourceVertexId(getVertexId());

                    // update edgeTypes
                    EdgeTypeList tmp = (EdgeTypeList) edgeTypes.clone();
                    tmp.add(et);
                    outgoingMsg.setEdgeTypeList(tmp);

                    sendMsg(dest, outgoingMsg);
                }
            }
        }
    }

    public void receiveEndInSource(BubbleMergeWithSearchMessage incomingMsg) {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();
        // update pathList
        vertex.getArrayOfPathList().add(incomingMsg.getPathList());

        // update internalKmer
        vertex.getArrayOfInternalKmer().add(incomingMsg.getInternalKmer());

        // update edgeTypeList
        vertex.getArrayOfEdgeTypes().add(incomingMsg.getEdgeTypeList());

        // update numBranches
        int numBranches = vertex.getNumBranches();
        numBranches--;
        vertex.setNumBranches(numBranches);
        if (numBranches == 0) {
            /* process in src */
            // step1: figure out which path to keep
            int k = 1; // FIXME compare similarity with startHead and get the most possible path, here for test, using 1
            VKmerList pathList = vertex.getArrayOfPathList().get(k);
            VKmer mergeKmer = vertex.getArrayOfInternalKmer().get(k);
            EdgeTypeList edgeTypes = vertex.getArrayOfEdgeTypes().get(k);

            // step2: replace internalKmer with mergeKmer and clear edges towards path nodes
            vertex.setInternalKmer(mergeKmer);
            vertex.getEdgeMap(vertex.getArrayOfEdgeTypes().get(k).get(0)).remove(pathList.getPosition(1));

            // step3: send kill message to path nodes
            for (int i = 1; i < pathList.size(); i++) {
                VKmer dest = new VKmer(pathList.getPosition(i));

                outgoingMsg.reset();
                outgoingMsg.setFlag(State.KILL_MESSAGE_FROM_SOURCE);

                // prev stores in pathList(0)
                VKmerList kmerList = new VKmerList();
                kmerList.append(pathList.getPosition(i - 1));

                if (i + 1 < pathList.size()) {
                    // next stores in pathList(1)
                    kmerList.append(pathList.getPosition(i + 1));
                }
                outgoingMsg.setPathList(kmerList);

                // store edgeType in msg.ArrayListWritable<EDGETYPE>(0)
                outgoingMsg.getEdgeTypeList().add(edgeTypes.get(i - 1).mirror());
                if (i < edgeTypes.size())
                    outgoingMsg.getEdgeTypeList().add(edgeTypes.get(i));
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    public void sendKillMsgToPathNodes(BubbleMergeWithSearchMessage incomingMsg) {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();

        // send msg to delete edges except the path nodes
        for (EDGETYPE et : EDGETYPE.values()) {
            for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                if ((et == incomingMsg.getEdgeTypeList().get(0) && dest
                        .equals(incomingMsg.getPathList().getPosition(0)))
                        || (incomingMsg.getEdgeTypeList().size() == 2 && et == incomingMsg.getEdgeTypeList().get(1) && dest
                                .equals(incomingMsg.getPathList().getPosition(1))))
                    continue;
                outgoingMsg.reset();
                outFlag |= State.PRUNE_DEAD_EDGE;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(dest, outgoingMsg);
            }
        }
        deleteVertex(getVertexId());
    }

    @Override
    public void compute(Iterator<BubbleMergeWithSearchMessage> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            // FIXME need to add statistical method to get top 10% startHeads as begin point
            /** begin BFS in source vertices here "AAC" for test **/
            if (getVertexId().toString().equals("AAC"))
                beginBFS();
        } else if (getSuperstep() >= 2) {
            /** continue BFS **/
            continueBFS(msgIterator);
        }
        voteToHalt();
    }

}
