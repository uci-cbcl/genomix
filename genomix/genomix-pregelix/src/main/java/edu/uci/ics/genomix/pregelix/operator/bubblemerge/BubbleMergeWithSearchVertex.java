package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.format.BubbleMergeWithSearchVertexToNodeOutputFormat;
import edu.uci.ics.genomix.pregelix.format.NodeToBubbleMergeWithSearchVertexInputFormat;
import edu.uci.ics.genomix.pregelix.io.BubbleMergeWithSearchVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.BubbleMergeWithSearchVertexValueWritable.BubbleMergeWithSearchState;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.message.BubbleMergeWithSearchMessage;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * Graph clean pattern: Bubble Merge With Search
 * Details: We do a breadth-first search by passing along messages. All paths are explored,
 * accumulating a long kmer that represents the sequence of that path. Each branch searches
 * up to a certain kmer distance away and the source node keeps track of how many active
 * branches there are. When there are no more active branches from this path, we compare the
 * branches to detect similar paths and collapse those dubbed both too similar and inferior
 * in some way (e.g., too low coverage).
 * Ex. B - C - D - E - F - G
 * A - B - H ------------- G
 * From this example, CDEF and H are formed bubble and B and G are two end points of this bubble.
 * For this operator,
 * 1. it starts BFSearching from start point B, and then it will collect all the valid paths(BCDEFG and BHG) to B
 * 2. compare all the valid paths with its own startHeads and pick one path which is enough similar so that we
 * have confidence to say this path is the correct path
 * 3. keep this similar path and merge the path nodes in B
 * 4. delete all path nodes and the corresponding edges with these path nodes
 */
public class BubbleMergeWithSearchVertex extends
        DeBruijnGraphCleanVertex<BubbleMergeWithSearchVertexValueWritable, BubbleMergeWithSearchMessage> {

    //    private static final Logger LOG = Logger.getLogger(BubbleMergeWithSearchVertex.class.getName());

    private static int MAX_BFS_LENGTH = -1;
    private static DIR SEARCH_DIRECTION = null;

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (MAX_BFS_LENGTH < 0)
            MAX_BFS_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.BUBBLE_MERGE_WITH_SEARCH_MAX_LENGTH));
        if (SEARCH_DIRECTION == null) {
            SEARCH_DIRECTION = DIR.valueOf(getContext().getConfiguration().get(
                    GenomixJobConf.BUBBLE_MERGE_WITH_SEARCH_SEARCH_DIRECTION));
        }
        if (outgoingMsg == null)
            outgoingMsg = new BubbleMergeWithSearchMessage();
        //        StatisticsAggregator.preGlobalCounters.clear();
        //        else
        //            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        //        counters.clear();
        //        getVertexValue().getCounters().clear();
    }

    public void beginBFS() {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();
        int internalKmerLength = vertex.getInternalKmer().getKmerLetterLength();
        if (internalKmerLength > MAX_BFS_LENGTH)
            return;

        // set vertex.state = SEARCH_THROUGH
        vertex.setState(BubbleMergeWithSearchState.SEARCH_THROUGH);

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

        outgoingMsg.setFlag(BubbleMergeWithSearchState.UPDATE_PATH_IN_NEXT);
        for (EDGETYPE et : SEARCH_DIRECTION.edgeTypes()) {
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

            // if other path has passed this vertex, end this path(send back to source)
            if ((vertex.getState() & BubbleMergeWithSearchState.SEARCH_THROUGH_MASK) > 0) {
                outgoingMsg.reset();
                outgoingMsg.setAsCopy(incomingMsg);
                outgoingMsg.setFlag(BubbleMergeWithSearchState.END_NOTICE_IN_SRC);
                sendMsg(incomingMsg.getPathList().getPosition(0), outgoingMsg);
                continue;
            }

            // get msg flag
            byte flag = (byte) (incomingMsg.getFlag() & BubbleMergeWithSearchState.BUBBLE_WITH_SEARCH_FLAG_MASK);

            // different operators for different message flags
            if (flag == BubbleMergeWithSearchState.UPDATE_PATH_IN_NEXT) {
                // set vertex.state = SEARCH_THROUGH
                vertex.setState(BubbleMergeWithSearchState.SEARCH_THROUGH);
                updatePathInNextNode(incomingMsg);
            } else if (flag == BubbleMergeWithSearchState.UPDATE_BRANCH_IN_SRC) {
                // update numBranches in src
                vertex.setNumBranches(vertex.getNumBranches() + incomingMsg.getNumBranches() - 1);
            } else if (flag == BubbleMergeWithSearchState.END_NOTICE_IN_SRC) {
                receiveEndInSource(incomingMsg);
            } else if (flag == BubbleMergeWithSearchState.SAVE_ONLY_PATH_NODES
                    || flag == BubbleMergeWithSearchState.SAVE_ONLY_PATH_NODES_LAST_NODE) {
                saveOnlyPathEdges(incomingMsg);
            } else if (flag == BubbleMergeWithSearchState.PRUNE_DEAD_EDGE) {
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
        /* if newLength > MAX_BFS_LENGTH, send back to source vertex with message(pathList, 
         * internalKmer and setEdgeTypeList) */
        if (newLength > MAX_BFS_LENGTH) {
            outgoingMsg.reset();

            // set flag
            outgoingMsg.setFlag(BubbleMergeWithSearchState.END_NOTICE_IN_SRC);

            // update pathList
            VKmerList pathList = new VKmerList(incomingMsg.getPathList());
            pathList.append(getVertexId());
            outgoingMsg.setPathList(pathList);

            // update internalKmer up to MAX_BFS_LENGTH
            EdgeTypeList edgeTypes = incomingMsg.getEdgeTypeList();
            int size = edgeTypes.size();
            VKmer internalKmer = new VKmer(incomingMsg.getInternalKmer());
            int limitedLength = MAX_BFS_LENGTH - incomingMsg.getPreKmerLength() - kmerSize + 1;
            String newKmer = vertex.getInternalKmer().toString().substring(0, limitedLength);
            internalKmer.mergeWithKmerInDir(edgeTypes.get(size - 1), kmerSize, new VKmer(newKmer));
            outgoingMsg.setInternalKmer(internalKmer);

            // update edgeTypeList
            outgoingMsg.setEdgeTypeList(incomingMsg.getEdgeTypeList());
            sendMsg(source, outgoingMsg);
        }
        /* if newLength <= MAX_BFS_LENGTH, send message to next */
        else {
            outgoingMsg.reset();

            // update pathList
            VKmerList pathList = new VKmerList(incomingMsg.getPathList());
            pathList.append(getVertexId());
            outgoingMsg.setPathList(pathList);

            // update internalKmer
            EdgeTypeList edgeTypes = incomingMsg.getEdgeTypeList();
            int size = edgeTypes.size();
            VKmer internalKmer = new VKmer(incomingMsg.getInternalKmer());
            internalKmer.mergeWithKmerInDir(edgeTypes.get(size - 1), kmerSize, vertex.getInternalKmer());
            outgoingMsg.setInternalKmer(internalKmer);

            // update preKmerLength
            outgoingMsg.setPreKmerLength(newLength);

            // if numBranches == 0, send end to source
            if (vertex.outDegree() == 0) {
                outgoingMsg.setEdgeTypeList(incomingMsg.getEdgeTypeList());
                outgoingMsg.setFlag(BubbleMergeWithSearchState.END_NOTICE_IN_SRC);
                sendMsg(source, outgoingMsg);
                return;
            }
            // if numBranches > 1, update numBranches
            if (vertex.outDegree() > 1) {
                outgoingMsg.setFlag(BubbleMergeWithSearchState.UPDATE_BRANCH_IN_SRC);
                outgoingMsg.setNumBranches(vertex.outDegree());
                sendMsg(source, outgoingMsg);
            }

            // send to next 
            EDGETYPE preToMe = incomingMsg.getEdgeTypeList().get(incomingMsg.getEdgeTypeList().size() - 1);
            for (EDGETYPE et : preToMe.neighborDir().edgeTypes()) {
                for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                    // set flag and source vertex
                    outgoingMsg.setFlag((byte) (BubbleMergeWithSearchState.UPDATE_PATH_IN_NEXT | et.mirror().get()));

                    // update edgeTypes
                    outgoingMsg.setEdgeTypeList(edgeTypes);
                    outgoingMsg.getEdgeTypeList().add(et);

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
        vertex.setNumBranches(--numBranches);
        if (numBranches == 0) {
            /* process in src */
            pickBestPath();
        }
    }

    public void pickBestPath() {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();

        // step1: figure out which path to keep
        int k = 1; // FIXME compare similarity with startHead and get the most possible path, here for test, using 1
        VKmerList pathList = vertex.getArrayOfPathList().get(k);
        EdgeTypeList edgeTypes = vertex.getArrayOfEdgeTypes().get(k);

        // step2: clear edges except those towards similar path
        for (EDGETYPE et : SEARCH_DIRECTION.edgeTypes()) {
            for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                if (edgeTypes.get(0) == et && pathList.getPosition(1).equals(dest))
                    continue;
                vertex.getEdgeMap(et).remove(dest);
            }
        }

        // step3: send save_only_path_nodes_edges message to path nodes
        VKmerList kmerList = new VKmerList();
        for (int i = 1; i < pathList.size(); i++) {
            VKmer dest = new VKmer(pathList.getPosition(i));

            outgoingMsg.reset();
            // last node should be treated as special case, it doesn't need to prune forward or reverse edges
            if(i == pathList.size() - 1) 
                outgoingMsg.setFlag(BubbleMergeWithSearchState.SAVE_ONLY_PATH_NODES_LAST_NODE);
            else
                outgoingMsg.setFlag(BubbleMergeWithSearchState.SAVE_ONLY_PATH_NODES);

            // prev stores in pathList(0)
            kmerList.reset();
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

    public void saveOnlyPathEdges(BubbleMergeWithSearchMessage incomingMsg) {
        BubbleMergeWithSearchVertexValueWritable vertex = getVertexValue();
        
        /* for last node, only prune SEARCH_DIRECTION.mirror() */
        EDGETYPE[] pruneET;
        if(incomingMsg.getFlag() == BubbleMergeWithSearchState.SAVE_ONLY_PATH_NODES)
            pruneET = EDGETYPE.values();
        else 
            pruneET = SEARCH_DIRECTION.mirror().edgeTypes();
        
        // send msg to delete edges except the path nodes
        for (EDGETYPE et : pruneET) {
            for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                // the edges connecting similar set don't need to be pruned 
                if ((et == incomingMsg.getEdgeTypeList().get(0) && dest
                        .equals(incomingMsg.getPathList().getPosition(0)))
                        || (incomingMsg.getEdgeTypeList().size() == 2 && et == incomingMsg.getEdgeTypeList().get(1) && dest
                                .equals(incomingMsg.getPathList().getPosition(1))))
                    continue;
                outgoingMsg.reset();
                outFlag |= BubbleMergeWithSearchState.PRUNE_DEAD_EDGE;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    @Override
    public void compute(Iterator<BubbleMergeWithSearchMessage> msgIterator) {
        if (getSuperstep() == 1) {
            // FIXME need to add statistical method to get top 10% startHeads as begin point
            /** begin BFS in source vertices here "AAC" for test **/
            initVertex();
            if (getVertexId().toString().equals("AAC"))
                beginBFS();
        } else if (getSuperstep() >= 2) {
            /** continue BFS **/
            /* For self- and extended-loop, it will stop when the loop node receives message.
             * Because we mark a search-flag once we search through this node*/
            continueBFS(msgIterator);
        }
        voteToHalt();
    }

    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexInputFormatClass(NodeToBubbleMergeWithSearchVertexInputFormat.class);
        job.setVertexOutputFormatClass(BubbleMergeWithSearchVertexToNodeOutputFormat.class);
        //        job.setGlobalAggregatorClass(ScaffoldingAggregator.class);
        return job;
    }
}
