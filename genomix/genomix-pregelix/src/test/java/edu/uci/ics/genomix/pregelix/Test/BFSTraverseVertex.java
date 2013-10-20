package edu.uci.ics.genomix.pregelix.Test;

import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.PathAndEdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.SearchInfo;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.BasicBFSTraverseVertex;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BFSTraverseVertex extends BasicBFSTraverseVertex {

    public static final String NUM_STEP_SIMULATION_END_BFS = "BFSTraverseVertex.maxBFSIteration";
    public static final String MAX_TRAVERSAL_LENGTH = "BFSTraverseVertex.maxTraversalLength";
    public static final String SOURCE = "BFSTraverseVertex.source";
    public static final String DESTINATION = "BFSTraverseVertex.destination";
    public static final String COMMOND_READID = "BFSTraverseVertex.commonReadId";

    private int maxBFSIteration = -1;
    private int maxTraversalLength = -1;
    private String source = "";
    private String destination = "";
    private long commonReadId = -1;

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (maxBFSIteration == -1) {
            maxBFSIteration = Integer.parseInt(getContext().getConfiguration().get(NUM_STEP_SIMULATION_END_BFS));
        }
        if (maxTraversalLength == -1) {
            maxTraversalLength = Integer.parseInt(getContext().getConfiguration().get(MAX_TRAVERSAL_LENGTH));
        }
        if (source == "") {
            source = getContext().getConfiguration().get(SOURCE);
        }
        if (destination == "") {
            destination = getContext().getConfiguration().get(DESTINATION);
        }
        if (commonReadId == -1) {
            commonReadId = Long.parseLong(getContext().getConfiguration().get(COMMOND_READID));
        }
        if (outgoingMsg == null)
            outgoingMsg = new BFSTraverseMessage();
        if (fakeVertex == null) {
            fakeVertex = new VKmer();
        }
    }

    /**
     * step 3:
     */
    public void BFSearch(Iterator<BFSTraverseMessage> msgIterator, SEARCH_TYPE searchType) {
        ScaffoldingVertexValueWritable vertex = getVertexValue();
        HashMapWritable<LongWritable, BooleanWritable> unambiguousReadIds = vertex.getUnambiguousReadIds();
        BFSTraverseMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            /**
             * For dest node -- save PathList and EdgeTypeList if valid (stop
             * when ambiguous)
             **/
            if (incomingMsg.getTargetVertexId().equals(getVertexId())) {
                LongWritable commonReadId = new LongWritable(incomingMsg.getReadId());
                HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap = vertex.getPathMap();
                if (pathMap.containsKey(commonReadId)) { // if it's ambiguous
                                                         // path
                    // put empty in value to mark it as ambiguous path
                    pathMap.remove(commonReadId);
                    unambiguousReadIds.put(commonReadId, new BooleanWritable(false));
                    continue; // stop BFS search here
                } else { // if it's unambiguous path, save
                    VKmerList updatedKmerList = new VKmerList(incomingMsg.getPathList());
                    updatedKmerList.append(getVertexId());
                    // doesn't need to update edgeTypeList
                    PathAndEdgeTypeList pathAndEdgeTypeList = new PathAndEdgeTypeList(updatedKmerList,
                            incomingMsg.getEdgeTypeList());
                    pathMap.put(commonReadId, pathAndEdgeTypeList);
                    unambiguousReadIds.put(commonReadId, new BooleanWritable(true));
                }
            }
            /**
             * For all nodes -- send messge to all neighbors if there exists
             * valid path
             **/
            // iteration 3 is the beginning of the search-- use the portion of
            // the kmer remaining after accounting for the offset
            int totalBFSLength = updateBFSLength(incomingMsg,
                    searchType == SEARCH_TYPE.BEGIN_SEARCH ? UPDATELENGTH_TYPE.SRC_OFFSET
                            : UPDATELENGTH_TYPE.WHOLE_LENGTH);
            if (totalBFSLength < maxTraversalLength) {
                // setup ougoingMsg and prepare to sendMsg
                outgoingMsg.reset();

                // copy targetVertex
                outgoingMsg.setTargetVertexId(incomingMsg.getTargetVertexId());
                // copy commonReadId
                outgoingMsg.setReadId(incomingMsg.getReadId());
                // update totalBFSLength
                outgoingMsg.setTotalBFSLength(totalBFSLength);

                // update PathList
                VKmerList updatedKmerList = incomingMsg.getPathList();
                updatedKmerList.append(getVertexId());
                outgoingMsg.setPathList(updatedKmerList);

                // send message to valid neighbor
                EdgeTypeList oldEdgeTypeList = incomingMsg.getEdgeTypeList();
                if (searchType == SEARCH_TYPE.BEGIN_SEARCH) { // the initial BFS
                    // send message to the neighbors based on srcFlip and update
                    // EdgeTypeList
                    switch (incomingMsg.getSrcReadHeadOrientation()) {
                        case FLIPPED:
                            sendMsgToNeighbors(oldEdgeTypeList, DIR.REVERSE);
                            break;
                        case UNFLIPPED:
                            sendMsgToNeighbors(oldEdgeTypeList, DIR.FORWARD);
                            break;
                        default:
                            throw new IllegalStateException("ReadHeadType only has two kinds: FLIPPED AND UNFLIPPED!");
                    }
                } else {
                    // A -> B -> C, neighor: A, me: B, validDir: B -> C
                    EDGETYPE BtoA = EDGETYPE.fromByte(incomingMsg.getFlag());
                    DIR validBtoCDir = BtoA.dir().mirror();

                    // send message to valid neighbors and update EdgeTypeList
                    sendMsgToNeighbors(oldEdgeTypeList, validBtoCDir);
                }
            }
        }
        // check if there is any unambiguous node
        if (anyUnambiguous(unambiguousReadIds))
            activate();
        else
            voteToHalt();
    }

    @Override
    public void compute(Iterator<BFSTraverseMessage> msgIterator) {
        if (getSuperstep() == 1) {
            initVertex();
            addMapPartitionVertices("A");
            voteToHalt();
        } else if (getSuperstep() == 2) {
            // for test, read srcNode and destNode from jobconf
            VKmer srcNode = new VKmer(source);
            VKmer destNode = new VKmer(destination);
            SearchInfo srcSearchInfo = new SearchInfo(srcNode, READHEAD_ORIENTATION.UNFLIPPED);
            SearchInfo destSearchInfo = new SearchInfo(destNode, READHEAD_ORIENTATION.FLIPPED);
            ArrayListWritable<SearchInfo> searchInfoList = new ArrayListWritable<SearchInfo>();
            searchInfoList.add(srcSearchInfo);
            searchInfoList.add(destSearchInfo);
            // initiate two nodes -- srcNode and destNode
            setSrcAndOutgoingMsgForDest(commonReadId, searchInfoList);
            sendMsg(srcNode, outgoingMsg);

            deleteVertex(getVertexId());
        } else if (getSuperstep() >= 3 && getSuperstep() < maxBFSIteration) {
            if (getSuperstep() == 3)
                BFSearch(msgIterator, SEARCH_TYPE.BEGIN_SEARCH);
            else
                BFSearch(msgIterator, SEARCH_TYPE.CONTINUE_SEARCH);
        } else if (getSuperstep() == maxBFSIteration) {
            sendMsgToPathNode();
            voteToHalt();
        } else if (getSuperstep() == maxBFSIteration + 1) {
            appendCommonReadId(msgIterator);
            voteToHalt();
        } else {
            throw new IllegalStateException("Programmer error!!!");
        }

    }

}
