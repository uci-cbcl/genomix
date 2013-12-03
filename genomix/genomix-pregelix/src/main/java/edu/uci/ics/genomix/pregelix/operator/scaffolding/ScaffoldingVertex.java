package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NodeToScaffoldingVertexInputFormat;
import edu.uci.ics.genomix.pregelix.format.ScaffoldingVertexToNodeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.PathAndEdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.SearchInfo;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.pregelix.api.job.PregelixJob;

/**
 * Graph clean pattern: Scaffolding
 */
public class ScaffoldingVertex extends BasicBFSTraverseVertex {

    // TODO Optimization: send map to readId.hashValue() bin
    // TODO BFS can seperate into simple BFS to filter and real BFS

    public static int NUM_STEP_END_BFS = -1;
    public static int SCAFFOLDING_MIN_TRAVERSAL_LENGTH = -1;
    public static int SCAFFOLDING_MAX_TRAVERSAL_LENGTH = -1;
    public static int SCAFFOLDING_VERTEX_MIN_COVERAGE = -1;
    public static int SCAFFOLDING_VERTEX_MIN_LENGTH = -1;

    @Override
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
        if (outgoingMsg == null)
            outgoingMsg = new BFSTraverseMessage();
        else
            outgoingMsg.reset();
        if (SCAFFOLDING_MIN_TRAVERSAL_LENGTH < 0)
            SCAFFOLDING_MIN_TRAVERSAL_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_MIN_TRAVERSAL_LENGTH));
        if (SCAFFOLDING_MAX_TRAVERSAL_LENGTH < 0)
            SCAFFOLDING_MAX_TRAVERSAL_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_MAX_TRAVERSAL_LENGTH));
        if (SCAFFOLDING_VERTEX_MIN_COVERAGE < 0)
            SCAFFOLDING_VERTEX_MIN_COVERAGE = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_VERTEX_MIN_COVERAGE));
        if (SCAFFOLDING_VERTEX_MIN_LENGTH < 0)
            SCAFFOLDING_VERTEX_MIN_LENGTH = Integer.parseInt(getContext().getConfiguration().get(
                    GenomixJobConf.SCAFFOLDING_VERTEX_MIN_LENGTH));
        if (NUM_STEP_END_BFS < 0)
            NUM_STEP_END_BFS = SCAFFOLDING_MAX_TRAVERSAL_LENGTH - kmerSize + 3;
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    public void addReadsToScaffoldingMap(HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap,
            ReadHeadSet readIds, READHEAD_ORIENTATION readHeadType) {
        // searchInfo can be a struct but not a class
        SearchInfo searchInfo;
        ArrayListWritable<SearchInfo> searchInfoList;

        for (Long longReadId : readIds.getReadIdSet()) {
            LongWritable readId = new LongWritable(longReadId);
            if (scaffoldingMap.containsKey(readId)) {
                searchInfoList = scaffoldingMap.get(readId);
            } else {
                searchInfoList = new ArrayListWritable<SearchInfo>();
                scaffoldingMap.put(readId, searchInfoList);
            }
            searchInfo = new SearchInfo(getVertexId(), readHeadType);
            searchInfoList.add(searchInfo);
        }
    }

    /**
     * step 1:
     */
    public void generateScaffoldingMap() {
        // add a fake vertex 
        addFakeVertex("A");
        // grouped by 5'/~5' readId in aggregator
        ScaffoldingVertexValueWritable vertex = getVertexValue();
        if (vertex.isValidScaffoldingSearchNode()) {
            outgoingMsg.reset();
            HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> map = outgoingMsg.getScaffoldingMap();
            addReadsToScaffoldingMap(map, vertex.getUnflippedReadIds(), READHEAD_ORIENTATION.UNFLIPPED);
            addReadsToScaffoldingMap(map, vertex.getFlippedReadIds(), READHEAD_ORIENTATION.FLIPPED);
            sendMsg(fakeVertex, outgoingMsg);
        }
        voteToHalt();
    }

    /**
     * step 2:
     */
    public void processScaffoldingMap(Iterator<BFSTraverseMessage> msgIterator) {
        // aggregate incomingMsg.scaffoldingMap
        HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap = new HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>>();
        BFSTraverseMessage incomingMsg;
        HashMapWritable<LongWritable, ArrayListWritable<SearchInfo>> curMap;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            curMap = incomingMsg.getScaffoldingMap();
            for (LongWritable key : curMap.keySet()) {
                ArrayListWritable<SearchInfo> value;
                if (scaffoldingMap.containsKey(key)) {
                    value = scaffoldingMap.get(key);
                } else {
                    value = new ArrayListWritable<SearchInfo>();
                }
                value.addAll(curMap.get(key));
                scaffoldingMap.put(key, value);
            }
        }
        // fake vertex process scaffoldingMap 
        ArrayListWritable<SearchInfo> searchInfoList;
        for (Entry<LongWritable, ArrayListWritable<SearchInfo>> entry : scaffoldingMap.entrySet()) {
            searchInfoList = entry.getValue();
            if (searchInfoList.size() > 2)
                throw new IllegalStateException(
                        "The size of SearchInfoList should be not bigger than 2, but here its size " + "is "
                                + searchInfoList.size() + "!");
            if (searchInfoList.size() == 2) {
                outgoingMsg.reset();
                VKmer srcNode = setSrcAndOutgoingMsgForDest(entry.getKey().get(), searchInfoList);
                sendMsg(srcNode, outgoingMsg);
            }
        }

        deleteVertex(getVertexId());
    }

    public boolean isInRange(int traversalLength) {
        return traversalLength <= SCAFFOLDING_MAX_TRAVERSAL_LENGTH
                && traversalLength >= SCAFFOLDING_MIN_TRAVERSAL_LENGTH;
    }

    public boolean isValidDestination(BFSTraverseMessage incomingMsg) {
        // update totalBFSLength
        int totalBFSLength = updateBFSLength(incomingMsg, UPDATELENGTH_TYPE.DEST_OFFSET);
        return isValidOrientation(incomingMsg) && isInRange(totalBFSLength);
    }

    public void BFSearch(Iterator<BFSTraverseMessage> msgIterator, SEARCH_TYPE searchType) {
        ScaffoldingVertexValueWritable vertex = getVertexValue();
        HashMapWritable<LongWritable, BooleanWritable> unambiguousReadIds = vertex.getUnambiguousReadIds();
        BFSTraverseMessage incomingMsg;
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            /** For dest node -- save PathList and EdgeTypeList if valid (stop when ambiguous) **/
            if (incomingMsg.getTargetVertexId().equals(getVertexId()) && isValidDestination(incomingMsg)) {
                LongWritable commonReadId = new LongWritable(incomingMsg.getReadId());
                HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap = vertex.getPathMap();
                if (pathMap.containsKey(commonReadId)) { // if it's ambiguous path
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
            /** For all nodes -- send messge to all neighbor if there exists valid path **/
            // iteration 3 is the beginning of the search-- use the portion of the kmer remaining after accounting for the offset
            int totalBFSLength = updateBFSLength(incomingMsg,
                    searchType == SEARCH_TYPE.BEGIN_SEARCH ? UPDATELENGTH_TYPE.SRC_OFFSET
                            : UPDATELENGTH_TYPE.WHOLE_LENGTH);
            if (totalBFSLength < SCAFFOLDING_MAX_TRAVERSAL_LENGTH) {
                // setup ougoingMsg and prepare to sendMsg
                outgoingMsg.reset();

                // copy unchanged details from incomingMsg
                outgoingMsg.setSourceVertexId(incomingMsg.getSourceVertexId());
                outgoingMsg.setTargetVertexId(incomingMsg.getTargetVertexId());
                outgoingMsg.setReadId(incomingMsg.getReadId());
                outgoingMsg.setSrcReadHeadOrientation(incomingMsg.getSrcReadHeadOrientation());
                outgoingMsg.setDestReadHeadOrientation(incomingMsg.getDestReadHeadOrientation());
                // update totalBFSLength 
                outgoingMsg.setTotalBFSLength(totalBFSLength);

                // update PathList
                VKmerList updatedKmerList = incomingMsg.getPathList();
                updatedKmerList.append(getVertexId());
                outgoingMsg.setPathList(updatedKmerList);

                // send message to valid neighbor
                EdgeTypeList oldEdgeTypeList = incomingMsg.getEdgeTypeList();
                if (searchType == SEARCH_TYPE.BEGIN_SEARCH) { // the initial BFS 
                    // send message to the neighbors based on srcFlip and update EdgeTypeList
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
        initVertex();
        if (getSuperstep() == 1) {
            generateScaffoldingMap();
        } else if (getSuperstep() == 2) {
            processScaffoldingMap(msgIterator);
        } else if (getSuperstep() >= 3 && getSuperstep() < NUM_STEP_END_BFS) {
            if (getSuperstep() == 3)
                BFSearch(msgIterator, SEARCH_TYPE.BEGIN_SEARCH);
            else
                BFSearch(msgIterator, SEARCH_TYPE.CONTINUE_SEARCH);
        } else if (getSuperstep() == NUM_STEP_END_BFS) {
            sendMsgToPathNode();
            voteToHalt();
        } else if (getSuperstep() == NUM_STEP_END_BFS + 1) {
            appendCommonReadId(msgIterator);
            voteToHalt();
        } else {
            throw new IllegalStateException("Programmer error!!!");
        }
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, ScaffoldingVertex.class));
    }

    public static PregelixJob getConfiguredJob(
            GenomixJobConf conf,
            Class<? extends DeBruijnGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass)
            throws IOException {
        PregelixJob job = DeBruijnGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setVertexInputFormatClass(NodeToScaffoldingVertexInputFormat.class);
        job.setVertexOutputFormatClass(ScaffoldingVertexToNodeOutputFormat.class);
        //        job.setGlobalAggregatorClass(ScaffoldingAggregator.class);
        return job;
    }
}
