package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.pregelix.io.PathAndEdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class BFSTraverseVertex extends DeBruijnGraphCleanVertex<ScaffoldingVertexValueWritable, BFSTraverseMessage> {

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

    public enum READHEAD_TYPE {
        UNFLIPPED,
        FLIPPED;
    }

    public enum UPDATELENGTH_TYPE {
        SRC_OFFSET,
        DEST_OFFSET,
        WHOLE_LENGTH;
    }

    public enum SEARCH_TYPE {
        BEGIN_SEARCH,
        CONTINUE_SEARCH;
    }

    /**
     * SearchInfo stores information about destinationKmer and ifFlip(true == from EndReads; false == from startReads)
     * Used by scaffolding
     */
    public static class SearchInfo implements Writable {
        private VKmer destKmer;
        private boolean flip; // TODO if use enum, move byte transformation inside write()...

        public SearchInfo() {
            destKmer = new VKmer();
            flip = false;
        }

        public SearchInfo(VKmer otherKmer, boolean flip) {
            this();
            this.destKmer.setAsCopy(otherKmer);
            this.flip = flip;
        }

        public SearchInfo(VKmer otherKmer, READHEAD_TYPE flip) {
            this();
            this.destKmer.setAsCopy(otherKmer);
            this.flip = flip == READHEAD_TYPE.FLIPPED ? true : false;
        }

        public VKmer getDestKmer() {
            return destKmer;
        }

        public void setDestKmer(VKmer destKmer) {
            this.destKmer = destKmer;
        }

        public boolean isFlip() {
            return flip;
        }

        public void setFlip(boolean flip) {
            this.flip = flip;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            destKmer.write(out);
            out.writeBoolean(flip);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            destKmer.readFields(in);
            flip = in.readBoolean();
        }
    }

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (maxBFSIteration == -1) {
            maxBFSIteration = getContext().getConfiguration().getInt(NUM_STEP_SIMULATION_END_BFS, 10);
        }
        if (maxTraversalLength == -1) {
            maxTraversalLength = getContext().getConfiguration().getInt(MAX_TRAVERSAL_LENGTH, 10);
        }
        if (source == "") {
            source = getContext().getConfiguration().get(SOURCE);
        }
        if (destination == "") {
            destination = getContext().getConfiguration().get(DESTINATION);
        }
        if (commonReadId == -1) {
            commonReadId = getContext().getConfiguration().getLong(COMMOND_READID, 5);
        }
        if (outgoingMsg == null)
            outgoingMsg = new BFSTraverseMessage();
        else
            outgoingMsg.reset();
        if (fakeVertex == null) {
            fakeVertex = new VKmer();
        }
    }

    public void addFakeVertex(String fakeKmer) {
        synchronized (lock) {
            fakeVertex.setFromStringBytes(1, fakeKmer.getBytes(), 0);
            if (!fakeVertexExist) {
                //add a fake vertex
                Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());

                ScaffoldingVertexValueWritable vertexValue = new ScaffoldingVertexValueWritable();
                vertexValue.setFakeVertex(true);

                vertex.setVertexId(fakeVertex);
                vertex.setVertexValue(vertexValue);

                addVertex(fakeVertex, vertex);
                fakeVertexExist = true;
            }
        }
    }

    public boolean isValidOrientation(BFSTraverseMessage incomingMsg) {
        EDGETYPE meToIncoming = EDGETYPE.fromByte(incomingMsg.getFlag());
        if (incomingMsg.isDestFlip())
            return meToIncoming.dir() == DIR.REVERSE;
        else
            return meToIncoming.dir() == DIR.FORWARD;
    }

    public int updateBFSLength(BFSTraverseMessage incomingMsg, UPDATELENGTH_TYPE type) {
        int totalBFSLength = incomingMsg.getTotalBFSLength();
        VertexValueWritable vertex = getVertexValue();
        int internalKmerLength = vertex.getInternalKmer().getKmerLetterLength();
        ReadHeadSet readHeadSet;
        int offset;
        switch (type) {
        // remember to account for partial overlaps
        // src or dest, as long as flip, updateLength += offset, otherwise updateLength += kmerLength - offset
        // TODO (Anbang) Please check this update length during the code review
            case SRC_OFFSET:
                boolean srcIsFlip = incomingMsg.isSrcFlip();
                readHeadSet = srcIsFlip ? vertex.getEndReads() : vertex.getStartReads();
                offset = readHeadSet.getOffsetFromReadId(incomingMsg.getReadId());
                return srcIsFlip ? offset : internalKmerLength - offset;
            case DEST_OFFSET:
                boolean destIsFlip = incomingMsg.isDestFlip();
                readHeadSet = destIsFlip ? vertex.getEndReads() : vertex.getStartReads();
                offset = readHeadSet.getOffsetFromReadId(incomingMsg.getReadId());
                return destIsFlip ? totalBFSLength + offset - kmerSize + 1 : totalBFSLength + internalKmerLength
                        - offset - kmerSize + 1;
            case WHOLE_LENGTH:
                return totalBFSLength + internalKmerLength - kmerSize + 1;
            default:
                throw new IllegalStateException("Update length type only has two kinds: offset and whole_length!");
        }

    }

    public void sendMsgToNeighbors(EdgeTypeList edgeTypeList, DIR direction) {
        VertexValueWritable vertex = getVertexValue();
        for (EDGETYPE et : direction.edgeTypes()) {
            for (VKmer dest : vertex.getEdgeMap(et).keySet()) {
                outFlag &= EDGETYPE.CLEAR;
                outFlag |= et.mirror().get();
                outgoingMsg.setFlag(outFlag);
                // update EdgeTypeList
                edgeTypeList.add(et);
                outgoingMsg.setEdgeTypeList(edgeTypeList);
                sendMsg(dest, outgoingMsg);
            }
        }
    }

    public VKmer setSrcAndOutgoingMsgForDest(long readId, ArrayListWritable<SearchInfo> searchInfoList) {
        // src is greater; dest is smaller
        boolean firstIsSrc = searchInfoList.get(0).destKmer.compareTo(searchInfoList.get(1).destKmer) >= 0;
        VKmer firstKmer = searchInfoList.get(0).getDestKmer();
        boolean firstFlip = searchInfoList.get(0).isFlip();
        VKmer secondKmer = searchInfoList.get(1).getDestKmer();
        boolean secondFlip = searchInfoList.get(1).isFlip();
        VKmer srcNode;
        if (firstIsSrc) {
            srcNode = firstKmer;
            outgoingMsg.setSrcFlip(firstFlip);
            outgoingMsg.setTargetVertexId(secondKmer);
            outgoingMsg.setDestFlip(secondFlip);
        } else {
            srcNode = secondKmer;
            outgoingMsg.setSrcFlip(secondFlip);
            outgoingMsg.setTargetVertexId(firstKmer);
            outgoingMsg.setDestFlip(firstFlip);
        }

        outgoingMsg.setReadId(readId); // commonReadId

        return srcNode;
    }

    public boolean anyUnambiguous(HashMapWritable<LongWritable, BooleanWritable> unambiguousReadIds) {
        boolean anyUnambiguous = false;
        for (BooleanWritable b : unambiguousReadIds.values()) {
            if (b.get()) {
                anyUnambiguous = true;
                break;
            }
        }
        return anyUnambiguous;
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
            /** For dest node -- save PathList and EdgeTypeList if valid (stop when ambiguous) **/
            if (incomingMsg.getTargetVertexId().equals(getVertexId())) {
                long commonReadId = incomingMsg.getReadId();
                HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap = vertex.getPathMap();
                if (pathMap.containsKey(commonReadId)) { // if it's ambiguous path
                    // put empty in value to mark it as ambiguous path
                    pathMap.remove(commonReadId);
                    unambiguousReadIds.put(new LongWritable(commonReadId), new BooleanWritable(false));
                    continue; // stop BFS search here
                } else { // if it's unambiguous path, save 
                    VKmerList updatedKmerList = new VKmerList(incomingMsg.getPathList());
                    updatedKmerList.append(getVertexId());
                    // doesn't need to update edgeTypeList
                    PathAndEdgeTypeList pathAndEdgeTypeList = new PathAndEdgeTypeList(updatedKmerList,
                            incomingMsg.getEdgeTypeList());
                    pathMap.put(new LongWritable(commonReadId), pathAndEdgeTypeList);
                    unambiguousReadIds.put(new LongWritable(commonReadId), new BooleanWritable(true));
                }
            }
            /** For all nodes -- send messge to all neighbors if there exists valid path **/
            // iteration 3 is the beginning of the search-- use the portion of the kmer remaining after accounting for the offset
            int totalBFSLength = updateBFSLength(incomingMsg,
                    searchType == SEARCH_TYPE.BEGIN_SEARCH ? UPDATELENGTH_TYPE.SRC_OFFSET
                            : UPDATELENGTH_TYPE.WHOLE_LENGTH);
            if (totalBFSLength < maxTraversalLength) {
                // setup ougoingMsg and prepare to sendMsg
                outgoingMsg.reset();

                // copy targetVertex
                outgoingMsg.setTargetVertexId(incomingMsg.getTargetVertexId());
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
                    if (incomingMsg.isSrcFlip())
                        sendMsgToNeighbors(oldEdgeTypeList, DIR.REVERSE);
                    else
                        sendMsgToNeighbors(oldEdgeTypeList, DIR.FORWARD);
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

    /**
     * step 4:
     */
    public void sendMsgToPathNode() {
        ScaffoldingVertexValueWritable vertex = getVertexValue();
        // send message to all the path nodes to add this common readId
        sendMsgToPathNodeToAddCommondReadId(vertex.getPathMap());
        //set statistics counter: Num_Scaffodings
        incrementCounter(StatisticsCounter.Num_Scaffodings);
        vertex.setCounters(counters);
    }

    /**
     * send message to all the path nodes to add this common readId
     */
    public void sendMsgToPathNodeToAddCommondReadId(HashMapWritable<LongWritable, PathAndEdgeTypeList> pathMap) {
        VertexValueWritable vertex = getVertexValue();
        for (LongWritable commonReadId : pathMap.keySet()) {
            outgoingMsg.reset();
            outgoingMsg.setReadId(2); //commonReadId.get()

            PathAndEdgeTypeList pathAndEdgeTypeList = pathMap.get(commonReadId);
            VKmerList kmerList = pathAndEdgeTypeList.getKmerList();
            EdgeTypeList edgeTypeList = pathAndEdgeTypeList.getEdgeTypeList();
            EdgeTypeList outputEdgeTypeList = outgoingMsg.getEdgeTypeList();
            VKmerList pathList = outgoingMsg.getPathList();
            // msg.pathList and msg.edgeTypeList store neighbor information 
            for (int i = 0; i < pathAndEdgeTypeList.size() - 1; i++) {
                pathList.reset();
                // set next edgeType
                outputEdgeTypeList.clear();
                outputEdgeTypeList.add(edgeTypeList.get(i)); // msg.edgeTypeList[0] stores nextEdgeType
                VKmer nextKmer = kmerList.getPosition(i + 1);
                pathList.append(nextKmer); // msg.pathList[0] stores nextKmer

                // first iteration doesn't need to send prev edgeType to srcNode
                if (i == 0) {
                    sendMsg(kmerList.getPosition(i), outgoingMsg);
                } else {
                    // set prev edgeType
                    outputEdgeTypeList.add(edgeTypeList.get(i - 1).mirror()); // msg.edgeTypeList[1] stores preEdgeType
                    VKmer preKmer = kmerList.getPosition(i - 1);
                    pathList.append(preKmer); // msg.pathList[1] stores preKmer 

                    sendMsg(kmerList.getPosition(i), outgoingMsg);
                }
            }
            // only last element in pathAndEdgeTypeList updates self
            EDGETYPE prevToMe = edgeTypeList.get(pathAndEdgeTypeList.size() - 2);
            VKmer preKmer = kmerList.getPosition(pathAndEdgeTypeList.size() - 2);

            vertex.getEdgeMap(prevToMe.mirror()).get(preKmer).add(new Long(2));//commonReadId.get()
        }
    }

    /**
     * append common readId to the corresponding edge
     */
    public void appendCommonReadId(Iterator<BFSTraverseMessage> msgIterator) {
        VertexValueWritable vertex = getVertexValue();
        while (msgIterator.hasNext()) {
            BFSTraverseMessage incomingMsg = msgIterator.next();
            long commonReadId = incomingMsg.getReadId();
            VKmerList pathList = incomingMsg.getPathList();
            EdgeTypeList edgeTypeList = incomingMsg.getEdgeTypeList();
            if (pathList.size() > 2)
                throw new IllegalStateException("When path node receives message to append common readId,"
                        + "PathList should only have one(next) or two(prev and next) elements!");
            for (int i = 0; i < pathList.size(); i++) {
                vertex.getEdgeMap(edgeTypeList.get(i)).get(pathList.getPosition(i)).add(commonReadId);
            }
        }
    }

    @Override
    public void compute(Iterator<BFSTraverseMessage> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            addFakeVertex("A");
            voteToHalt();
        } else if (getSuperstep() == 2) {
            // for test, assign two kmer to srcNode and destNode
            VKmer srcNode = new VKmer(source);
            VKmer destNode = new VKmer(destination);
            SearchInfo srcSearchInfo = new SearchInfo(srcNode, false);
            SearchInfo destSearchInfo = new SearchInfo(destNode, true);
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
