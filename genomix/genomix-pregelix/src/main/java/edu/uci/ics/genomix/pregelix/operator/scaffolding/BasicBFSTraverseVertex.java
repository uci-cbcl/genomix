package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import edu.uci.ics.genomix.pregelix.io.PathAndEdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.SearchInfo;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.EdgeTypeList;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.type.DIR;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class BasicBFSTraverseVertex extends
        DeBruijnGraphCleanVertex<ScaffoldingVertexValueWritable, BFSTraverseMessage> {

    public enum UPDATELENGTH_TYPE {
        SRC_OFFSET,
        DEST_OFFSET,
        WHOLE_LENGTH;
    }

    public enum SEARCH_TYPE {
        BEGIN_SEARCH,
        CONTINUE_SEARCH;
    }

    public void addFakeVertex(String fakeKmer) {
        synchronized (lock) {
            fakeVertex.setFromStringBytes(1, fakeKmer.getBytes(), 0);
            if (!fakeVertexExist) {
                // add a fake vertex
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

    public int updateBFSLength(BFSTraverseMessage incomingMsg, UPDATELENGTH_TYPE type) {

        int totalBFSLength = incomingMsg.getTotalBFSLength();
        VertexValueWritable vertex = getVertexValue();
        int internalKmerLength = vertex.getInternalKmer().getKmerLetterLength();
        ReadHeadSet readHeadSet;
        int offset;
        switch (type) {
        // remember to account for partial overlaps
        // src or dest, as long as flip, updateLength += offset, otherwise
        // updateLength += kmerLength - offset
            case SRC_OFFSET:
                boolean srcIsFlip = incomingMsg.getSrcReadHeadOrientation() == READHEAD_ORIENTATION.FLIPPED;
                readHeadSet = srcIsFlip ? vertex.getFlippedReadIds() : vertex.getUnflippedReadIds();
                offset = readHeadSet.getOffsetFromReadId(incomingMsg.getReadId());
                return srcIsFlip ? offset : internalKmerLength - offset;
            case DEST_OFFSET:
                boolean destIsFlip = incomingMsg.getDestReadHeadOrientation() == READHEAD_ORIENTATION.FLIPPED;
                readHeadSet = destIsFlip ? vertex.getFlippedReadIds() : vertex.getUnflippedReadIds();
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
        boolean firstIsSrc = searchInfoList.get(0).getSrcOrDestKmer()
                .compareTo(searchInfoList.get(1).getSrcOrDestKmer()) >= 0;
        VKmer firstKmer = searchInfoList.get(0).getSrcOrDestKmer();
        READHEAD_ORIENTATION firstReadHeadType = searchInfoList.get(0).getReadHeadOrientation();
        VKmer secondKmer = searchInfoList.get(1).getSrcOrDestKmer();
        READHEAD_ORIENTATION secondReadHeadType = searchInfoList.get(1).getReadHeadOrientation();
        VKmer srcNode;
        if (firstIsSrc) {
            srcNode = firstKmer;
            outgoingMsg.setSrcReadHeadOrientation(firstReadHeadType);
            outgoingMsg.setTargetVertexId(secondKmer);
            outgoingMsg.setDestReadHeadOrientation(secondReadHeadType);
        } else {
            srcNode = secondKmer;
            outgoingMsg.setSrcReadHeadOrientation(secondReadHeadType);
            outgoingMsg.setTargetVertexId(firstKmer);
            outgoingMsg.setDestReadHeadOrientation(firstReadHeadType);
        }
        outgoingMsg.setReadId(readId); // commonReadId
        outgoingMsg.setTotalBFSLength(0); // this is the beginning of the search

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
     * step 4:
     */
    public void sendMsgToPathNode() {
        ScaffoldingVertexValueWritable vertex = getVertexValue();
        // send message to all the path nodes to add this common readId
        sendMsgToPathNodeToAddCommondReadId(vertex.getPathMap());
        // set statistics counter: Num_Scaffodings
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
            outgoingMsg.setReadId(commonReadId.get()); // Option: put 2 for test

            PathAndEdgeTypeList pathAndEdgeTypeList = pathMap.get(commonReadId);
            VKmerList kmerList = pathAndEdgeTypeList.getKmerList();
            EdgeTypeList edgeTypeList = pathAndEdgeTypeList.getEdgeTypeList();
            EdgeTypeList outputEdgeTypeList = outgoingMsg.getEdgeTypeList();
            VKmerList pathList = outgoingMsg.getPathList();
            // msg.pathList and msg.edgeTypeList store neighbor information
            for (int i = 0; i < pathAndEdgeTypeList.size() - 1; i++) {
                pathList.clear();
                // set next edgeType
                outputEdgeTypeList.clear();
                outputEdgeTypeList.add(edgeTypeList.get(i)); // msg.edgeTypeList[0]
                                                             // stores
                                                             // nextEdgeType
                VKmer nextKmer = kmerList.getPosition(i + 1);
                pathList.append(nextKmer); // msg.pathList[0] stores nextKmer

                // first iteration doesn't need to send prev edgeType to srcNode
                if (i == 0) {
                    sendMsg(kmerList.getPosition(i), outgoingMsg);
                } else {
                    // set prev edgeType
                    outputEdgeTypeList.add(edgeTypeList.get(i - 1).mirror()); // msg.edgeTypeList[1]
                                                                              // stores
                                                                              // preEdgeType
                    VKmer preKmer = kmerList.getPosition(i - 1);
                    pathList.append(preKmer); // msg.pathList[1] stores preKmer

                    sendMsg(kmerList.getPosition(i), outgoingMsg);
                }
            }
            // only last element in pathAndEdgeTypeList updates self
            EDGETYPE prevToMe = edgeTypeList.get(pathAndEdgeTypeList.size() - 2);
            VKmer preKmer = kmerList.getPosition(pathAndEdgeTypeList.size() - 2);

            vertex.getEdgeMap(prevToMe.mirror()).get(preKmer).add(commonReadId.get()); // Option: put 2 for test
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

    public boolean isValidOrientation(BFSTraverseMessage incomingMsg) {
        EDGETYPE meToIncoming = EDGETYPE.fromByte(incomingMsg.getFlag());
        switch (incomingMsg.getDestReadHeadOrientation()) {
            case FLIPPED:
                return meToIncoming.dir() == DIR.REVERSE;
            case UNFLIPPED:
                return meToIncoming.dir() == DIR.FORWARD;
            default:
                throw new IllegalStateException("ReadHeadType only has two kinds: FLIPPED AND UNFLIPPED!");
        }
    }

    @Override
    public void compute(Iterator<BFSTraverseMessage> msgIterator) throws Exception {

    }

}
