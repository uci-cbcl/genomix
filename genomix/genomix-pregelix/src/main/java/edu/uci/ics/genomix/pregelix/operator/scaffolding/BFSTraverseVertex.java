package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.scaffolding.ScaffoldingVertex.SearchInfo;
import edu.uci.ics.genomix.pregelix.type.EdgeTypes;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.genomix.type.Node.EDGETYPE;

public class BFSTraverseVertex extends BasicGraphCleanVertex<VertexValueWritable, BFSTraverseMessage> {

    //    protected VKmerBytesWritable srcNode = new VKmerBytesWritable("AAT");
    //    protected VKmerBytesWritable destNode = new VKmerBytesWritable("AGA");
    protected long commonReadId = 2;

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if (outgoingMsg == null)
            outgoingMsg = new BFSTraverseMessage();
        else
            outgoingMsg.reset();
        if (fakeVertex == null) {
            fakeVertex = new VKmer();
            //            String random = generaterRandomString(kmerSize + 1);
            //            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
    }

    public VKmer setOutgoingSrcAndDest(long readId, ArrayListWritable<SearchInfo> searchInfoList) {
        //TODO src is smaller; dest is greater
        VKmer srcNode = searchInfoList.get(0).getKmer();
        outgoingMsg.setSrcFlip(searchInfoList.get(0).isFlip());
        VKmer destNode = searchInfoList.get(1).getKmer();
        outgoingMsg.setDestFlip(searchInfoList.get(1).isFlip());
        outgoingMsg.setReadId(readId); // commonReadId
        outgoingMsg.setSeekedVertexId(destNode);

        return srcNode;
    }

    public void broadcaseBFSTraverse(BFSTraverseMessage incomingMsg) {
        // keep same seekedVertexId, srcFlip, destFlip, commonReadId, pathList and edgeTypesList
        outgoingMsg.reset();
        outgoingMsg.setAsCopy(incomingMsg);
        outgoingMsg.setSourceVertexId(getVertexId()); // update srcVertexId //TODO remove?
        outgoingMsg.getPathList().append(getVertexId()); // update pathList

        // A -> B -> C, neighor: A, me: B, validDir: B -> C 
        if (getSuperstep() > 3) {
            EDGETYPE meToNeighbor = EDGETYPE.fromByte(incomingMsg.getFlag());
            DIR validDir = meToNeighbor.dir().mirror();
            // update EdgeTypesList
            updateEdgeTypesList(incomingMsg.getEdgeTypesList(), meToNeighbor);
            // send msg to valid destination
            sendSettledMsgs(validDir, getVertexValue());
        }
    }

    public void updateEdgeTypesList(ArrayListWritable<EdgeTypes> edgeTypesList, EDGETYPE meToNeighbor) {
        EdgeTypes edgeTypes;
        if (edgeTypesList.size() == 0) {//first time from srcNode
            // set srcNode's next edgeType
            edgeTypes = new EdgeTypes();
            edgeTypesList.add(edgeTypes);
        } else {
            // set preNode's next edgeType
            edgeTypes = edgeTypesList.get(edgeTypesList.size() - 1);
        }
        edgeTypes.setMeToNextEdgeType(meToNeighbor.mirror());
        // set curNode's prev edgeType
        if (edgeTypesList.size() != 0) {//first time from srcNode
            edgeTypes = new EdgeTypes();
        }
        edgeTypes.setMeToPrevEdgeType(meToNeighbor);
    }

    public boolean isValidDestination(BFSTraverseMessage incomingMsg) {
        EDGETYPE meToNeighbor = EDGETYPE.fromByte(incomingMsg.getFlag());
        if (incomingMsg.isDestFlip())
            return meToNeighbor.dir() == DIR.REVERSE;
        else
            return meToNeighbor.dir() == DIR.FORWARD;
    }

    public void finalProcessBFS(BFSTraverseMessage incomingMsg) {
        VKmerList pathList = incomingMsg.getPathList();
        pathList.append(getVertexId());
        EDGETYPE meToNeighbor = EDGETYPE.fromByte(incomingMsg.getFlag());
        updateEdgeTypesList(incomingMsg.getEdgeTypesList(), meToNeighbor);
    }

    public void sendMsgToPathNodeToAddCommondReadId(long readId, VKmerList pathList,
            ArrayListWritable<EdgeTypes> edgeTypesList) {
        outgoingMsg.reset();
        outgoingMsg.setTraverseMsg(false);
        outgoingMsg.setReadId(readId);
        int size = pathList.getCountOfPosition();
        VKmerList outPathList = outgoingMsg.getPathList();
        ArrayListWritable<EdgeTypes> outEdgeTypesList = outgoingMsg.getEdgeTypesList();
        for (int i = 0; i < size; i++) {
            outEdgeTypesList.clear();
            outEdgeTypesList.add(edgeTypesList.get(i));
            outPathList.reset();
            if (i == 0) { // the first kmer in pathList
                outPathList.append(new VKmer());
                outPathList.append(pathList.getPosition(i + 1));
            } else if (i == size - 1) { // the last kmer in pathList
                outPathList.append(pathList.getPosition(i - 1));
                outPathList.append(new VKmer());
            } else { // the middle kmer in pathList
                outPathList.append(pathList.getPosition(i - 1));
                outPathList.append(pathList.getPosition(i + 1));
            }
            VKmer destVertexId = pathList.getPosition(i);
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    public void appendCommonReadId(BFSTraverseMessage incomingMsg) {
        long readId = incomingMsg.getReadId();
        VKmer tmpKmer;
        //add readId to prev edge 
        EDGETYPE meToPrev = incomingMsg.getEdgeTypesList().get(0).getMeToPrevEdgeType();
        tmpKmer = incomingMsg.getPathList().getPosition(0);
        if (tmpKmer.getKmerLetterLength() != 0) {
            getVertexValue().getEdgeList(meToPrev).get(tmpKmer).add(readId);
        }
        //add readId to next edge
        EDGETYPE meToNext = incomingMsg.getEdgeTypesList().get(0).getMeToNextEdgeType();
        tmpKmer = incomingMsg.getPathList().getPosition(1);
        if (tmpKmer.getKmerLetterLength() != 0) {
            getVertexValue().getEdgeList(meToNext).get(tmpKmer).add(readId);
        }
    }

    @Override
    public void compute(Iterator<BFSTraverseMessage> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            addFakeVertex("A");
            voteToHalt();
        } else if (getSuperstep() == 2) {
            //            // for test, assign two kmer to srcNode and destNode
            //            kmerList.append(srcNode);
            //            kmerList.append(destNode);
            //            // initiate two nodes -- srcNode and destNode
            //            initiateSrcAndDestNode(kmerList, commonReadId, false, true);
            //            sendMsg(srcNode, outgoingMsg);

            deleteVertex(getVertexId());
        } else if (getSuperstep() == 3) {
            while (msgIterator.hasNext()) {
                BFSTraverseMessage incomingMsg = msgIterator.next();
                // begin to BFS
                broadcaseBFSTraverse(incomingMsg);
            }
            voteToHalt();
        } else if (getSuperstep() > 3) {
            while (msgIterator.hasNext()) {
                BFSTraverseMessage incomingMsg = msgIterator.next();
                if (incomingMsg.isTraverseMsg()) {
                    // check if find destination
                    if (incomingMsg.getSeekedVertexId().equals(getVertexId())) {
                        if (isValidDestination(incomingMsg)) {
                            // final step to process BFS -- pathList and dirList
                            finalProcessBFS(incomingMsg);
                            // send message to all the path nodes to add this common readId 
                            sendMsgToPathNodeToAddCommondReadId(incomingMsg.getReadId(), incomingMsg.getPathList(),
                                    incomingMsg.getEdgeTypesList());
                        } else {//continue to BFS
                            broadcaseBFSTraverse(incomingMsg);
                        }
                    } else {//continue to BFS
                        broadcaseBFSTraverse(incomingMsg);
                    }
                } else {// append common readId to the corresponding edge
                    appendCommonReadId(incomingMsg);
                }
            }
            voteToHalt();
        }

    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, BFSTraverseVertex.class));
    }

}
