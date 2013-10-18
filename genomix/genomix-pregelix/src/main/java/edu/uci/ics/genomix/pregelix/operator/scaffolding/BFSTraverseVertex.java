package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.ScaffoldingVertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.Node.EDGETYPE;
import edu.uci.ics.genomix.type.Node.READHEAD_ORIENTATION;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;

public class BFSTraverseVertex extends DeBruijnGraphCleanVertex<ScaffoldingVertexValueWritable, BFSTraverseMessage> {

    public enum UPDATELENGTH_TYPE {
        SRC_OFFSET,
        DEST_OFFSET,
        WHOLE_LENGTH;
    }

    public enum SEARCH_TYPE {
        BEGIN_SEARCH,
        CONTINUE_SEARCH;
    }

    public static class SearchInfo implements Writable {
        private VKmer kmer;
        private READHEAD_ORIENTATION readHeadOrientation;

        public SearchInfo(VKmer otherKmer, READHEAD_ORIENTATION readHeadOrientation) {
            this.kmer.setAsCopy(otherKmer);
            this.readHeadOrientation = readHeadOrientation;
        }

        public VKmer getKmer() {
            return kmer;
        }

        public void setKmer(VKmer kmer) {
            this.kmer = kmer;
        }

        public READHEAD_ORIENTATION getReadHeadOrientation() {
            return readHeadOrientation;
        }

        public void setReadHeadOrientation(READHEAD_ORIENTATION readHeadOrientation) {
            this.readHeadOrientation = readHeadOrientation;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            kmer.write(out);
            out.writeByte(readHeadOrientation.get());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            kmer.readFields(in);
            readHeadOrientation = READHEAD_ORIENTATION.fromByte(in.readByte());
        }
    }

    public static class PathAndEdgeTypeList implements Writable {
        VKmerList kmerList;
        ArrayListWritable<EDGETYPE> edgeTypeList;

        public PathAndEdgeTypeList() {
            kmerList = new VKmerList();
            edgeTypeList = new ArrayListWritable<EDGETYPE>();
        }

        public PathAndEdgeTypeList(VKmerList kmerList, ArrayListWritable<EDGETYPE> edgeTypeList) {
            this.kmerList.setCopy(kmerList);
            this.edgeTypeList.clear();
            this.edgeTypeList.addAll(edgeTypeList);
        }

        public void reset() {
            kmerList.reset();
            edgeTypeList.clear();
        }

        public int size() {
            return kmerList.getCountOfPosition(); // TODO rename to size
        }

        @Override
        public void write(DataOutput out) throws IOException {
            kmerList.write(out);
            edgeTypeList.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            kmerList.readFields(in);
            edgeTypeList.readFields(in);
        }

        public VKmerList getKmerList() {
            return kmerList;
        }

        public void setKmerList(VKmerList kmerList) {
            this.kmerList.setCopy(kmerList);
        }

        public ArrayListWritable<EDGETYPE> getEdgeTypeList() {
            return edgeTypeList;
        }

        public void setEdgeTypeList(ArrayListWritable<EDGETYPE> edgeTypeList) {
            this.edgeTypeList.clear();
            this.edgeTypeList.addAll(edgeTypeList);
        }

    }

    //    protected VKmerBytesWritable srcNode = new VKmerBytesWritable("AAT");
    //    protected VKmerBytesWritable destNode = new VKmerBytesWritable("AGA");
    protected long commonReadId = 2;

    public static int NUM_STEP_SIMULATION_END_BFS = 20;

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
        // src is greater; dest is smaller
        boolean firstIsSrc = searchInfoList.get(0).kmer.compareTo(searchInfoList.get(1).kmer) >= 0;
        VKmer firstKmer = searchInfoList.get(0).getKmer();
        READHEAD_ORIENTATION firstReadHeadType = searchInfoList.get(0).getReadHeadOrientation();
        VKmer secondKmer = searchInfoList.get(1).getKmer();
        READHEAD_ORIENTATION secondReadHeadType = searchInfoList.get(1).getReadHeadOrientation();
        VKmer srcNode;
        if (firstIsSrc) {
            srcNode = firstKmer;
            outgoingMsg.setSrcReadHeadOrientation(firstReadHeadType);
            outgoingMsg.setSeekedVertexId(secondKmer);
            outgoingMsg.setDestReadHeadOrientation(secondReadHeadType);
        } else {
            srcNode = secondKmer;
            outgoingMsg.setSrcReadHeadOrientation(secondReadHeadType);
            outgoingMsg.setSeekedVertexId(firstKmer);
            outgoingMsg.setDestReadHeadOrientation(firstReadHeadType);
        }
        outgoingMsg.setReadId(readId); // commonReadId

        return srcNode;
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
        }
        //        else if (getSuperstep() >= 3) {
        //            BFSearch(msgIterator);
        //        } else if (getSuperstep() > NUM_STEP_SIMULATION_END_BFS){
        //            sendMsgToPathNode();
        //            voteToHalt();
        //        } else if (getSuperstep() == NUM_STEP_SIMULATION_END_BFS + 1){
        //            appendCommonReadId(msgIterator);
        //        }

    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, BFSTraverseVertex.class));
    }

}
