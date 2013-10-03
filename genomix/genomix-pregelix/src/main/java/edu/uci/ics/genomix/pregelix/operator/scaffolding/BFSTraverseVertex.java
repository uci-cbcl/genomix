package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessage;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.EdgeType;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.genomix.type.Node.EDGETYPE;

public class BFSTraverseVertex extends BasicGraphCleanVertex<VertexValueWritable, BFSTraverseMessage> {

    public enum READHEAD_TYPE {
        UNFLIPPED,
        FLIPPED;
    }
    
    public enum UPDATELENGTH_TYPE {
        OFFSET,
        WHOLE_LENGTH;
    }
    
    public static class SearchInfo implements Writable {
        private VKmer kmer;
        private boolean flip;

        public SearchInfo(VKmer otherKmer, boolean flip) {
            this.kmer.setAsCopy(otherKmer);
            this.flip = flip;
        }

        public SearchInfo(VKmer otherKmer, READHEAD_TYPE flip) {
            this.kmer.setAsCopy(otherKmer);
            this.flip = flip == READHEAD_TYPE.FLIPPED ? true : false;
        }

        public VKmer getKmer() {
            return kmer;
        }

        public void setKmer(VKmer kmer) {
            this.kmer = kmer;
        }

        public boolean isFlip() {
            return flip;
        }

        public void setFlip(boolean flip) {
            this.flip = flip;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            kmer.write(out);
            out.writeBoolean(flip);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            kmer.readFields(in);
            flip = in.readBoolean();
        }
    }
    
    public static class PathAndEdgeTypeList implements Writable {
        VKmerList kmerList;
        ArrayListWritable<EdgeType> edgeTypeList;
      
        public PathAndEdgeTypeList(){
            kmerList = new VKmerList();
            edgeTypeList = new ArrayListWritable<EdgeType>();
        }
        
        public PathAndEdgeTypeList(VKmerList kmerList, ArrayListWritable<EdgeType> edgeTypeList){
            this.kmerList.setCopy(kmerList);
            this.edgeTypeList.clear();
            this.edgeTypeList.addAll(edgeTypeList);
        }
        
        public void reset(){
            kmerList.reset();
            edgeTypeList.clear();
        }
        
        public int size(){
            return kmerList.getCountOfPosition();
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

        public ArrayListWritable<EdgeType> getEdgeTypeList() {
            return edgeTypeList;
        }

        public void setEdgeTypeList(ArrayListWritable<EdgeType> edgeTypeList) {
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
        //TODO src is smaller; dest is greater
        VKmer srcNode = searchInfoList.get(0).getKmer();
        outgoingMsg.setSrcFlip(searchInfoList.get(0).isFlip());
        VKmer destNode = searchInfoList.get(1).getKmer();
        outgoingMsg.setDestFlip(searchInfoList.get(1).isFlip());
        outgoingMsg.setReadId(readId); // commonReadId
        outgoingMsg.setSeekedVertexId(destNode);

        return srcNode;
    }
    
    public boolean isValidDestination(BFSTraverseMessage incomingMsg) {
        EDGETYPE meToNeighbor = EDGETYPE.fromByte(incomingMsg.getFlag());
        if (incomingMsg.isDestFlip())
            return meToNeighbor.dir() == DIR.REVERSE;
        else
            return meToNeighbor.dir() == DIR.FORWARD;
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
