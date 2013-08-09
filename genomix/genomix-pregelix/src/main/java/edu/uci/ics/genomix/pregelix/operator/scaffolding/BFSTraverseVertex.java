package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.MapReduceVertex;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class BFSTraverseVertex extends
    MapReduceVertex {
    
    public class pathId{
        long readId;
        VKmerBytesWritable middleVertexId;
        
        public pathId(){
            readId = 0;
            middleVertexId = new VKmerBytesWritable();
        }
        
        public void set(long readId, VKmerBytesWritable middleVertexId){
            this.readId = readId;
            this.middleVertexId.setAsCopy(middleVertexId);
        }

        public long getReadId() {
            return readId;
        }

        public void setReadId(long readId) {
            this.readId = readId;
        }

        public VKmerBytesWritable getMiddleVertexId() {
            return middleVertexId;
        }

        public void setMiddleVertexId(VKmerBytesWritable middleVertexId) {
            this.middleVertexId = middleVertexId;
        }
        
    }
    
    private VKmerBytesWritable srcNode = new VKmerBytesWritable();
    private VKmerBytesWritable destNode = new VKmerBytesWritable();
    private List<MessageWritable> msgList = new  ArrayList<MessageWritable>();
    Map<Long, List<MessageWritable>> receivedMsg = new HashMap<Long, List<MessageWritable>>();
    
    private boolean isFakeVertex = false;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(incomingMsg == null)
            incomingMsg = new MessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if(reverseKmer == null)
            reverseKmer = new VKmerBytesWritable();
        if(kmerList == null)
            kmerList = new VKmerListWritable();
        else
            kmerList.reset();
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable(kmerSize);
        isFakeVertex = ((byte)getVertexValue().getState() & State.FAKEFLAG_MASK) > 0 ? true : false;
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
    }
    
    public void aggregateMsgAndGroupedByReadIdInReachedNode(Iterator<MessageWritable> msgIterator){
        receivedMsg.clear();
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            long readId = incomingMsg.getNodeIdList().getPosition(0).getReadId();
            if(receivedMsg.containsKey(readId)){
                msgList.addAll(receivedMsg.get(readId));
                msgList.add(incomingMsg);
                receivedMsg.put(readId, msgList);
            } else{
                msgList.clear();
                msgList.add(incomingMsg);
                receivedMsg.put(readId, msgList);
            }
        }
    }
    
    public void sendOddMsgToFakeNode(MessageWritable msg){
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(msg.getSourceVertexId());
        outgoingMsg.setSeekedVertexId(msg.getSeekedVertexId());
        outgoingMsg.setPathList(msg.getPathList());
        outgoingMsg.setNodeIdList(msg.getNodeIdList());
        outgoingMsg.setMiddleVertexId(getVertexId());
        outgoingMsg.setEven(false);
        sendMsg(fakeVertex, outgoingMsg);
    }
    
    public void sendEvenMsgToFakeNode(MessageWritable msg){
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(msg.getSourceVertexId());
        outgoingMsg.setSeekedVertexId(msg.getSeekedVertexId());
        outgoingMsg.setPathList(msg.getPathList());
        outgoingMsg.setNodeIdList(msg.getNodeIdList());
        outgoingMsg.setMiddleVertexId(getVertexId());
        outgoingMsg.setEven(true);
        sendMsg(fakeVertex, outgoingMsg);
    }
    
    public void initialBroadcaseBFSTraverse(){
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setSeekedVertexId(incomingMsg.getSeekedVertexId());
        outgoingMsg.getPathList().append(getVertexId());
        outgoingMsg.setNodeIdList(incomingMsg.getNodeIdList()); //only one readId
        outgoingMsg.setEven(true);
        sendMsgToAllNeighborNodes(getVertexValue());
        //add footprint
        getVertexValue().getTraverseMap().put(getVertexId(), null);
    }
    
    public void broadcastBFSTraverse(){
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(incomingMsg.getSourceVertexId());
        outgoingMsg.setSeekedVertexId(incomingMsg.getSeekedVertexId());
        outgoingMsg.getPathList().append(getVertexId());
        outgoingMsg.setNodeIdList(incomingMsg.getNodeIdList()); //only one readId
        outgoingMsg.setEven(true);
        sendMsgToAllNeighborNodes(getVertexValue());
        //add footprint
        getVertexValue().getTraverseMap().put(getVertexId(), null);
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            addFakeVertex();
            voteToHalt();
        }
        else if(getSuperstep() == 2){
            tmpKmer.setByRead(kmerSize, "AAT".getBytes(), 0);
            kmerList.append(tmpKmer);
            tmpKmer.setByRead(kmerSize, "CTA".getBytes(), 0);
            kmerList.append(tmpKmer);
            /** initiate two nodes -- srcNode and destNode **/
            srcNode.setAsCopy(kmerList.getPosition(0));
            destNode.setAsCopy(kmerList.getPosition(1));
            // outgoingMsg.setNodeIdList(); set as common readId
            PositionWritable nodeId = new PositionWritable();
            nodeId.set((byte) 0, 1, 0);
            PositionListWritable nodeIdList = new PositionListWritable();
            nodeIdList.append(nodeId);
            outgoingMsg.setNodeIdList(nodeIdList);
            outgoingMsg.setSeekedVertexId(destNode);
            sendMsg(srcNode, outgoingMsg);
            outgoingMsg.setSeekedVertexId(srcNode);
            sendMsg(destNode, outgoingMsg);
        } else if(getSuperstep() == 3){
            if(!isFakeVertex){
                if(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    /** begin to BFS **/
                    initialBroadcaseBFSTraverse();
                }
                voteToHalt();
            }
        } else if(getSuperstep() > 3 && getSuperstep() < 5){
            if(!isFakeVertex){
                /** aggregate message & grouped by readId**/
                aggregateMsgAndGroupedByReadIdInReachedNode(msgIterator);
                /** process receivedMsg  **/
                for(long readId : receivedMsg.keySet()){
                    msgList.clear();
                    msgList.addAll(receivedMsg.get(readId));
                    /** |msg| == 2, two msg meet in the same node **/
                    if(msgList.size() == 2){
                        /** Aggregate both msgs to Fake Node and mark flag as odd **/
                        sendOddMsgToFakeNode(msgList.get(0));
                        sendOddMsgToFakeNode(msgList.get(1));
                        //add footprint
                        getVertexValue().getTraverseMap().put(getVertexId(), null);
                    } else if(msgList.size() == 1){
                        if(getVertexValue().hasPathTo(incomingMsg.getSeekedVertexId())){
                            sendEvenMsgToFakeNode(msgList.get(0));
                        } else{
                            broadcastBFSTraverse();
                        }
                    }
                }
                voteToHalt();
            } else{ //FakeVertex receives and processes Msg
                /** aggregate message & grouped by readId and middleVertex **/
            }
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BFSTraverseVertex.class.getSimpleName());
        job.setVertexClass(BFSTraverseVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
