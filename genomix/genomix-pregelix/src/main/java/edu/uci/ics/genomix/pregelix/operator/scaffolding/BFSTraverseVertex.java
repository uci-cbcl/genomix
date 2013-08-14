package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.BFSTraverseMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.BFSTraverseMessageWritable.EdgeDirs;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class BFSTraverseVertex extends
    BasicGraphCleanVertex<BFSTraverseMessageWritable> {
    
    protected VKmerBytesWritable srcNode = new VKmerBytesWritable("AAT");
    protected VKmerBytesWritable destNode = new VKmerBytesWritable("AGA");
    private long commonReadId = 2; 
    
    private EdgeDirs edgeDirs =  new EdgeDirs();
    private ArrayListWritable<EdgeDirs> edgeDirsList = new ArrayListWritable<EdgeDirs>();
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(incomingMsg == null)
            incomingMsg = new BFSTraverseMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new BFSTraverseMessageWritable();
        else
            outgoingMsg.reset();
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
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
    }
    
    public void initiateSrcAndDestNode(VKmerListWritable pairKmerList, long readId, boolean srcFlip, 
            boolean destFlip){
        srcNode.setAsCopy(pairKmerList.getPosition(0));
        destNode.setAsCopy(pairKmerList.getPosition(1));
        outgoingMsg.setReadId(readId);
        outgoingMsg.setSeekedVertexId(destNode);
        outgoingMsg.setSrcFlip(srcFlip);
        outgoingMsg.setDestFlip(destFlip);
    }
    
    public void initialBroadcaseBFSTraverse(){
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setSeekedVertexId(incomingMsg.getSeekedVertexId());
        outgoingMsg.setSrcFlip(incomingMsg.isSrcFlip());
        outgoingMsg.setDestFlip(incomingMsg.isDestFlip());
        kmerList.reset();
        kmerList.append(getVertexId());
        outgoingMsg.setPathList(kmerList);
        outgoingMsg.setReadId(incomingMsg.getReadId()); //only one readId
        if(incomingMsg.isSrcFlip())
            sendSettledMsgToAllPreviousNodes();
        else
            sendSettledMsgToAllNextNodes();
    }
    
    public void broadcaseBFSTraverse(){
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(incomingMsg.getSourceVertexId());
        outgoingMsg.setSeekedVertexId(incomingMsg.getSeekedVertexId());
        outgoingMsg.setSrcFlip(incomingMsg.isSrcFlip());
        outgoingMsg.setDestFlip(incomingMsg.isDestFlip());
        kmerList.setCopy(incomingMsg.getPathList());
        kmerList.append(getVertexId());
        outgoingMsg.setPathList(kmerList);
        outgoingMsg.setReadId(incomingMsg.getReadId()); //only one readId
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        /** set edgeDirs **/
        setEdgeDirs(meToNeighborDir, neighborToMeDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                sendSettledMsgToAllPreviousNodes();
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                sendSettledMsgToAllNextNodes();
                break;
        }
    }
    
    public void setEdgeDirs(byte meToNeighborDir, byte neighborToMeDir){
        edgeDirsList.addAll(incomingMsg.getEdgeDirsList());
        if(edgeDirsList.isEmpty()){ //first time from srcNode
            /** set srcNode's next dir **/
            edgeDirs.reset();
            edgeDirs.setNextToMeDir(meToNeighborDir);
            edgeDirsList.add(edgeDirs);
            /** set curNode's prev dir **/
            edgeDirs.reset();
            edgeDirs.setPrevToMeDir(neighborToMeDir);
            edgeDirsList.add(edgeDirs);
        } else {
            /** set preNode's next dir **/
            edgeDirs.set(edgeDirsList.get(edgeDirsList.size() - 1));
            edgeDirs.setNextToMeDir(meToNeighborDir);
            edgeDirsList.set(edgeDirsList.size() - 1, edgeDirs);
            /** set curNode's prev dir **/
            edgeDirs.reset();
            edgeDirs.setPrevToMeDir(neighborToMeDir);
            edgeDirsList.add(edgeDirs);
        }
    }
    
    public boolean isValidDestination(){
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        if(incomingMsg.isDestFlip())
            return neighborToMeDir == MessageFlag.DIR_RF || neighborToMeDir == MessageFlag.DIR_RR;
        else
            return neighborToMeDir == MessageFlag.DIR_FF || neighborToMeDir == MessageFlag.DIR_FR;
    }
    
    public void sendMsgToPathNodeToAddCommondReadId(){
        outgoingMsg.reset();
        outgoingMsg.setTraverseMsg(false);
        outgoingMsg.setReadId(incomingMsg.getReadId());
        int count = incomingMsg.getEdgeDirsList().size();
        for(int i = 0; i < count; i++){
            outgoingMsg.getEdgeDirsList().add(incomingMsg.getEdgeDirsList().get(i));
            destVertexId.setAsCopy(incomingMsg.getPathList().getPosition(i));
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    @Override
    public void compute(Iterator<BFSTraverseMessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            addFakeVertex();
            voteToHalt();
        }
        else if(getSuperstep() == 2){
            /** for test, assign two kmer to srcNode and destNode **/
            kmerList.append(srcNode);
            kmerList.append(destNode);
            /** initiate two nodes -- srcNode and destNode **/
            initiateSrcAndDestNode(kmerList, commonReadId, true, false);
            sendMsg(srcNode, outgoingMsg);
            
            deleteVertex(getVertexId());
        } else if(getSuperstep() == 3){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                /** begin to BFS **/
                initialBroadcaseBFSTraverse();
            }
            voteToHalt();
        } else if(getSuperstep() > 3){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                if(incomingMsg.isTraverseMsg()){
                    /** check if find destination **/
                    if(incomingMsg.getSeekedVertexId().equals(getVertexId())){
                        if(isValidDestination()){
                            kmerList.setCopy(incomingMsg.getPathList());
                            kmerList.append(getVertexId());
                            /** send message to all the path nodes to add this common readId **/
                            sendMsgToPathNodeToAddCommondReadId();
                        }
                        else{
                            //continue to BFS
                            broadcaseBFSTraverse();
                        }
                    } else {
                        //continue to BFS
                        broadcaseBFSTraverse();
                    }
                } else{
                    /** add common readId to the corresponding edge **/
//                    getVertexValue().getEdgeList(incomingMsg.getEdgeDirsList().get(0).getPrevToMeDir())
                }
            }
            voteToHalt();
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
