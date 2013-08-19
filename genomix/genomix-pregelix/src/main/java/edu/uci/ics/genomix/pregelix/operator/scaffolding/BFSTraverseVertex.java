package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.Iterator;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.BFSTraverseMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.type.EdgeDirs;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class BFSTraverseVertex extends
    BasicGraphCleanVertex<BFSTraverseMessageWritable> {
    
    protected VKmerBytesWritable srcNode = new VKmerBytesWritable("AAT");
    protected VKmerBytesWritable destNode = new VKmerBytesWritable("AGA");
    protected long commonReadId = 2; 
    
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
            sendSettledMsgToAllPrevNodes(getVertexValue());
        else
            sendSettledMsgToAllNextNodes(getVertexValue());
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
                sendSettledMsgToAllPrevNodes(getVertexValue());
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                sendSettledMsgToAllNextNodes(getVertexValue());
                break;
        }
    }
    
    public void setEdgeDirs(byte meToNeighborDir, byte neighborToMeDir){
        edgeDirsList.clear();
        edgeDirsList.addAll(incomingMsg.getEdgeDirsList());
        if(edgeDirsList.isEmpty()){ //first time from srcNode
            /** set srcNode's next dir **/
            edgeDirs.reset();
            edgeDirs.setNextToMeDir(meToNeighborDir);
            edgeDirsList.add(new EdgeDirs(edgeDirs)); 
            /** set curNode's prev dir **/
            edgeDirs.reset();
            edgeDirs.setPrevToMeDir(neighborToMeDir);
            edgeDirsList.add(new EdgeDirs(edgeDirs));
        } else {
            /** set preNode's next dir **/
            edgeDirs.set(edgeDirsList.get(edgeDirsList.size() - 1));
            edgeDirs.setNextToMeDir(meToNeighborDir);
            edgeDirsList.set(edgeDirsList.size() - 1, new EdgeDirs(edgeDirs));
            /** set curNode's prev dir **/
            edgeDirs.reset();
            edgeDirs.setPrevToMeDir(neighborToMeDir);
            edgeDirsList.add(new EdgeDirs(edgeDirs));
        }
        outgoingMsg.setEdgeDirsList(edgeDirsList);
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
        int count = kmerList.getCountOfPosition();
        for(int i = 0; i < count; i++){
            outgoingMsg.getEdgeDirsList().clear();
            outgoingMsg.getEdgeDirsList().add(incomingMsg.getEdgeDirsList().get(i));
            outgoingMsg.getPathList().reset();
            if(i == 0){
                outgoingMsg.getPathList().append(new VKmerBytesWritable());
                outgoingMsg.getPathList().append(kmerList.getPosition(i + 1));
            } else if(i == count - 1){
                outgoingMsg.getPathList().append(kmerList.getPosition(i - 1));
                outgoingMsg.getPathList().append(new VKmerBytesWritable());
            } else{
                outgoingMsg.getPathList().append(kmerList.getPosition(i - 1));
                outgoingMsg.getPathList().append(kmerList.getPosition(i + 1));  
            }
            destVertexId.setAsCopy(kmerList.getPosition(i));
            sendMsg(destVertexId, outgoingMsg);
        }
    }
    
    public void finalProcessBFS(){
        kmerList.setCopy(incomingMsg.getPathList());
        kmerList.append(getVertexId());
        incomingMsg.setPathList(kmerList);
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        setEdgeDirs(meToNeighborDir, neighborToMeDir);
        incomingMsg.setEdgeDirsList(outgoingMsg.getEdgeDirsList());
    }
    
    public void appendCommonReadId(){
        long readId = incomingMsg.getReadId();
        //add readId to prev edge 
        byte prevToMeDir = incomingMsg.getEdgeDirsList().get(0).getPrevToMeDir();
        tmpKmer.setAsCopy(incomingMsg.getPathList().getPosition(0));
        if(tmpKmer.getKmerLetterLength() != 0)
            getVertexValue().getEdgeList(prevToMeDir).getReadIDs(tmpKmer).appendReadId(readId);
        //set readId to next edge
        byte nextToMeDir = incomingMsg.getEdgeDirsList().get(0).getNextToMeDir();
        tmpKmer.setAsCopy(incomingMsg.getPathList().getPosition(1));
        if(tmpKmer.getKmerLetterLength() != 0)
            getVertexValue().getEdgeList(nextToMeDir).getReadIDs(tmpKmer).appendReadId(readId);
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
            initiateSrcAndDestNode(kmerList, commonReadId, false, true);
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
                            /** final step to process BFS -- pathList and dirList **/
                            finalProcessBFS();
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
                    /** append common readId to the corresponding edge **/
                    appendCommonReadId();
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
