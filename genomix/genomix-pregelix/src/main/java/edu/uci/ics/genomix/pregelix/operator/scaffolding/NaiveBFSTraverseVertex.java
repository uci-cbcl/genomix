package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.MapReduceVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class NaiveBFSTraverseVertex extends
    MapReduceVertex {
    
    protected VKmerBytesWritable srcNode = new VKmerBytesWritable();
    protected VKmerBytesWritable destNode = new VKmerBytesWritable();
    Map<Long, List<MessageWritable>> receivedMsg = new HashMap<Long, List<MessageWritable>>();

    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
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
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
    }
    
    public void initialBroadcaseBFSTraverse(){
        outgoingMsg.reset();
        outgoingMsg.setSourceVertexId(getVertexId());
        outgoingMsg.setSeekedVertexId(incomingMsg.getSeekedVertexId());
        outgoingMsg.setSrcFlip(incomingMsg.isSrcFlip());
        outgoingMsg.setDestFlip(incomingMsg.isDestFlip());
        kmerList.append(getVertexId());
        outgoingMsg.setPathList(kmerList);
        outgoingMsg.setNodeIdList(incomingMsg.getNodeIdList()); //only one readId
        if(incomingMsg.isSrcFlip())
            sendSettledMsgToAllPreviousNodes(getVertexValue());
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
        outgoingMsg.setNodeIdList(incomingMsg.getNodeIdList()); //only one readId
        byte meToNeighborDir = (byte) (incomingMsg.getFlag() & MessageFlag.DIR_MASK);
        byte neighborToMeDir = mirrorDirection(meToNeighborDir);
        switch(neighborToMeDir){
            case MessageFlag.DIR_FF:
            case MessageFlag.DIR_FR:
                sendSettledMsgToAllPreviousNodes(getVertexValue());
                break;
            case MessageFlag.DIR_RF:
            case MessageFlag.DIR_RR:
                sendSettledMsgToAllNextNodes(getVertexValue());
                break;
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
  
    public void initiateSrcAndDestNode(VKmerListWritable pairKmerList, ArrayList<Boolean> flagList, 
            PositionListWritable nodeIdList){
        initiateSrcAndDestNode(pairKmerList, flagList.get(0), flagList.get(1), nodeIdList);
    }
    
    public void initiateSrcAndDestNode(VKmerListWritable pairKmerList, boolean srcFlip, boolean destFlip, 
            PositionListWritable nodeIdList){
        srcNode.setAsCopy(pairKmerList.getPosition(0));
        destNode.setAsCopy(pairKmerList.getPosition(1));
        outgoingMsg.setNodeIdList(nodeIdList);
        outgoingMsg.setSeekedVertexId(destNode);
        outgoingMsg.setSrcFlip(srcFlip);
        outgoingMsg.setDestFlip(destFlip);
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
            tmpKmer.setByRead(kmerSize, "AGA".getBytes(), 0);
            kmerList.append(tmpKmer);
            // outgoingMsg.setNodeIdList(); set as common readId
            PositionWritable nodeId = new PositionWritable();
            nodeId.set((byte) 0, 1, 0);
            PositionListWritable nodeIdList = new PositionListWritable();
            nodeIdList.append(nodeId);
            /** initiate two nodes -- srcNode and destNode **/
            initiateSrcAndDestNode(kmerList, true, false, nodeIdList);
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
                /** check if find destination **/
                if(incomingMsg.getSeekedVertexId().equals(getVertexId())){
                    if(isValidDestination()){
                        kmerList.setCopy(incomingMsg.getPathList());
                        kmerList.append(getVertexId());
                        //TODO do some process after finding a path
                    }
                    else{
                        //keep BFS
                        broadcaseBFSTraverse();
                    }
                } else {
                    //keep BFS
                    broadcaseBFSTraverse();
                }
            }
            voteToHalt();
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf) throws IOException {
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(NaiveBFSTraverseVertex.class.getSimpleName());
        else
            job = new PregelixJob(conf, NaiveBFSTraverseVertex.class.getSimpleName());
        job.setVertexClass(NaiveBFSTraverseVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
