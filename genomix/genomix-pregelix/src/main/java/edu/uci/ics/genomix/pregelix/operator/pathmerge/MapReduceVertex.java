package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.genomix.type.VKmer;

public class MapReduceVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
	BasicPathMergeVertex<V, M>{
    
    protected VKmer forwardKmer = new VKmer();
    protected VKmer reverseKmer = new VKmer();

    /**
     * initiate kmerSize, maxIteration
     */
    @SuppressWarnings("unchecked")
    @Override
    public void initVertex() {
        super.initVertex();
        if(outgoingMsg == null) {
            outgoingMsg = (M) new PathMergeMessageWritable();
        }
    }
    
    public Map<VKmer, VKmerList> mapKeyByInternalKmer(Iterator<M> msgIterator){
        Map<VKmer, VKmerList> kmerMapper = new HashMap<VKmer, VKmerList>();
        VKmerList kmerList;
        M incomingMsg;
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            String kmerString = incomingMsg.getInternalKmer().toString();
            forwardKmer.setFromStringBytes(kmerString.length(), kmerString.getBytes(), 0);
            reverseKmer.setReversedFromStringBytes(kmerString.length(), kmerString.getBytes(), 0);

            VKmer kmer = reverseKmer.compareTo(forwardKmer) > 0 ? forwardKmer : reverseKmer;
            if(!kmerMapper.containsKey(kmer)){
                kmerList = new VKmerList();
                kmerMapper.put(new VKmer(kmer), kmerList);
            } else{
                kmerList = kmerMapper.get(kmer);
                kmerMapper.put(kmer, kmerList);
            }
            kmerList.append(incomingMsg.getSourceVertexId());
        }
        
        return kmerMapper;
    }
    
    public void reduceKeyByInternalKmer(Map<VKmer, VKmerList> kmerMapper){
        for(VKmer key : kmerMapper.keySet()){
            VKmerList kmerList = kmerMapper.get(key);
            for(VKmer dest : kmerList){
                sendMsg(dest, outgoingMsg);
            }
        }
    }
    
    /**
     *  step 2: NON-FAKE send msg to FAKE vertex
     */
    public void sendMsgToFakeVertex(){
        if(!getVertexValue().isFakeVertex()){
            outgoingMsg.reset();
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
            sendMsg(fakeVertex, outgoingMsg);
        }
        voteToHalt();
    }
    
    /**
     * step 3:
     */
    public void mapReduceInFakeVertex(Iterator<M> msgIterator){
        // Mapper
        Map<VKmer, VKmerList> kmerMapper = mapKeyByInternalKmer(msgIterator);
        
        // Reducer
        reduceKeyByInternalKmer(kmerMapper);
        
        //delele self(fake vertex)
        fakeVertexExist = false;
        deleteVertex(fakeVertex);
    }
    
    @Override
    public void compute(Iterator<M> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            addFakeVertex("A");
        } else if(getSuperstep() == 2){
            sendMsgToFakeVertex();
        } else if(getSuperstep() == 3){
            mapReduceInFakeVertex(msgIterator);
        } else if(getSuperstep() == 4){
            broadcastKillself();
        } else if(getSuperstep() == 5){
            responseToDeadNode(msgIterator);
            voteToHalt();
        }
    }
}
