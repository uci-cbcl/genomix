package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class MapReduceVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
	BasicPathMergeVertex<V, M>{
    
    protected VKmerBytesWritable forwardKmer = new VKmerBytesWritable();
    protected VKmerBytesWritable reverseKmer = new VKmerBytesWritable();

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
    
    public Map<VKmerBytesWritable, VKmerListWritable> mapKeyByInternalKmer(Iterator<M> msgIterator){
        Map<VKmerBytesWritable, VKmerListWritable> kmerMapper = new HashMap<VKmerBytesWritable, VKmerListWritable>();
        VKmerListWritable kmerList;
        M incomingMsg;
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            String kmerString = incomingMsg.getInternalKmer().toString();
            forwardKmer.setByRead(kmerString.length(), kmerString.getBytes(), 0);
            reverseKmer.setByReadReverse(kmerString.length(), kmerString.getBytes(), 0);

            VKmerBytesWritable kmer = reverseKmer.compareTo(forwardKmer) > 0 ? forwardKmer : reverseKmer;
            if(!kmerMapper.containsKey(kmer)){
                kmerList = new VKmerListWritable();
                kmerMapper.put(new VKmerBytesWritable(kmer), kmerList);
            } else{
                kmerList = kmerMapper.get(kmer);
                kmerMapper.put(kmer, kmerList);
            }
            kmerList.append(incomingMsg.getSourceVertexId());
        }
        
        return kmerMapper;
    }
    
    public void reduceKeyByInternalKmer(Map<VKmerBytesWritable, VKmerListWritable> kmerMapper){
        for(VKmerBytesWritable key : kmerMapper.keySet()){
            VKmerListWritable kmerList = kmerMapper.get(key);
            for(VKmerBytesWritable dest : kmerList){
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
        Map<VKmerBytesWritable, VKmerListWritable> kmerMapper = mapKeyByInternalKmer(msgIterator);
        
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
