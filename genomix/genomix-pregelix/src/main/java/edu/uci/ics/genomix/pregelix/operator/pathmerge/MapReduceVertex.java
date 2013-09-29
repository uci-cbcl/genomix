package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class MapReduceVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
	BasicPathMergeVertex<V, M>{
    
    protected VKmerBytesWritable forwardKmer;
    protected VKmerBytesWritable reverseKmer;
    protected Map<VKmerBytesWritable, VKmerListWritable> kmerMapper = new HashMap<VKmerBytesWritable, VKmerListWritable>();

    /**
     * initiate kmerSize, maxIteration
     */
    @SuppressWarnings("unchecked")
    @Override
    public void initVertex() {
        super.initVertex();
        if(outgoingMsg == null)
            outgoingMsg = (M) new PathMergeMessageWritable();
        else
            outgoingMsg.reset();
        if(reverseKmer == null)
            reverseKmer = new VKmerBytesWritable();
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
        if(forwardKmer == null)
            forwardKmer = new VKmerBytesWritable();
    }
    
    /**
     * 
     */
    public String generateString(int length){
        StringBuffer outputBuffer = new StringBuffer(length);
        for (int i = 0; i < length; i++){
           outputBuffer.append("A");
        }
        return outputBuffer.toString();
    }
    
    public void sendMsgToFakeVertex(){
        outgoingMsg.reset();
        if(!getVertexValue().isFakeVertex()){
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setInternalKmer(getVertexValue().getInternalKmer());
            sendMsg(fakeVertex, outgoingMsg);
            voteToHalt();
        }
    }
    
    public void mapKeyByInternalKmer(Iterator<M> msgIterator){
//        ArrayList<Byte> kmerDir = new ArrayList<Byte>();
        while(msgIterator.hasNext()){
            M incomingMsg = msgIterator.next();
            String kmerString = incomingMsg.getInternalKmer().toString();
            forwardKmer.setByRead(kmerString.length(), kmerString.getBytes(), 0);
            reverseKmer.setByReadReverse(kmerString.length(), kmerString.getBytes(), 0);

            VKmerBytesWritable kmer = new VKmerBytesWritable();
            VKmerListWritable kmerList = new VKmerListWritable();
            if(reverseKmer.compareTo(forwardKmer) > 0){
//                kmerDir.add(KmerDir.FORWARD);
                kmer.setAsCopy(forwardKmer);
            }
            else{
//                kmerDir.add(KmerDir.REVERSE);
                kmer.setAsCopy(reverseKmer);
            }
            if(!kmerMapper.containsKey(kmer)){
                //kmerList.reset();
                kmerList.append(incomingMsg.getSourceVertexId());
                kmerMapper.put(kmer, kmerList);
            } else{
                kmerList.setCopy(kmerMapper.get(kmer));
                kmerList.append(incomingMsg.getSourceVertexId());
                kmerMapper.put(kmer, kmerList);
            }
        }
    }
    
    public void reduceKeyByInternalKmer(){
        for(VKmerBytesWritable key : kmerMapper.keySet()){
            VKmerListWritable kmerList = kmerMapper.get(key);
            for(int i = 1; i < kmerList.getCountOfPosition(); i++){
                //send kill message
//                outgoingMsg.setFlag(MessageFlag.KILL);
                VKmerBytesWritable destVertexId = kmerList.getPosition(i);
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    public void finalVertexResponseToFakeVertex(Iterator<M> msgIterator){
        while(msgIterator.hasNext()){
            M incomingMsg = msgIterator.next();
            inFlag = incomingMsg.getFlag();
//            if(inFlag == MessageFlag.KILL){
//                broadcaseKillself();
//            }
        }
    }
    
    @Override
    public void compute(Iterator<M> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            addFakeVertex();
        }
        else if(getSuperstep() == 2){
            //NON-FAKE and Final vertice send msg to FAKE vertex
            sendMsgToFakeVertex();
        } else if(getSuperstep() == 3){
            kmerMapper.clear();
            //Mappe
            mapKeyByInternalKmer(msgIterator);
            //Reducer
            reduceKeyByInternalKmer();
        } else if(getSuperstep() == 4){
            // only for test single MapReduce job
            if(!msgIterator.hasNext() && getVertexValue().getState() == State.IS_FAKE){
                fakeVertexExist = false;
                deleteVertex(fakeVertex);
            }
            finalVertexResponseToFakeVertex(msgIterator);
        } else if(getSuperstep() == 5){
            while (msgIterator.hasNext()) {
                M incomingMsg = msgIterator.next();
//                if(isResponseKillMsg(incomingMsg))
//                    responseToDeadVertex(incomingMsg);
            }
            voteToHalt();
        }
    }
}
