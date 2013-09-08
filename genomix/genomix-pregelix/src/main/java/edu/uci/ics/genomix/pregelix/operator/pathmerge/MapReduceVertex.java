package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class MapReduceVertex<V extends VertexValueWritable, M extends PathMergeMessageWritable> extends
	BasicPathMergeVertex<V, M>{
    
    public static class KmerDir{
        public static final byte FORWARD = 1 << 0;
        public static final byte REVERSE = 1 << 1;
    }
    
    protected VKmerBytesWritable reverseKmer;
    protected Map<VKmerBytesWritable, VKmerListWritable> kmerMapper = new HashMap<VKmerBytesWritable, VKmerListWritable>();

    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(incomingMsg == null)
            incomingMsg = (M) new PathMergeMessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = (M) new PathMergeMessageWritable();
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
            destVertexId = new VKmerBytesWritable();
        if(tmpKmer == null)
            tmpKmer = new VKmerBytesWritable();
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
    
    public ArrayList<Byte> mapKeyByInternalKmer(Iterator<M> msgIterator){
        ArrayList<Byte> kmerDir = new ArrayList<Byte>();
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            String kmerString = incomingMsg.getInternalKmer().toString();
            tmpKmer.setByRead(kmerString.length(), kmerString.getBytes(), 0);
            reverseKmer.setByReadReverse(kmerString.length(), kmerString.getBytes(), 0);

            VKmerBytesWritable kmer = new VKmerBytesWritable();
            kmerList = new VKmerListWritable();
            if(reverseKmer.compareTo(tmpKmer) > 0){
                kmerDir.add(KmerDir.FORWARD);
                kmer.setAsCopy(tmpKmer);
            }
            else{
                kmerDir.add(KmerDir.REVERSE);
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
        return kmerDir;
    }
    
    public void reduceKeyByInternalKmer(){
        for(VKmerBytesWritable key : kmerMapper.keySet()){
            kmerList = kmerMapper.get(key);
            for(int i = 1; i < kmerList.getCountOfPosition(); i++){
                //send kill message
                outgoingMsg.setFlag(MessageFlag.KILL);
                destVertexId.setAsCopy(kmerList.getPosition(i));
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    public void finalVertexResponseToFakeVertex(Iterator<M> msgIterator){
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            inFlag = incomingMsg.getFlag();
            if(inFlag == MessageFlag.KILL){
                broadcaseKillself();
            }
        }
    }
    
    @Override
    public void compute(Iterator<M> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            addFakeVertex();
        }
        else if(getSuperstep() == 2){
            /** NON-FAKE and Final vertice send msg to FAKE vertex **/
            sendMsgToFakeVertex();
        } else if(getSuperstep() == 3){
            kmerMapper.clear();
            /** Mapper **/
            mapKeyByInternalKmer(msgIterator);
            /** Reducer **/
            reduceKeyByInternalKmer();
        } else if(getSuperstep() == 4){
            /** only for test single MapReduce job**/
            if(!msgIterator.hasNext() && getVertexValue().getState() == State.IS_FAKE){
                fakeVertexExist = false;
                deleteVertex(fakeVertex);
            }
            finalVertexResponseToFakeVertex(msgIterator);
        } else if(getSuperstep() == 5){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(isResponseKillMsg())
                    responseToDeadVertex();
            }
            voteToHalt();
        }
    }
}
