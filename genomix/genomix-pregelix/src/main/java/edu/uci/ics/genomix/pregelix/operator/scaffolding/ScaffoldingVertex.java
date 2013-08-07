package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.io.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.pathmerge.MapReduceVertex;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;

public class ScaffoldingVertex extends 
    MapReduceVertex{

    public static Map<Long, VKmerListWritable> scaffoldingMap = new HashMap<Long, VKmerListWritable>();
    
    private HashMapWritable<VKmerBytesWritable, VKmerListWritable> traverseMap = new HashMapWritable<VKmerBytesWritable, VKmerListWritable>();
    
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(incomingMsg == null)
            incomingMsg = new MessageWritable(kmerSize);
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable(kmerSize);
        else
            outgoingMsg.reset(kmerSize);
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable(kmerSize);
        if(kmerList == null)
            kmerList = new VKmerListWritable();
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            /** add a fake vertex **/
            addFakeVertex();
            /** grouped by 5' readId **/ //TODO
            long mainReadId = getVertexValue().getStartReads().getPosition(0).getReadId();
            if(mainReadId != 0){ //empty or not
                if(scaffoldingMap.containsKey(mainReadId)){
                    kmerList.setCopy(scaffoldingMap.get(mainReadId));
                    kmerList.append(getVertexId());
                } else{
                    kmerList.reset();
                    kmerList.append(getVertexId());
                }
                scaffoldingMap.put(mainReadId, kmerList);
            }
            voteToHalt();
        } else if(getSuperstep() == 2){
            /** process scaffoldingMap **/
            for(Long readId : scaffoldingMap.keySet()){
                kmerList.setCopy(scaffoldingMap.get(readId));
                if(kmerList.getCountOfPosition() == 2){
                    outgoingMsg.setSeekedVertexId(kmerList.getPosition(1));
                    sendMsg(kmerList.getPosition(0), outgoingMsg);
                    outgoingMsg.setSeekedVertexId(kmerList.getPosition(0));
                    sendMsg(kmerList.getPosition(1), outgoingMsg);
                }
            }
            deleteVertex(getVertexId());
        } else if(getSuperstep() == 3){
            if(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                
                /** initiate the traverseMap in vertexValue **/
                kmerList.reset();
                kmerList.append(incomingMsg.getSeekedVertexId());
                traverseMap.clear();
                traverseMap.put(incomingMsg.getSeekedVertexId(), kmerList);
                getVertexValue().setTraverseMap(traverseMap);
                
                /** begin to traverse **/
                
            }
        }
    }
}
