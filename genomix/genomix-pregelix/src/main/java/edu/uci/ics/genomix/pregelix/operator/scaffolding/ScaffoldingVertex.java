package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.io.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;

public class ScaffoldingVertex extends 
    NaiveBFSTraverseVertex{

    public class KmerListAndFlagList{
        private ArrayList<Boolean> flagList;
        private VKmerListWritable kmerList;
        
        public KmerListAndFlagList(){
            flagList = new ArrayList<Boolean>();
            kmerList = new VKmerListWritable();
        }
        
        public void set(KmerListAndFlagList kmerAndflag){
            flagList.clear();
            kmerList.reset();
            flagList.addAll(kmerAndflag.getFlagList());
            kmerList.appendList(kmerAndflag.getKmerList());
        }
        
        public int size(){
            return flagList.size();
        }
        
        public ArrayList<Boolean> getFlagList() {
            return flagList;
        }

        public void setFlagList(ArrayList<Boolean> flagList) {
            this.flagList = flagList;
        }

        public VKmerListWritable getKmerList() {
            return kmerList;
        }

        public void setKmerList(VKmerListWritable kmerList) {
            this.kmerList = kmerList;
        }
        
    }
    private ArrayList<Boolean> flagList = new ArrayList<Boolean>();
    private KmerListAndFlagList kmerListAndflagList = new KmerListAndFlagList();
    public static Map<Long, KmerListAndFlagList> scaffoldingMap = new HashMap<Long, KmerListAndFlagList>();
    
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
    
    public void addStartReadsToScaffoldingMap(){
        boolean isflip = false;
        for(PositionWritable pos : getVertexValue().getStartReads()){
            long readId = pos.getReadId();
            if(scaffoldingMap.containsKey(readId)){
                kmerList.setCopy(scaffoldingMap.get(readId).getKmerList());
                kmerList.append(getVertexId());
                flagList.clear();
                flagList.addAll(scaffoldingMap.get(readId).getFlagList());
                flagList.add(isflip);
            } else{
                kmerList.reset();
                kmerList.append(getVertexId());
                flagList.clear();
                flagList.add(isflip);
            }
            kmerListAndflagList.setKmerList(kmerList);
            kmerListAndflagList.setFlagList(flagList);
            scaffoldingMap.put(readId, kmerListAndflagList);
        }
    }
    
    public void addEndReadsToScaffoldingMap(){
        boolean isflip = true;
        for(PositionWritable pos : getVertexValue().getEndReads()){
            long readId = pos.getReadId();
            if(scaffoldingMap.containsKey(readId)){
                kmerList.setCopy(scaffoldingMap.get(readId).getKmerList());
                kmerList.append(getVertexId());
                flagList.clear();
                flagList.addAll(scaffoldingMap.get(readId).getFlagList());
                flagList.add(isflip);
            } else{
                kmerList.reset();
                kmerList.append(getVertexId());
                flagList.clear();
                flagList.add(isflip);
            }
            kmerListAndflagList.setKmerList(kmerList);
            kmerListAndflagList.setFlagList(flagList);
            scaffoldingMap.put(readId, kmerListAndflagList);
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            /** add a fake vertex **/
            addFakeVertex();
            /** grouped by 5' readId **/
            addStartReadsToScaffoldingMap();
            addEndReadsToScaffoldingMap();
            
            voteToHalt();
        } else if(getSuperstep() == 2){
            /** process scaffoldingMap **/
            for(Long readId : scaffoldingMap.keySet()){
                ////////////
                PositionWritable nodeId = new PositionWritable();
                nodeId.set((byte) 0, 1, 0);
                PositionListWritable nodeIdList = new PositionListWritable();
                nodeIdList.append(nodeId);
                ////////////
                kmerListAndflagList.set(scaffoldingMap.get(readId));
                if(kmerListAndflagList.size() == 2){
                    initiateSrcAndDestNode(kmerListAndflagList.kmerList, kmerListAndflagList.flagList, nodeIdList);
                    sendMsg(srcNode, outgoingMsg);
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
