package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.BFSTraverseMessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class ScaffoldingVertex extends 
    BFSTraverseVertex{

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
    public void compute(Iterator<BFSTraverseMessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            /** add a fake vertex **/
            addFakeVertex();
            /** grouped by 5'/~5' readId **/
            addStartReadsToScaffoldingMap();
            addEndReadsToScaffoldingMap();
            
            voteToHalt();
        } else if(getSuperstep() == 2){
            /** process scaffoldingMap **/
            for(Long readId : scaffoldingMap.keySet()){
                kmerListAndflagList.set(scaffoldingMap.get(readId));
                if(kmerListAndflagList.size() == 2){
                    initiateSrcAndDestNode(kmerListAndflagList.kmerList, commonReadId, kmerListAndflagList.flagList.get(0),
                            kmerListAndflagList.flagList.get(1));
                    sendMsg(srcNode, outgoingMsg);
                }
            }
            
            deleteVertex(getVertexId());
        } else if(getSuperstep() == 3){
            if(msgIterator.hasNext()){
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
                            finalProcessBTS();
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
        PregelixJob job = new PregelixJob(ScaffoldingVertex.class.getSimpleName());
        job.setVertexClass(ScaffoldingVertex.class);
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
