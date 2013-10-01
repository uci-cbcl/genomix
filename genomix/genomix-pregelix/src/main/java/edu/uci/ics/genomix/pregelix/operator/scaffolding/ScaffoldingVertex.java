package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.common.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.common.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.common.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.message.BFSTraverseMessageWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

/**
 * Graph clean pattern: Scaffolding
 * @author anbangx
 *
 */
public class ScaffoldingVertex extends 
    BFSTraverseVertex{

    public static class SearchInfo implements Writable{
        private VKmerBytesWritable kmer;
        private boolean flip;
        
        public SearchInfo(VKmerBytesWritable otherKmer, boolean flip){
            this.kmer.setAsCopy(otherKmer);
            this.flip = flip;
        }
        
        public VKmerBytesWritable getKmer() {
            return kmer;
        }

        public void setKmer(VKmerBytesWritable kmer) {
            this.kmer = kmer;
        }

        public boolean isFlip() {
            return flip;
        }

        public void setFlip(boolean flip) {
            this.flip = flip;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            kmer.write(out);
            out.writeBoolean(flip);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            kmer.readFields(in);
            flip = in.readBoolean();
        }
    }
    
    public static int MIN_TRAVERSAL_LENGTH = 20;
    public static int MAX_TRAVERSAL_LENGTH = 100;
//    private ArrayListWritable<BooleanWritable> flagList = new ArrayListWritable<BooleanWritable>();
//    private KmerListAndFlagListWritable kmerListAndflagList = new KmerListAndFlagListWritable();
//    private HashMapWritable<VLongWritable, KmerListAndFlagListWritable> scaffoldingMap = new HashMapWritable<VLongWritable, KmerListAndFlagListWritable>();
//    private ArrayListWritable<SearchInfo> searchInfoList = new ArrayListWritable<SearchInfo>();
    private HashMapWritable<VLongWritable, ArrayListWritable<SearchInfo>> scaffoldingMap = new HashMapWritable<VLongWritable, ArrayListWritable<SearchInfo>>();
    
    @Override
    public void initVertex() {
        super.initVertex();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
//        else
//            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        if(getSuperstep() == 1)
            ScaffoldingAggregator.preScaffoldingMap.clear();
        else if(getSuperstep() == 2)
            ScaffoldingAggregator.preScaffoldingMap = readScaffoldingMapResult(getContext().getConfiguration());
        counters.clear();
        scaffoldingMap.clear();
        getVertexValue().getCounters().clear();
        getVertexValue().getScaffoldingMap().clear();
    }
    
    public void addStartReadsToScaffoldingMap(){
        SearchInfo searchInfo;
        ArrayListWritable<SearchInfo> searchInfoList;
        boolean isflip = false;
        for(PositionWritable pos : getVertexValue().getStartReads()){
            long readId = pos.getReadId();
            if(scaffoldingMap.containsKey(readId)){
                searchInfoList = scaffoldingMap.get(readId);
            } else{
                searchInfoList = new ArrayListWritable<SearchInfo>();
            }
            searchInfo = new SearchInfo(getVertexId(), isflip);
            searchInfoList.add(searchInfo);
            scaffoldingMap.put(new VLongWritable(readId), searchInfoList);
        }
    }
    
    public void addEndReadsToScaffoldingMap(){
        SearchInfo searchInfo;
        ArrayListWritable<SearchInfo> searchInfoList;
        boolean isflip = true;
        for(PositionWritable pos : getVertexValue().getEndReads()){
            long readId = pos.getReadId();
            if(scaffoldingMap.containsKey(readId)){
                searchInfoList = scaffoldingMap.get(readId);
            } else{
                searchInfoList = new ArrayListWritable<SearchInfo>();
            }
            searchInfo = new SearchInfo(getVertexId(), isflip);
            searchInfoList.add(searchInfo);
            scaffoldingMap.put(new VLongWritable(readId), searchInfoList);
        }
    }
    
    public boolean isInRange(int traversalLength){
        return traversalLength < MAX_TRAVERSAL_LENGTH && traversalLength > MIN_TRAVERSAL_LENGTH;
    }
    
    @Override
    public void compute(Iterator<BFSTraverseMessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            // add a fake vertex 
            addFakeVertex("A");
            // grouped by 5'/~5' readId in aggregator
            addStartReadsToScaffoldingMap();
            addEndReadsToScaffoldingMap();
            getVertexValue().setScaffoldingMap(scaffoldingMap);
            
            voteToHalt();
        } else if(getSuperstep() == 2){
            // fake vertex process scaffoldingMap 
            ArrayListWritable<SearchInfo> searchInfoList;
            for(VLongWritable readId : ScaffoldingAggregator.preScaffoldingMap.keySet()){
                searchInfoList = ScaffoldingAggregator.preScaffoldingMap.get(readId);
                if(searchInfoList.size() != 2)
                    throw new IllegalStateException("The size of SearchInfoList should be 2, but here its size " +
                    		"is " + searchInfoList.size() + "!");
                if(searchInfoList.size() == 2){
                    outgoingMsg.reset();
                    VKmerBytesWritable srcNode = initiateSrcAndDestNode(readId.get(), searchInfoList);
                    sendMsg(srcNode, outgoingMsg);
                }
            }
            
            deleteVertex(getVertexId());
        } else if(getSuperstep() >= 3){
            BFSTraverseMessageWritable incomingMsg;
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                if(incomingMsg.isTraverseMsg()){
                    // check if find destination 
                    if(incomingMsg.getSeekedVertexId().equals(getVertexId())){
                        int traversalLength = incomingMsg.getPathList().getCountOfPosition();
                        if(isValidDestination(incomingMsg) && isInRange(traversalLength)){
                            // final step to process BFS -- pathList and dirList
                            finalProcessBFS(incomingMsg);
                            // send message to all the path nodes to add this common readId
                            sendMsgToPathNodeToAddCommondReadId(incomingMsg);
                            //set statistics counter: Num_RemovedLowCoverageNodes
                            incrementCounter(StatisticsCounter.Num_Scaffodings);
                            getVertexValue().setCounters(counters);
                        }
                        else{
                            //continue to BFS
                            broadcaseBFSTraverse(incomingMsg);
                        }
                    } else {
                        //begin(step == 3) or continue(step > 3) to BFS
                        broadcaseBFSTraverse(incomingMsg);
                    }
                } else{
                    // append common readId to the corresponding edge
                    appendCommonReadId(incomingMsg);
                }
            }
            voteToHalt();
        }
    }
    
    public static HashMapWritable<VLongWritable, ArrayListWritable<SearchInfo>> readScaffoldingMapResult(Configuration conf) {
        try {
            VertexValueWritable value = (VertexValueWritable) IterationUtils
                    .readGlobalAggregateValue(conf, BspUtils.getJobId(conf));
            return value.getScaffoldingMap();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null, ScaffoldingVertex.class));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf, Class<? extends BasicGraphCleanVertex<? extends VertexValueWritable, ? extends MessageWritable>> vertexClass) throws IOException {
        PregelixJob job = BasicGraphCleanVertex.getConfiguredJob(conf, vertexClass);
        job.setGlobalAggregatorClass(ScaffoldingAggregator.class);
        return job;
    }
}
