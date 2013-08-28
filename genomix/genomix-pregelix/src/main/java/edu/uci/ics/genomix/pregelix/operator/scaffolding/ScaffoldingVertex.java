package edu.uci.ics.genomix.pregelix.operator.scaffolding;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.ArrayListWritable;
import edu.uci.ics.genomix.pregelix.io.BFSTraverseMessageWritable;
import edu.uci.ics.genomix.pregelix.io.HashMapWritable;
import edu.uci.ics.genomix.pregelix.io.KmerListAndFlagListWritable;
import edu.uci.ics.genomix.pregelix.io.VLongWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;

public class ScaffoldingVertex extends 
    BFSTraverseVertex{

    private ArrayListWritable<BooleanWritable> flagList = new ArrayListWritable<BooleanWritable>();
    private KmerListAndFlagListWritable kmerListAndflagList = new KmerListAndFlagListWritable();
    private HashMapWritable<VLongWritable, KmerListAndFlagListWritable> scaffoldingMap = new HashMapWritable<VLongWritable, KmerListAndFlagListWritable>();
    
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if (maxIteration < 0)
            maxIteration = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.GRAPH_CLEAN_MAX_ITERATIONS));
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
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        else
            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
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
        boolean isflip = false;
        for(PositionWritable pos : getVertexValue().getStartReads()){
            long readId = pos.getReadId();
            if(scaffoldingMap.containsKey(readId)){
                kmerList.setCopy(scaffoldingMap.get(readId).getKmerList());
                kmerList.append(getVertexId());
                flagList.clear();
                flagList.addAll(scaffoldingMap.get(readId).getFlagList());
                flagList.add(new BooleanWritable(isflip));
            } else{
                kmerList.reset();
                kmerList.append(getVertexId());
                flagList.clear();
                flagList.add(new BooleanWritable(isflip));
            }
            kmerListAndflagList.setKmerList(kmerList);
            kmerListAndflagList.setFlagList(flagList);
            scaffoldingMap.put(new VLongWritable(readId), kmerListAndflagList);
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
                flagList.add(new BooleanWritable(isflip));
            } else{
                kmerList.reset();
                kmerList.append(getVertexId());
                flagList.clear();
                flagList.add(new BooleanWritable(isflip));
            }
            kmerListAndflagList.setKmerList(kmerList);
            kmerListAndflagList.setFlagList(flagList);
            scaffoldingMap.put(new VLongWritable(readId), kmerListAndflagList);
        }
    }
    
    @Override
    public void compute(Iterator<BFSTraverseMessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            // add a fake vertex 
            addFakeVertex();
            // grouped by 5'/~5' readId in aggregator
            addStartReadsToScaffoldingMap();
            addEndReadsToScaffoldingMap();
            getVertexValue().setScaffoldingMap(scaffoldingMap);
            
            voteToHalt();
        } else if(getSuperstep() == 2){
            // process scaffoldingMap 
            for(VLongWritable readId : ScaffoldingAggregator.preScaffoldingMap.keySet()){
                kmerListAndflagList.set(ScaffoldingAggregator.preScaffoldingMap.get(readId));
                if(kmerListAndflagList.size() == 2){
                    initiateSrcAndDestNode(kmerListAndflagList.getKmerList(), commonReadId, kmerListAndflagList.getFlagList().get(0).get(),
                            kmerListAndflagList.getFlagList().get(1).get());
                    sendMsg(srcNode, outgoingMsg);
                }
            }
            
            deleteVertex(getVertexId());
        } else if(getSuperstep() == 3){
            if(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                // begin to BFS
                initialBroadcaseBFSTraverse();
            }
            voteToHalt();
        } else if(getSuperstep() > 3){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                if(incomingMsg.isTraverseMsg()){
                    // check if find destination 
                    if(incomingMsg.getSeekedVertexId().equals(getVertexId())){
                        if(isValidDestination()){
                            // final step to process BFS -- pathList and dirList
                            finalProcessBFS();
                            // send message to all the path nodes to add this common readId
                            sendMsgToPathNodeToAddCommondReadId();
                            //set statistics counter: Num_RemovedLowCoverageNodes
                            updateStatisticsCounter(StatisticsCounter.Num_Scaffodings);
                            getVertexValue().setCounters(counters);
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
                    // append common readId to the corresponding edge
                    appendCommonReadId();
                }
            }
            voteToHalt();
        }
    }
    
    public static HashMapWritable<VLongWritable, KmerListAndFlagListWritable> readScaffoldingMapResult(Configuration conf) {
        try {
            VertexValueWritable value = (VertexValueWritable) IterationUtils
                    .readGlobalAggregateValue(conf, BspUtils.getJobId(conf));
            return value.getScaffoldingMap();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf) throws IOException {
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(ScaffoldingVertex.class.getSimpleName());
        else
            job = new PregelixJob(conf, ScaffoldingVertex.class.getSimpleName());
        job.setVertexClass(ScaffoldingVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
