package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.aggregator.StatisticsAggregator;
import edu.uci.ics.genomix.pregelix.type.StatisticsCounter;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: MessageWritable
 * 
 * DNA:
 * A: 00
 * C: 01
 * G: 10
 * T: 11
 * 
 * succeed node
 *  A 00000001 1
 *  G 00000010 2
 *  C 00000100 4
 *  T 00001000 8
 * precursor node
 *  A 00010000 16
 *  G 00100000 32
 *  C 01000000 64
 *  T 10000000 128
 *  
 * For example, ONE LINE in input file: 00,01,10    0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 * Naive Algorithm for path merge graph
 */
public class BridgeRemoveVertex extends
    BasicGraphCleanVertex<MessageWritable> {
    private int length = -1;

    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
   
    /**
     * initiate kmerSize, maxIteration
     */
    @Override
    public void initVertex() {
        super.initVertex();
        if(length == -1)
            length = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.BRIDGE_REMOVE_MAX_LENGTH));
        if(incomingMsg == null)
            incomingMsg = new MessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        receivedMsgList.clear();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        else
            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    /**
     * aggregate received messages
     */
    public void aggregateReceivedMsgs(Iterator<MessageWritable> msgIterator){
        int i = 0;
        while (msgIterator.hasNext()) {
            if(i == 3)
                break;
            receivedMsgList.add(msgIterator.next());
            i++;
        }
    }
    
    /**
     * remove bridge pattern
     */
    public void removeBridge(){
        if(receivedMsgList.size() == 2){
            if(getVertexValue().getKmerLength() <= length 
                    && getVertexValue().getDegree() == 2){
                broadcaseKillself();
                //set statistics counter: Num_RemovedBridges
                updateStatisticsCounter(StatisticsCounter.Num_RemovedBridges);
                getVertexValue().setCounters(counters);
            }
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            //filter bridge vertex
            if(VertexUtil.isUpBridgeVertex(getVertexValue())){
                sendSettledMsgToAllNextNodes(getVertexValue());
            }
            else if(VertexUtil.isDownBridgeVertex(getVertexValue())){
                sendSettledMsgToAllPrevNodes(getVertexValue());
            }
        }
        else if (getSuperstep() == 2){
            //aggregate received messages
            aggregateReceivedMsgs(msgIterator);
            //detect bridge pattern and remove it
            removeBridge();
        }
        else if(getSuperstep() == 3){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                responseToDeadVertex();
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf) throws IOException {
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(BridgeRemoveVertex.class.getSimpleName());
        else
            job = new PregelixJob(conf, BridgeRemoveVertex.class.getSimpleName());
        job.setVertexClass(BridgeRemoveVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
