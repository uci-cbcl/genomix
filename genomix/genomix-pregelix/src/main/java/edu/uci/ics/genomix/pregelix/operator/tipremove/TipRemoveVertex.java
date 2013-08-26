package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.io.IOException;
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
 *  Remove tip or single node when l > constant
 */
public class TipRemoveVertex extends
        BasicGraphCleanVertex<MessageWritable> {
    private int length = -1;
    
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if(length == -1)
            length = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.TIP_REMOVE_MAX_LENGTH));
        if(incomingMsg == null)
            incomingMsg = new MessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(getSuperstep() == 1)
            StatisticsAggregator.preGlobalCounters.clear();
        else
            StatisticsAggregator.preGlobalCounters = BasicGraphCleanVertex.readStatisticsCounterResult(getContext().getConfiguration());
        counters.clear();
        getVertexValue().getCounters().clear();
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1){
            if(VertexUtil.isIncomingTipVertex(getVertexValue())){
            	if(getVertexValue().getKmerLength() <= length){
            	    sendSettledMsgToNextNode();
            		deleteVertex(getVertexId());
                    //set statistics counter: Num_RemovedTips
                    updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
                    getVertexValue().setCounters(counters);
            	}
            }
            else if(VertexUtil.isOutgoingTipVertex(getVertexValue())){
                if(getVertexValue().getKmerLength() <= length){
                    sendSettledMsgToPrevNode();
                    deleteVertex(getVertexId());
                    //set statistics counter: Num_RemovedTips
                    updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
                    getVertexValue().setCounters(counters);
                }
            }
            else if(VertexUtil.isSingleVertex(getVertexValue())){
                if(getVertexValue().getKmerLength() <= length){
                    deleteVertex(getVertexId());
                    //set statistics counter: Num_RemovedTips
                    updateStatisticsCounter(StatisticsCounter.Num_RemovedTips);
                    getVertexValue().setCounters(counters);
                }
            }
        }
        else if(getSuperstep() == 2){
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
            job = new PregelixJob(TipRemoveVertex.class.getSimpleName());
        else
            job = new PregelixJob(conf, TipRemoveVertex.class.getSimpleName());
        job.setVertexClass(TipRemoveVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
