package edu.uci.ics.genomix.pregelix.operator.removelowcoverage;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.operator.bridgeremove.BridgeRemoveVertex;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class RemoveLowCoverageVertex extends
    BasicGraphCleanVertex<MessageWritable> {
    public static int kmerSize = -1;
    private static float minAverageCoverage = -1;
    
    private Set<VKmerBytesWritable> deadNodeSet = new HashSet<VKmerBytesWritable>();
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if(minAverageCoverage == -1)
            minAverageCoverage = Float.parseFloat(getContext().getConfiguration().get(GenomixJobConf.REMOVE_LOW_COVERAGE_MAX_COVERAGE));
        if(incomingMsg == null)
            incomingMsg = new MessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable();
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1){
            if(getVertexValue().getAvgCoverage() <= minAverageCoverage){
                broadcaseKillself();
                deadNodeSet.add(getVertexId());
            }
            else
                voteToHalt();
        } else if(getSuperstep() == 2){
            if(deadNodeSet.contains(getVertexId()))
                deleteVertex(getVertexId());
            else{
                while(msgIterator.hasNext()){
                    incomingMsg = msgIterator.next();
                    if(isResponseKillMsg())
                        responseToDeadVertex();
                }
                voteToHalt();
            }
        } 
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf) throws IOException {
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(RemoveLowCoverageVertex.class.getSimpleName());
        else
            job = new PregelixJob(conf, RemoveLowCoverageVertex.class.getSimpleName());
        job.setVertexClass(RemoveLowCoverageVertex.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
