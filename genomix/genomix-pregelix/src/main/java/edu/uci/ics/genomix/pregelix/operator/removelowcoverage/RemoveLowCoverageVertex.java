package edu.uci.ics.genomix.pregelix.operator.removelowcoverage;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class RemoveLowCoverageVertex extends
    Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "RemoveLowCoverageVertex.kmerSize";
    public static final String MIN_AVERAGECOVERAGE = "RemoveLowCoverageVertex.minAverageCoverage";
    public static int kmerSize = -1;
    private static float minAverageCoverage = -1;
    
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if(minAverageCoverage == -1)
            minAverageCoverage = getContext().getConfiguration().getFloat(MIN_AVERAGECOVERAGE, 5);
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getVertexValue().getAverageCoverage() < minAverageCoverage)
            deleteVertex(getVertexId());
        else
            voteToHalt();
    }
    
    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(RemoveLowCoverageVertex.class.getSimpleName());
        job.setVertexClass(RemoveLowCoverageVertex.class);
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
