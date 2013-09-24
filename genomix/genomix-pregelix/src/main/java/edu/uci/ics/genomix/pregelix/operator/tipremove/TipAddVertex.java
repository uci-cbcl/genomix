package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;

/**
 * @author anbangx
 * Add tip 
 */
public class TipAddVertex extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static int kmerSize = -1;
   
    private VKmerBytesWritable splitNode = new VKmerBytesWritable("CTA");
    private VKmerBytesWritable insertedTip = new VKmerBytesWritable("AGC");
    private EDGETYPE tipToSplitDir = EDGETYPE.FR;
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void insertTip(EDGETYPE dir, EdgeListWritable edgeList, VKmerBytesWritable insertedTip){
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        
        VertexValueWritable vertexValue = new VertexValueWritable(); //kmerSize
        /**
         * set the src vertex id
         */
        vertex.setVertexId(insertedTip);
        /**
         * set the vertex value
         */
        vertexValue.setEdgeList(dir, edgeList);
        vertex.setVertexValue(vertexValue);
        
        addVertex(insertedTip, vertex);
    }
    
    public EdgeListWritable getEdgeListFromKmer(VKmerBytesWritable kmer){
        EdgeListWritable edgeList = new EdgeListWritable();
        EdgeWritable newEdge = new EdgeWritable();
        newEdge.setKey(kmer);
        newEdge.appendReadID(0);
        edgeList.add(newEdge);
        return edgeList;
    }
    
    public void addEdgeToInsertedTip(EDGETYPE dir, VKmerBytesWritable insertedTip){
        EdgeWritable newEdge = new EdgeWritable();
        newEdge.setKey(insertedTip);
        newEdge.appendReadID(0);
        getVertexValue().getEdgeList(dir).add(newEdge);
    }
    
    /**
     * create a new vertex point to split node
     */
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1){
            if(getVertexId().equals(splitNode)){
                /** add edge pointing to insertedTip **/
                addEdgeToInsertedTip(tipToSplitDir, insertedTip);
                /** insert tip **/
                EDGETYPE splitToTipDir = tipToSplitDir.mirror();
                insertTip(splitToTipDir, getEdgeListFromKmer(splitNode), insertedTip);
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(TipAddVertex.class.getSimpleName());
        job.setVertexClass(TipAddVertex.class);
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
