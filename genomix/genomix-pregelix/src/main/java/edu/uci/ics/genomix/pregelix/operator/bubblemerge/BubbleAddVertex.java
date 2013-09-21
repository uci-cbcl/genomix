package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.EnumSet;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;

/**
 * Testing tool: Add Bubble
 * Add a bubble to a "good" graph
 * @author anbangx
 *
 */
public class BubbleAddVertex extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static int kmerSize = -1;
   
    private VKmerBytesWritable majorVertexId = new VKmerBytesWritable("ACA"); //forward
    private VKmerBytesWritable middleVertexId = new VKmerBytesWritable("ATG"); //reverse
    private VKmerBytesWritable minorVertexId = new VKmerBytesWritable("TCA"); //forward
    private VKmerBytesWritable insertedBubble = new VKmerBytesWritable("ATA"); //reverse
    private VKmerBytesWritable internalKmerInNewBubble = new VKmerBytesWritable("ATG");
    private float coverageOfInsertedBubble = 1;
    private EDGETYPE majorToNewBubbleDir = EDGETYPE.FR;
    private EDGETYPE minorToNewBubbleDir = EDGETYPE.FR;
    
    private EdgeListWritable[] edges = new EdgeListWritable[4];
    
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
    }
   
    /**
     * Returns the edge dir for B->A when the A->B edge is type @dir
     */
    public byte mirrorDirection(byte dir) {
        switch (dir) {
            case MessageFlag.DIR_FF:
                return MessageFlag.DIR_RR;
            case MessageFlag.DIR_FR:
                return MessageFlag.DIR_FR;
            case MessageFlag.DIR_RF:
                return MessageFlag.DIR_RF;
            case MessageFlag.DIR_RR:
                return MessageFlag.DIR_FF;
            default:
                throw new RuntimeException("Unrecognized direction in flipDirection: " + dir);
        }
    }
    
    /**
     * add a bubble
     */
    @SuppressWarnings("unchecked")
    public void insertBubble(EdgeListWritable[] edges, VKmerBytesWritable insertedBubble, VKmerBytesWritable internalKmer){
        //add bubble vertex
        @SuppressWarnings("rawtypes")
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        VertexValueWritable vertexValue = new VertexValueWritable();
        /**
         * set the src vertex id
         */
        vertex.setVertexId(insertedBubble);
        /**
         * set the vertex value
         */
        vertexValue.setEdges(edges);
        vertexValue.setInternalKmer(internalKmer);
        vertexValue.setAverageCoverage(coverageOfInsertedBubble);
        vertexValue.setInternalKmer(internalKmerInNewBubble);

        vertex.setVertexValue(vertexValue);
        
        addVertex(insertedBubble, vertex);
    }
    
    public void addEdgeToInsertedBubble(EDGETYPE meToNewBubbleDir, VKmerBytesWritable insertedBubble){
        EdgeWritable newEdge = new EdgeWritable();
        newEdge.setKey(insertedBubble);
        newEdge.appendReadID(0);
        EDGETYPE newBubbleToMeDir = meToNewBubbleDir.mirror(); 
        getVertexValue().getEdgeList(newBubbleToMeDir).add(newEdge);
    }
    
    public void setupEdgeForInsertedBubble(){
        for (EDGETYPE et : EnumSet.allOf(EDGETYPE.class)) {
            edges[et.get()] = new EdgeListWritable();
        }
        edges[majorToNewBubbleDir.get()].add(majorVertexId);
        edges[minorToNewBubbleDir.get()].add(minorVertexId);
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1){
            if(getVertexId().equals(majorVertexId)){
                /** add edge pointing to insertedBubble **/
                addEdgeToInsertedBubble(majorToNewBubbleDir, insertedBubble);
            } 
            else if(getVertexId().equals(minorVertexId)){
                /** add edge pointing to insertedBubble **/
                addEdgeToInsertedBubble(minorToNewBubbleDir, insertedBubble);
            } 
            else if(getVertexId().equals(middleVertexId)){
                /** setup edges of insertedBubble**/
                setupEdgeForInsertedBubble();
                
                /** insert new bubble **/
                insertBubble(edges, insertedBubble, getVertexValue().getInternalKmer());
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BubbleAddVertex.class.getSimpleName());
        job.setVertexClass(BubbleAddVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(InitialGraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
