package edu.uci.ics.genomix.pregelix.operator.bubblemerge;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;

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
public class BubbleAddVertex extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static int kmerSize = -1;
   
    private VKmerBytesWritable majorVertexId = new VKmerBytesWritable("ACA"); //forward
    private VKmerBytesWritable middleVertexId = new VKmerBytesWritable("ATG"); //reverse
    private VKmerBytesWritable minorVertexId = new VKmerBytesWritable("TCA"); //forward
    private VKmerBytesWritable insertedBubble = new VKmerBytesWritable("ATA"); //reverse
    private VKmerBytesWritable internalKmerInNewBubble = new VKmerBytesWritable("ATG");
    private float coverageOfInsertedBubble = 1;
    private byte majorToNewBubbleDir = MessageFlag.DIR_FR;
    private byte minorToNewBubbleDir = MessageFlag.DIR_FR;
    
    private EdgeListWritable[] edges = new EdgeListWritable[4];
    
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
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
    
    public void addEdgeToInsertedBubble(byte meToNewBubbleDir, VKmerBytesWritable insertedBubble){
        EdgeWritable newEdge = new EdgeWritable();
        newEdge.setKey(insertedBubble);
        newEdge.appendReadID(0);
        byte newBubbleToMeDir = mirrorDirection(meToNewBubbleDir);
        getVertexValue().getEdgeList(newBubbleToMeDir).add(newEdge);
    }
    
    public void setupEdgeForInsertedBubble(){
        for (byte d : DirectionFlag.values) {
            edges[d] = new EdgeListWritable();
        }
        edges[majorToNewBubbleDir].add(majorVertexId);
        edges[minorToNewBubbleDir].add(minorVertexId);
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
