package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
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
 * Naive Algorithm for path merge graph
 */
public class BridgeAddVertex extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static int kmerSize = -1;
    private int length = -1;
    
    private VKmerBytesWritable upBridge = new VKmerBytesWritable("ATA");
    private VKmerBytesWritable downBridge = new VKmerBytesWritable("ACG");
    private VKmerBytesWritable insertedBridge = new VKmerBytesWritable("GTA");
    private byte bridgeToUpDir = MessageFlag.DIR_FR; 
    private byte bridgeToDownDir = MessageFlag.DIR_RF; 
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1) {
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
            KmerBytesWritable.setGlobalKmerLength(kmerSize);
        }
        if (length == -1)
            length = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.BRIDGE_REMOVE_MAX_LENGTH));
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void insertBridge(byte dirToUp, EdgeListWritable edgeListToUp, byte dirToDown,
            EdgeListWritable edgeListToDown, VKmerBytesWritable insertedBridge){
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();
        
        VertexValueWritable vertexValue = new VertexValueWritable(); //kmerSize
        /**
         * set the src vertex id
         */
        vertex.setVertexId(insertedBridge);
        /**
         * set the vertex value
         */
        vertexValue.setEdgeList(dirToUp, edgeListToUp);
        vertexValue.setEdgeList(dirToDown, edgeListToDown);
        vertex.setVertexValue(vertexValue);
        
        addVertex(insertedBridge, vertex);
    }
    
    public EdgeListWritable getEdgeListFromKmer(VKmerBytesWritable kmer){
        EdgeListWritable edgeList = new EdgeListWritable();
        EdgeWritable newEdge = new EdgeWritable();
        newEdge.setKey(kmer);
        newEdge.appendReadID(0);
        edgeList.add(newEdge);
        return edgeList;
    }
    
    public void addEdgeToInsertedBridge(byte dir, VKmerBytesWritable insertedBridge){
        EdgeWritable newEdge = new EdgeWritable();
        newEdge.setKey(insertedBridge);
        newEdge.appendReadID(0);
        getVertexValue().getEdgeList(dir).add(newEdge);
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
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            if(getVertexId().toString().equals("ATA")){
                /** add edge pointing to inserted bridge **/
                byte upToBridgeDir = mirrorDirection(bridgeToUpDir);
                addEdgeToInsertedBridge(upToBridgeDir, insertedBridge);
                
                /** insert bridge **/
                insertBridge(bridgeToUpDir, getEdgeListFromKmer(upBridge),
                        bridgeToDownDir, getEdgeListFromKmer(downBridge), 
                        insertedBridge);
            } 
            else if(getVertexId().toString().equals("ACG")){
                /** add edge pointing to new bridge **/
                byte downToBridgeDir = mirrorDirection(bridgeToDownDir);
                addEdgeToInsertedBridge(downToBridgeDir, insertedBridge);
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BridgeAddVertex.class.getSimpleName());
        job.setVertexClass(BridgeAddVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
