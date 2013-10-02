package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.ReadIdListWritable;
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
 * Testing tool: Add Bridge
 * Add some noise data/vertice to "good" graph 
 * @author anbangx
 *
 */
public class BridgeAddVertex extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static int kmerSize = -1;
    private int length = -1;
    
    private VKmerBytesWritable upBridge = new VKmerBytesWritable("ATA");
    private VKmerBytesWritable downBridge = new VKmerBytesWritable("ACG");
    private VKmerBytesWritable insertedBridge = new VKmerBytesWritable("GTA");
    private EDGETYPE bridgeToUpDir = EDGETYPE.FR; 
    private EDGETYPE bridgeToDownDir = EDGETYPE.RF; 
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1) {
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        }
        if (length == -1)
            length = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.BRIDGE_REMOVE_MAX_LENGTH));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void insertBridge(EDGETYPE dirToUp, EdgeListWritable edgeListToUp, EDGETYPE dirToDown,
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
        edgeList.put(kmer, new ReadIdListWritable(Arrays.asList(new Long(0))));
        return edgeList;
    }
    
    public void addEdgeToInsertedBridge(EDGETYPE dir, VKmerBytesWritable insertedBridge){
        getVertexValue().getEdgeList(dir).put(insertedBridge, new ReadIdListWritable(Arrays.asList(new Long(0))));
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            if(getVertexId().toString().equals("ATA")){
                /** add edge pointing to inserted bridge **/
                EDGETYPE upToBridgeDir = bridgeToUpDir.mirror(); 
                addEdgeToInsertedBridge(upToBridgeDir, insertedBridge);
                
                /** insert bridge **/
                insertBridge(bridgeToUpDir, getEdgeListFromKmer(upBridge),
                        bridgeToDownDir, getEdgeListFromKmer(downBridge), 
                        insertedBridge);
            } 
            else if(getVertexId().toString().equals("ACG")){
                /** add edge pointing to new bridge **/
                EDGETYPE downToBridgeDir = bridgeToDownDir.mirror(); 
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
