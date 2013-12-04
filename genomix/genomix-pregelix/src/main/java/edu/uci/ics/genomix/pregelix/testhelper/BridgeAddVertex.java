package edu.uci.ics.genomix.pregelix.testhelper;

import java.util.Arrays;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * Testing tool: Add Bridge
 * Add some noise data/vertice to "good" graph
 * 
 * @author anbangx
 */
public class BridgeAddVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {
    public static int kmerSize = -1;

    private VKmer upBridge = new VKmer("ATA");
    private VKmer downBridge = new VKmer("ACG");
    private VKmer insertedBridge = new VKmer("GTA");
    private EDGETYPE bridgeToUpDir = EDGETYPE.FR;
    private EDGETYPE bridgeToDownDir = EDGETYPE.RF;

    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1) {
            kmerSize = 3; // Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        }
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void insertBridge(EDGETYPE dirToUp, EdgeMap edgeListToUp, EDGETYPE dirToDown, EdgeMap edgeListToDown,
            VKmer insertedBridge) {
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
        vertexValue.setEdgeMap(dirToUp, edgeListToUp);
        vertexValue.setEdgeMap(dirToDown, edgeListToDown);
        vertex.setVertexValue(vertexValue);

        addVertex(insertedBridge, vertex);
    }

    public EdgeMap getEdgeListFromKmer(VKmer kmer) {
        EdgeMap edgeList = new EdgeMap();
        edgeList.put(kmer, new ReadIdSet(Arrays.asList(new Long(0))));
        return edgeList;
    }

    public void addEdgeToInsertedBridge(EDGETYPE dir, VKmer insertedBridge) {
        getVertexValue().getEdgeMap(dir).put(insertedBridge, new ReadIdSet(Arrays.asList(new Long(0))));
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if (getVertexId().toString().equals("ATA")) {
                /** add edge pointing to inserted bridge **/
                EDGETYPE upToBridgeDir = bridgeToUpDir.mirror();
                addEdgeToInsertedBridge(upToBridgeDir, insertedBridge);

                /** insert bridge **/
                insertBridge(bridgeToUpDir, getEdgeListFromKmer(upBridge), bridgeToDownDir,
                        getEdgeListFromKmer(downBridge), insertedBridge);
            } else if (getVertexId().toString().equals("ACG")) {
                /** add edge pointing to new bridge **/
                EDGETYPE downToBridgeDir = bridgeToDownDir.mirror();
                addEdgeToInsertedBridge(downToBridgeDir, insertedBridge);
            }
        }
        voteToHalt();
    }

}
