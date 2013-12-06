package edu.uci.ics.genomix.pregelix.operator.test;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.types.VKmerList;
import edu.uci.ics.genomix.pregelix.base.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;
import edu.uci.ics.genomix.pregelix.base.VertexValueWritable;
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
        try {
            GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void insertBridge(EDGETYPE dirToUp, VKmerList edgesToUp, EDGETYPE dirToDown, VKmerList edgesToDown,
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
        vertexValue.setEdges(dirToUp, edgesToUp);
        vertexValue.setEdges(dirToDown, edgesToDown);
        vertex.setVertexValue(vertexValue);

        addVertex(insertedBridge, vertex);
    }

    public VKmerList getEdgesFromKmer(VKmer kmer) {
        VKmerList edges = new VKmerList();
        edges.append(kmer);
        return edges;
    }

    public void addEdgeToInsertedBridge(EDGETYPE dir, VKmer insertedBridge) {
        if (!getVertexValue().getEdges(dir).contains(insertedBridge))
            getVertexValue().getEdges(dir).append(insertedBridge);
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
                insertBridge(bridgeToUpDir, getEdgesFromKmer(upBridge), bridgeToDownDir, getEdgesFromKmer(downBridge),
                        insertedBridge);
            } else if (getVertexId().toString().equals("ACG")) {
                /** add edge pointing to new bridge **/
                EDGETYPE downToBridgeDir = bridgeToDownDir.mirror();
                addEdgeToInsertedBridge(downToBridgeDir, insertedBridge);
            }
        }
        voteToHalt();
    }

}
