package edu.uci.ics.genomix.pregelix.testhelper;

import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;
import edu.uci.ics.genomix.pregelix.operator.DeBruijnGraphCleanVertex;
import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.VKmerList;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.util.BspUtils;

/**
 * Add tip
 */
public class TipAddVertex extends DeBruijnGraphCleanVertex<VertexValueWritable, MessageWritable> {
    public static int kmerSize = -1;
    public static final String SPLIT_NODE = "TipAddVertex.splitNode";
    public static final String INSERTED_TIP = "TipAddVertex.insertedTip";
    public static final String TIP_TO_SPLIT_EDGETYPE = "TipAddVertex.tipToSplitDir";

    private VKmer splitNode = null;
    private VKmer insertedTip = null;
    private EDGETYPE tipToSplitEdgetype = null;

    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
        if (splitNode == null) {
            splitNode = new VKmer(getContext().getConfiguration().get(SPLIT_NODE));
        }
        if (insertedTip == null) {
            insertedTip = new VKmer(getContext().getConfiguration().get(INSERTED_TIP));
        }
        if (tipToSplitEdgetype == null) {
            tipToSplitEdgetype = EDGETYPE.fromByte(Byte.parseByte(getContext().getConfiguration().get(
                    TIP_TO_SPLIT_EDGETYPE)));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void insertTip(EDGETYPE dir, VKmerList edges, VKmer insertedTip) {
        Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
        vertex.getMsgList().clear();
        vertex.getEdges().clear();

        VertexValueWritable vertexValue = new VertexValueWritable();
        /**
         * set the src vertex id
         */
        vertex.setVertexId(insertedTip);
        /**
         * set the vertex value
         */
        vertexValue.setEdges(dir, edges);
        vertex.setVertexValue(vertexValue);

        addVertex(insertedTip, vertex);
    }

    public VKmerList getEdgesFromKmer(VKmer kmer) {
        VKmerList edges = new VKmerList();
        edges.append(kmer);
        return edges;
    }

    public void addEdgeToInsertedTip(EDGETYPE dir, VKmer insertedTip) {
        getVertexValue().getEdges(dir).append(insertedTip);
    }

    /**
     * create a new vertex point to split node
     */
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if (getVertexId().equals(splitNode)) {
                /** add edge pointing to insertedTip **/
                addEdgeToInsertedTip(tipToSplitEdgetype, insertedTip);
                /** insert tip **/
                EDGETYPE splitToTipDir = tipToSplitEdgetype.mirror();
                insertTip(splitToTipDir, getEdgesFromKmer(splitNode), insertedTip);
            }
        }
        voteToHalt();
    }

}
