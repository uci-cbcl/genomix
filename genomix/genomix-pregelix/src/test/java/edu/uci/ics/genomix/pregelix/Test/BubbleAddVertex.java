package edu.uci.ics.genomix.pregelix.Test;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.EDGETYPE;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NodeToVertexInputFormat;
import edu.uci.ics.genomix.pregelix.format.VertexToNodeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.MessageWritable;

/**
 * Testing tool: Add Bubble
 * Add a bubble to a "good" graph
 * 
 * @author anbangx
 */
public class BubbleAddVertex extends Vertex<VKmer, VertexValueWritable, NullWritable, MessageWritable> {
    public static int kmerSize = -1;

    private VKmer majorVertexId = new VKmer("AAT"); //forward
    private VKmer middleVertexId = new VKmer("ATA"); //forward
    private VKmer minorVertexId = new VKmer("CTA"); //forward
    private VKmer insertedBubble = new VKmer("ACA"); //forward
    private VKmer internalKmerInNewBubble = new VKmer("TTC");
    private float coverageOfInsertedBubble = 4;
    private long readId = 2;
    private EDGETYPE newBubbleToMajorEdgetype = EDGETYPE.RR;
    private EDGETYPE newBubbleToMinorEdgeType = EDGETYPE.FR;

    private EdgeMap[] edges = new EdgeMap[4];

    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        GenomixJobConf.setGlobalStaticConstants(getContext().getConfiguration());
    }

    /**
     * add a bubble
     */
    @SuppressWarnings("unchecked")
    public void insertBubble(EdgeMap[] edges, VKmer insertedBubble, VKmer internalKmer) {
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

    public void addEdgeToInsertedBubble(EDGETYPE meToNewBubbleDir, VKmer insertedBubble) {
        EDGETYPE newBubbleToMeDir = meToNewBubbleDir.mirror();
        getVertexValue().getEdgeMap(newBubbleToMeDir).put(insertedBubble, new ReadIdSet(Arrays.asList(new Long(readId))));
    }

    public void setupEdgeForInsertedBubble() {
        for (EDGETYPE et : EnumSet.allOf(EDGETYPE.class)) {
            edges[et.get()] = new EdgeMap();
        }
        edges[newBubbleToMajorEdgetype.get()].put(majorVertexId, new ReadIdSet(Arrays.asList(new Long(readId))));
        edges[newBubbleToMinorEdgeType.get()].put(minorVertexId, new ReadIdSet(Arrays.asList(new Long(readId))));
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if (getVertexId().equals(majorVertexId)) {
                /** add edge pointing to insertedBubble **/
                addEdgeToInsertedBubble(newBubbleToMajorEdgetype, insertedBubble);
            } else if (getVertexId().equals(minorVertexId)) {
                /** add edge pointing to insertedBubble **/
                addEdgeToInsertedBubble(newBubbleToMinorEdgeType, insertedBubble);
            } else if (getVertexId().equals(middleVertexId)) {
                /** setup edges of insertedBubble **/
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
        job.setVertexInputFormatClass(NodeToVertexInputFormat.class);
        job.setVertexOutputFormatClass(VertexToNodeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(VKmer.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
