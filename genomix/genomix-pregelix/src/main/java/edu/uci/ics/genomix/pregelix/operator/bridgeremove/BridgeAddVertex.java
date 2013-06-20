package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.DataCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.DataCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;

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
        Vertex<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "BridgeRemoveVertex.kmerSize";
    public static final String LENGTH = "BridgeRemoveVertex.length";
    public static int kmerSize = -1;
    private int length = -1;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if(length == -1)
            length = getContext().getConfiguration().getInt(LENGTH, kmerSize + 5);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            if(getVertexId().getReadID() == 1 && getVertexId().getPosInRead() == 2){
                getVertexValue().getFFList().append(3, (byte)1);
                
                //add bridge vertex
                @SuppressWarnings("rawtypes")
                Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
                vertex.getMsgList().clear();
                vertex.getEdges().clear();
                PositionWritable vertexId = new PositionWritable();
                ValueStateWritable vertexValue = new ValueStateWritable();
                /**
                 * set the src vertex id
                 */
                vertexId.set(3, (byte)1);
                vertex.setVertexId(vertexId);
                /**
                 * set the vertex value
                 */
                byte[] array = { 'T', 'A', 'G', 'C', 'C', 'T'};
                KmerBytesWritable kmer = new KmerBytesWritable(array.length);
                kmer.setByRead(array, 0);
                vertexValue.setMergeChain(kmer);
                PositionListWritable plist = new PositionListWritable();
                plist.append(new PositionWritable(1, (byte)2));
                vertexValue.setRRList(plist);
                PositionListWritable plist2 = new PositionListWritable();
                plist2.append(new PositionWritable(2, (byte)2));
                vertexValue.setFFList(plist2);
                vertex.setVertexValue(vertexValue);
                
                addVertex(vertexId, vertex);
            }
            if(getVertexId().getReadID() == 2 && getVertexId().getPosInRead() == 2)
                getVertexValue().getRRList().append(3, (byte)1);
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BridgeAddVertex.class.getSimpleName());
        job.setVertexClass(BridgeAddVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(DataCleanInputFormat.class);
        job.setVertexOutputFormatClass(DataCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
    }
}
