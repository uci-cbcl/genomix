package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
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
 *  Remove tip or single node when l > constant
 */
public class TipRemoveVertex extends
        Vertex<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "TipRemoveVertex.kmerSize";
    public static final String LENGTH = "TipRemoveVertex.length";
    public static int kmerSize = -1;
    private int length = -1;

    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if(length == -1)
            length = getContext().getConfiguration().getInt(LENGTH, kmerSize + 5);
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); //getVertexValue().getLengthOfMergeChain() < length
        if(getSuperstep() == 1){
            
        }
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(TipRemoveVertex.class.getSimpleName());
        job.setVertexClass(TipRemoveVertex.class);
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
