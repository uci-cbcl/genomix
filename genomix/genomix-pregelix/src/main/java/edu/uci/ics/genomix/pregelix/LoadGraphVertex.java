package edu.uci.ics.genomix.pregelix;

import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.BinaryLoadGraphInputFormat;
import edu.uci.ics.genomix.pregelix.format.BinaryLoadGraphOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;

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
 * For example, ONE LINE in input file: 00,01,10	0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
public class LoadGraphVertex extends Vertex<BytesWritable, ByteWritable, NullWritable, MessageWritable>{

	/**
	 * For test, just output original file
	 */
	@Override
	public void compute(Iterator<MessageWritable> msgIterator) {
		voteToHalt();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		//final int k = Integer.parseInt(args[0]);
        PregelixJob job = new PregelixJob(LoadGraphVertex.class.getSimpleName());
        job.setVertexClass(LoadGraphVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(BinaryLoadGraphInputFormat.class); 
        job.setVertexOutputFormatClass(BinaryLoadGraphOutputFormat.class); 
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
	}
}
