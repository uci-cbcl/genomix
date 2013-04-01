package edu.uci.ics.pregelix;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.bitwise.BitwiseOperation;
import edu.uci.ics.pregelix.example.GraphMutationVertex;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.io.MessageWritable;

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
public class TestLoadGraphVertex extends Vertex<BytesWritable, ByteWritable, NullWritable, MessageWritable>{

	//private byte[] tmpVertexId;
	//private BytesWritable vid;
	//private TestLoadGraphVertex newVertex;
	//private MessageWritable tmpMsg = new MessageWritable();
	/**
	 * For test, just output original file
	 */
	@Override
	public void compute(Iterator<MessageWritable> msgIterator) {
		/*deleteVertex(getVertexId());
		tmpVertexId = getVertexId().getBytes();
		String a1 = "100100";
		byte[] b1 = BitwiseOperation.convertBinaryStringToBytes(a1);
		String a2 = "000000"; //"001001";
		byte[] b2 = BitwiseOperation.convertBinaryStringToBytes(a2);
		String valueString = "00000000";
		byte value = BitwiseOperation.convertBinaryStringToByte(valueString);
		if(getSuperstep() == 1 && Arrays.equals(b1,tmpVertexId)){
			newVertex = new TestLoadGraphVertex();
            vid.set(new BytesWritable(b2));
            newVertex.setVertexId(vid);
            newVertex.setVertexValue(getVertexValue());
            addVertex(vid, newVertex);
			//vertex.initialize(new BytesWritable(b2), new ByteWritable(value), null, null);
			//addVertex(new BytesWritable(b2),this.createdNewLiveVertex());
			deleteVertex(getVertexId());
		}*/
		/*String a2 = "100111";
		byte[] b2 = BitwiseOperation.convertBinaryStringToBytes(a2);
		String a3 = "11111111";
		byte[] b3 = BitwiseOperation.convertBinaryStringToBytes(a3);
		byte[] bb1 = getVertexId().getBytes();
		if(getSuperstep() == 1 && Arrays.equals(b1,bb1)){
			//addVertex(new BytesWritable(b3),new ByteWritable(bb1[0]));
			deleteVertex(new BytesWritable(b1));
		}
		else if(getSuperstep() == 2){
			if(msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
				byte[] b = tmpMsg.getChainVertexId();
				setVertexValue(new ByteWritable(b[0]));
			}		
		}*/
		voteToHalt();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		//final int k = Integer.parseInt(args[0]);
        PregelixJob job = new PregelixJob(MergeGraphVertex.class.getSimpleName());
        job.setVertexClass(TestLoadGraphVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(BinaryLoadGraphInputFormat.class); 
        job.setVertexOutputFormatClass(BinaryLoadGraphOutputFormat.class); 
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ByteWritable.class);
        Client.run(args, job);
	}
}
