package edu.uci.ics.genomix.pregelix;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.bitwise.BitwiseOperation;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.genomix.pregelix.example.client.Client;
import edu.uci.ics.genomix.pregelix.example.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.hdfs.HDFSOperation;

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
public class MergeGraphVertex extends Vertex<BytesWritable, ByteWritable, NullWritable, MessageWritable>{
	
    /** The number of bytes for vertex id */
    public static final int numBytes = (GraphVertexOperation.k-1)/4 + 1;
    private BytesWritable tmpVertextId = new BytesWritable();
	private BytesWritable tmpDestVertexId = new BytesWritable();
	private BytesWritable tmpChainVertexId = new BytesWritable();
	private ByteWritable tmpVertexValue = new ByteWritable();
	private MessageWritable tmpMsg = new MessageWritable();
	OutputStreamWriter writer; 
	/**
	 * Naive Algorithm for path merge graph
	 */
	@Override
	public void compute(Iterator<MessageWritable> msgIterator) {
		try {
			writer = new OutputStreamWriter(new FileOutputStream("test/check_Naive",true));
		} catch (FileNotFoundException e1) { e1.printStackTrace();}
		tmpVertextId.set(GraphVertexOperation.generateValidDataFromBytesWritable(getVertexId()),0,numBytes);
		if (getSuperstep() == 1) {
			if(GraphVertexOperation.isHead(getVertexValue())){ 
				tmpDestVertexId.set(tmpVertextId);
				//change
				Kmer.moveKmer(GraphVertexOperation.k, tmpDestVertexId.getBytes(), 
						(byte)GraphVertexOperation.findSucceedNode(getVertexValue().get()));
				//tmpDestVertexId.set(GraphVertexOperation.getDestVertexId(tmpDestVertexId.getBytes(),
				//		getVertexValue().get()), 0, numBytes); 
				tmpMsg.setSourceVertexId(tmpVertextId.getBytes());
				tmpMsg.setLengthOfChain(0);
				tmpMsg.setChainVertexId(tmpChainVertexId.getBytes());
				sendMsg(tmpDestVertexId,tmpMsg);
				//test
				GraphVertexOperation.testMessageCommunication(writer,getSuperstep(),tmpVertextId.getBytes(),
						tmpDestVertexId.getBytes(),tmpMsg);
				}
		}
		//path node sends message back to head node
		else if(getSuperstep()%2 == 0){
			 if(msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
				if(!tmpMsg.isRear()){
					if(GraphVertexOperation.isPathVertex(getVertexValue())){
						tmpDestVertexId.set(tmpMsg.getSourceVertexId(), 0, numBytes);
						tmpMsg.setNeighberInfo(getVertexValue().get()); //set neighber
						if(tmpMsg.getLengthOfChain() == 0){
							tmpMsg.setLengthOfChain(GraphVertexOperation.k);
							tmpMsg.setChainVertexId(tmpVertextId.getBytes());
						}
						else{
							/*
							tmpMsg.incrementLength();
							tmpMsg.setChainVertexId(GraphVertexOperation.updateChainVertexId(
									tmpChainVertexId,
									tmpMsg.getLengthOfChain()-1,
									tmpVertextId));
							 */
							 //change
							tmpMsg.setChainVertexId(KmerUtil.mergeKmerWithNextCode(
									tmpMsg.getLengthOfChain(),
									tmpMsg.getChainVertexId(),
									(byte)GraphVertexOperation.findSucceedNode(getVertexValue().get())));
							tmpMsg.incrementLength();
							deleteVertex(getVertexId());
						}
						sendMsg(tmpDestVertexId,tmpMsg);
						//test
						GraphVertexOperation.testMessageCommunication(writer,getSuperstep(),tmpVertextId.getBytes(),
								tmpDestVertexId.getBytes(),tmpMsg);
					}
					else if(GraphVertexOperation.isRear(getVertexValue())){
						tmpDestVertexId.set(tmpMsg.getSourceVertexId(), 0, numBytes);
						tmpMsg.setSourceVertexId(tmpVertextId.getBytes());
						tmpMsg.setRear(true);
						sendMsg(tmpDestVertexId,tmpMsg);
						//test
						try {
							writer.write("It is Rear!\r\n");
						} catch (IOException e) { e.printStackTrace(); }
						GraphVertexOperation.testMessageCommunication(writer,getSuperstep(),tmpVertextId.getBytes(),
								tmpDestVertexId.getBytes(),tmpMsg);
					}
				}
				else{
					tmpVertexValue.set(GraphVertexOperation.updateRightNeighberByVertexId(getVertexValue().get(),
							tmpMsg.getSourceVertexId()));
					try {
						GraphVertexOperation.flushChainToFile(tmpMsg.getChainVertexId(), 
								tmpMsg.getLengthOfChain(),tmpVertextId.getBytes());
					} catch (IOException e) { e.printStackTrace(); }
					//test
		        	GraphVertexOperation.testLastMessageCommunication(writer,getSuperstep(),tmpVertextId.getBytes(),
		        			tmpDestVertexId.getBytes(),tmpMsg);
				}
			}
		}
		//head node sends message to path node
		else if(getSuperstep()%2 == 1){
			if (msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
				if(!tmpMsg.isRear()){
					byte[] lastKmer = KmerUtil.getLastKmerFromChain(GraphVertexOperation.k,
							tmpMsg.getLengthOfChain(),
							tmpMsg.getChainVertexId());
					//byte[] lastKmer = GraphVertexOperation.getLastKmer(tmpMsg.getChainVertexId(), 
					//		tmpMsg.getLengthOfChain());
					tmpDestVertexId.set(lastKmer, 0, numBytes);
					//change
					Kmer.moveKmer(GraphVertexOperation.k, tmpDestVertexId.getBytes(), 
							(byte)GraphVertexOperation.findSucceedNode(getVertexValue().get()));
					//tmpDestVertexId.set(GraphVertexOperation.getDestVertexId(lastKmer,
					//		tmpMsg.getNeighberInfo()), 0, numBytes);
					tmpMsg.setSourceVertexId(tmpVertextId.getBytes());
					sendMsg(tmpDestVertexId,tmpMsg);
					//test
					GraphVertexOperation.testMessageCommunication(writer,getSuperstep(),tmpVertextId.getBytes(),
							tmpDestVertexId.getBytes(),tmpMsg);
				}
				else{	
					tmpDestVertexId.set(tmpVertextId);
					//change
					Kmer.moveKmer(GraphVertexOperation.k, tmpDestVertexId.getBytes(), 
							(byte)GraphVertexOperation.findSucceedNode(getVertexValue().get()));
					//tmpDestVertexId.set(GraphVertexOperation.getDestVertexId(tmpVertextId.getBytes(),
					//		getVertexValue().get()), 0, numBytes);
					sendMsg(tmpDestVertexId,tmpMsg);
					//test
					GraphVertexOperation.testMessageCommunication(writer,getSuperstep(),tmpVertextId.getBytes(),
							tmpDestVertexId.getBytes(),tmpMsg);
				}
			}
		}
		try {
			writer.close();
		} catch (IOException e) { e.printStackTrace(); }
		voteToHalt();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(MergeGraphVertex.class.getSimpleName());
        job.setVertexClass(MergeGraphVertex.class);
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
