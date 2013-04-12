package edu.uci.ics.genomix.pregelix;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.BinaryLoadGraphInputFormat;
import edu.uci.ics.genomix.pregelix.format.BinaryLoadGraphOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.State;

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
public class MergeGraphVertex extends Vertex<BytesWritable, ValueStateWritable, NullWritable, MessageWritable>{
	
    private byte[] tmpVertexId;
    private byte[] tmpDestVertexId;
	private BytesWritable destVertexId = new BytesWritable();
	private BytesWritable tmpChainVertexId = new BytesWritable();
	private ValueStateWritable tmpVertexValue = new ValueStateWritable();
	private MessageWritable tmpMsg = new MessageWritable();
	/**
	 * Naive Algorithm for path merge graph
	 * @throws Exception 
	 * @throws  
	 */
	
	@Override
	public void compute(Iterator<MessageWritable> msgIterator) {
		
		tmpVertexId = GraphVertexOperation.generateValidDataFromBytesWritable(getVertexId());
		if (getSuperstep() == 1) {
			if(GraphVertexOperation.isHeadVertex(getVertexValue().getValue())){ 
				tmpMsg.setSourceVertexId(tmpVertexId);
				tmpMsg.setHead(tmpVertexId);
				tmpMsg.setLengthOfChain(0);
				tmpMsg.setChainVertexId(tmpChainVertexId.getBytes());
				for(byte x = Kmer.GENE_CODE.A; x<= Kmer.GENE_CODE.T ; x++){
					if((getVertexValue().getValue() & (1 << x)) != 0){
						tmpDestVertexId = KmerUtil.shiftKmerWithNextCode(GraphVertexOperation.k, tmpVertexId, x);
						destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
						sendMsg(destVertexId,tmpMsg);
					}
				}
			}
		}
		
		//path node sends message back to head node
		else if(getSuperstep()%2 == 0){
			
			 if(msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
					
				if(!tmpMsg.isRear()){
					if(getSuperstep() == 2)
						tmpMsg.setHead(tmpVertexId);
					if(GraphVertexOperation.isPathVertex(getVertexValue().getValue())){
						tmpDestVertexId = tmpMsg.getSourceVertexId();
						tmpMsg.setNeighberInfo(getVertexValue().getValue()); //set neighber
						if(tmpMsg.getLengthOfChain() == 0){
							tmpMsg.setLengthOfChain(GraphVertexOperation.k);
							tmpMsg.setChainVertexId(tmpVertexId);
						}
						else{
							String source = Kmer.recoverKmerFrom(GraphVertexOperation.k, tmpVertexId, 0, tmpVertexId.length);
							tmpMsg.setChainVertexId(KmerUtil.mergeKmerWithNextCode(
									tmpMsg.getLengthOfChain(),
									tmpMsg.getChainVertexId(),
									Kmer.GENE_CODE.getCodeFromSymbol((byte)source.charAt(source.length() - 1))));
							tmpMsg.incrementLength();
							deleteVertex(getVertexId());
						}
						destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
						sendMsg(destVertexId,tmpMsg);
					}
					else if(GraphVertexOperation.isRearVertex(getVertexValue().getValue())){
						if(getSuperstep() == 2)
							voteToHalt();
						else{
							tmpDestVertexId = tmpMsg.getSourceVertexId();
							tmpMsg.setSourceVertexId(tmpVertexId);
							tmpMsg.setRear(true);
							destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
							sendMsg(destVertexId,tmpMsg);
						}
					}
				}
				else{
					tmpVertexValue.setState(State.START_VERTEX);
					tmpVertexValue.setValue(GraphVertexOperation.updateRightNeighberByVertexId(getVertexValue().getValue(),
							tmpMsg.getSourceVertexId()));
					tmpVertexValue.setLengthOfMergeChain(tmpMsg.getLengthOfChain());
					tmpVertexValue.setMergeChain(tmpMsg.getChainVertexId());
					setVertexValue(tmpVertexValue);
					try {
						//String source = Kmer.recoverKmerFrom(tmpMsg.getLengthOfChain(), tmpMsg.getChainVertexId(), 0, tmpMsg.getChainVertexId().length);
						GraphVertexOperation.flushChainToFile(tmpMsg.getChainVertexId(), 
								tmpMsg.getLengthOfChain(),tmpVertexId);
					} catch (IOException e) { e.printStackTrace(); }
				}
			}
		}
		//head node sends message to path node
		else if(getSuperstep()%2 == 1){
			while (msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
				if(!tmpMsg.isRear()){
					byte[] lastKmer = KmerUtil.getLastKmerFromChain(GraphVertexOperation.k,
							tmpMsg.getLengthOfChain(),
							tmpMsg.getChainVertexId());
					tmpDestVertexId = KmerUtil.shiftKmerWithNextCode(GraphVertexOperation.k, lastKmer, 
							Kmer.GENE_CODE.getGeneCodeFromBitMap((byte)(tmpMsg.getNeighberInfo() & 0x0F)));

					tmpMsg.setSourceVertexId(tmpVertexId);
					destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
					sendMsg(destVertexId,tmpMsg);
				}
				else{	
					tmpDestVertexId = tmpMsg.getHead();
					destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
					sendMsg(destVertexId,tmpMsg);
				}
			}
		}
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
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
	}
}
