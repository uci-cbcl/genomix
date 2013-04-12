package edu.uci.ics.genomix.pregelix;

import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForMergeGraphInputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForMergeGraphOutputFormat;
import edu.uci.ics.genomix.pregelix.io.LogAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;

/*
 * vertexId: BytesWritable
 * vertexValue: ValueStateWritable
 * edgeValue: NullWritable
 * message: LogAlgorithmMessageWritable
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
public class LogAlgorithmForMergeGraphVertex extends Vertex<BytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable>{

	private byte[] tmpVertexId;
	private byte[] tmpDestVertexId;
	private BytesWritable destVertexId = new BytesWritable();
	private byte[] mergeChainVertexId;
	private int lengthOfMergeChainVertex;
	private byte tmpVertexValue;
	private ValueStateWritable tmpVal = new ValueStateWritable();
	private LogAlgorithmMessageWritable tmpMsg = new LogAlgorithmMessageWritable();
	/**
	 * Log Algorithm for path merge graph
	 */
	
	@Override
	public void compute(Iterator<LogAlgorithmMessageWritable> msgIterator) {

		tmpVertexId = GraphVertexOperation.generateValidDataFromBytesWritable(getVertexId());
		tmpVal = getVertexValue();
		if (getSuperstep() == 1) {
			tmpMsg.setChainVertexId(new byte[0]);
			if(GraphVertexOperation.isHeadVertex(tmpVal.getValue())){
				tmpMsg.setMessage(Message.START);
				for(byte x = Kmer.GENE_CODE.A; x<= Kmer.GENE_CODE.T ; x++){
					if((tmpVal.getValue() & (1 << x)) != 0){
						tmpDestVertexId = KmerUtil.shiftKmerWithNextCode(GraphVertexOperation.k, tmpVertexId, x);
						destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
						sendMsg(destVertexId,tmpMsg);
					}
				}
				voteToHalt();
			}
			if(GraphVertexOperation.isRearVertex(tmpVal.getValue())){
				tmpMsg.setMessage(Message.END);
				
				for(byte x = Kmer.GENE_CODE.A; x<= Kmer.GENE_CODE.T ; x++){
					if(((tmpVal.getValue()>> 4) & (1 << x)) != 0){
						tmpDestVertexId = KmerUtil.shiftKmerWithPreCode(GraphVertexOperation.k, tmpVertexId, x);
						destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
						sendMsg(destVertexId,tmpMsg);
					}
				}
				voteToHalt();
			}
			if(GraphVertexOperation.isPathVertex(tmpVal.getValue())){
				tmpVal.setState(State.MID_VERTEX);
				setVertexValue(tmpVal);
			}
		}
		else if(getSuperstep() == 2){
			while(msgIterator.hasNext()){
				if(!GraphVertexOperation.isPathVertex(tmpVal.getValue())){
					msgIterator.next();
					voteToHalt();
				}
				else{
					tmpMsg = msgIterator.next();
					if(tmpMsg.getMessage() == Message.START && tmpVal.getState() == State.MID_VERTEX){
						tmpVal.setState(State.START_VERTEX);
						setVertexValue(tmpVal);
					}
					else if(tmpMsg.getMessage() == Message.END && tmpVal.getState() == State.MID_VERTEX){
						tmpVal.setState(State.END_VERTEX);
						setVertexValue(tmpVal);
						voteToHalt();
					}
					else
						voteToHalt();
				}
			}
		}
		//head node sends message to path node
		else if(getSuperstep()%3 == 0){
			if(getSuperstep() == 3){
				tmpMsg = new LogAlgorithmMessageWritable();
				tmpDestVertexId = KmerUtil.shiftKmerWithNextCode(GraphVertexOperation.k, tmpVertexId, 
						Kmer.GENE_CODE.getGeneCodeFromBitMap((byte)(tmpVal.getValue() & 0x0F)));
				destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
				if(tmpVal.getState() == State.START_VERTEX){
					tmpMsg.setMessage(Message.START);
					tmpMsg.setSourceVertexId(getVertexId().getBytes());
					sendMsg(destVertexId, tmpMsg);
					voteToHalt();
				}
				else if(tmpVal.getState() != State.END_VERTEX && tmpVal.getState() != State.FINAL_DELETE){
					tmpMsg.setMessage(Message.NON);
					tmpMsg.setSourceVertexId(getVertexId().getBytes());
					sendMsg(destVertexId,tmpMsg);
					voteToHalt();
				}
			}
			else{
				if(msgIterator.hasNext()){
					tmpMsg = msgIterator.next();
					byte[] lastKmer = KmerUtil.getLastKmerFromChain(GraphVertexOperation.k,
							tmpVal.getLengthOfMergeChain(),
							tmpVal.getMergeChain());
					tmpDestVertexId = KmerUtil.shiftKmerWithNextCode(GraphVertexOperation.k, lastKmer, 
							Kmer.GENE_CODE.getGeneCodeFromBitMap((byte)(tmpVal.getValue() & 0x0F))); //tmpMsg.getNeighberInfo()
					destVertexId.set(tmpDestVertexId, 0, tmpDestVertexId.length);
					if(tmpVal.getState() == State.START_VERTEX){
						tmpMsg.setMessage(Message.START);
						tmpMsg.setSourceVertexId(getVertexId().getBytes());
						sendMsg(destVertexId, tmpMsg);
						voteToHalt();
					}
					else if(tmpVal.getState() != State.END_VERTEX && tmpVal.getState() != State.FINAL_DELETE){
						tmpMsg.setMessage(Message.NON);
						tmpMsg.setSourceVertexId(getVertexId().getBytes());
						sendMsg(destVertexId,tmpMsg);
					}
				}
			}
		}
		
		//path node sends message back to head node
		else if(getSuperstep()%3 == 1){
			if(msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
				int message = tmpMsg.getMessage();
				if(tmpVal.getLengthOfMergeChain() == 0){
					tmpVal.setLengthOfMergeChain(GraphVertexOperation.k);
					tmpVal.setMergeChain(tmpVertexId);
					setVertexValue(tmpVal);
				}
				
				tmpMsg.setLengthOfChain(tmpVal.getLengthOfMergeChain());
				tmpMsg.setChainVertexId(tmpVal.getMergeChain());
				
				tmpMsg.setNeighberInfo(tmpVal.getValue()); //set neighber
				tmpMsg.setSourceVertexState(tmpVal.getState());
				
				//kill Message because it has been merged by the head
				if(tmpVal.getState() == State.END_VERTEX || tmpVal.getState() == State.FINAL_DELETE){
					tmpMsg.setMessage(Message.END);
					tmpVal.setState(State.FINAL_DELETE);
					setVertexValue(tmpVal);
					//deleteVertex(getVertexId());
				}
				else
					tmpMsg.setMessage(Message.NON);
				
				if(message == Message.START){
					tmpVal.setState(State.TODELETE);
					setVertexValue(tmpVal);
				}
				destVertexId.set(tmpMsg.getSourceVertexId(), 0, tmpMsg.getSourceVertexId().length);
				sendMsg(destVertexId,tmpMsg);
			}
			else{
				if(getVertexValue().getState() != State.START_VERTEX
						&& getVertexValue().getState() != State.END_VERTEX && getVertexValue().getState() != State.FINAL_DELETE)
					deleteVertex(getVertexId()); //killSelf because it doesn't receive any message
			}
		}
		else if(getSuperstep()%3 == 2){
			if(tmpVal.getState() == State.TODELETE)
				deleteVertex(getVertexId()); //killSelf
			else{
				if(msgIterator.hasNext()){
					tmpMsg = msgIterator.next();

					if(tmpMsg.getMessage() == Message.END){
						if(tmpVal.getState() != State.START_VERTEX)
							tmpVal.setState(State.END_VERTEX);
						else
							tmpVal.setState(State.FINAL_VERTEX);
					}
						
					if(getSuperstep() == 5){
						lengthOfMergeChainVertex = GraphVertexOperation.k;
						mergeChainVertexId = tmpVertexId;
					}
					else{
						lengthOfMergeChainVertex = tmpVal.getLengthOfMergeChain(); 
						mergeChainVertexId = tmpVal.getMergeChain(); 
					}
					mergeChainVertexId = KmerUtil.mergeTwoKmer(lengthOfMergeChainVertex, 
							mergeChainVertexId,
							tmpMsg.getLengthOfChain() - GraphVertexOperation.k + 1, 
							KmerUtil.getLastKmerFromChain(tmpMsg.getLengthOfChain() - GraphVertexOperation.k + 1,
									tmpMsg.getLengthOfChain(), tmpMsg.getChainVertexId()));
					lengthOfMergeChainVertex = lengthOfMergeChainVertex + tmpMsg.getLengthOfChain()
							- GraphVertexOperation.k + 1;
					tmpVal.setLengthOfMergeChain(lengthOfMergeChainVertex);
					tmpVal.setMergeChain(mergeChainVertexId);

					tmpVertexValue = GraphVertexOperation.updateRightNeighber(getVertexValue().getValue(),tmpMsg.getNeighberInfo());
					tmpVal.setValue(tmpVertexValue);
					if(tmpMsg.getMessage() != Message.END){
						setVertexValue(tmpVal);
						tmpMsg = new LogAlgorithmMessageWritable(); //reset
						tmpMsg.setNeighberInfo(tmpVertexValue);
						sendMsg(getVertexId(),tmpMsg);
					}
				}
				if(tmpVal.getState() == State.END_VERTEX || tmpVal.getState() == State.FINAL_DELETE)
					voteToHalt();
				if(tmpVal.getState() == State.FINAL_VERTEX){
					//String source = Kmer.recoverKmerFrom(tmpVal.getLengthOfMergeChain(), tmpVal.getMergeChain(), 0, tmpVal.getMergeChain().length);
					voteToHalt();
				}
			}
			
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(MergeGraphVertex.class.getSimpleName());
        job.setVertexClass(MergeGraphVertex.class);
        /**
         * BinaryInput and BinaryOutput~/
         */
        job.setVertexInputFormatClass(LogAlgorithmForMergeGraphInputFormat.class); 
        job.setVertexOutputFormatClass(LogAlgorithmForMergeGraphOutputFormat.class); 
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
	}
}
