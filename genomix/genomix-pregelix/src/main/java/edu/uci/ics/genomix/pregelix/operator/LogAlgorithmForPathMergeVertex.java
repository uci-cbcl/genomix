package edu.uci.ics.genomix.pregelix.operator;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.LogAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.pregelix.util.GraphVertexOperation;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;
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
public class LogAlgorithmForPathMergeVertex extends Vertex<KmerBytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable>{	
	public static final String KMER_SIZE = "LogAlgorithmForPathMergeVertex.kmerSize";
	public static final String ITERATIONS = "LogAlgorithmForPathMergeVertex.iteration";
	public static int kmerSize = -1;
	private int maxIteration = -1;
	
	private LogAlgorithmMessageWritable incomingMsg = new LogAlgorithmMessageWritable();
	private LogAlgorithmMessageWritable outgoingMsg = new LogAlgorithmMessageWritable();
	
	private VKmerBytesWritableFactory kmerFactory = new VKmerBytesWritableFactory(1);
	private VKmerBytesWritable chainVertexId = new VKmerBytesWritable(1);
	private VKmerBytesWritable lastKmer = new VKmerBytesWritable(1);
	/**
	 * initiate kmerSize, maxIteration
	 */
	public void initVertex(){
		if(kmerSize == -1)
			kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0) 
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        outgoingMsg.reset();
	}
	/**
	 * get destination vertex
	 */
	public VKmerBytesWritable getNextDestVertexId(KmerBytesWritable vertexId, byte geneCode){
		return kmerFactory.shiftKmerWithNextCode(vertexId, geneCode);
	}
	
	public VKmerBytesWritable getPreDestVertexId(KmerBytesWritable vertexId, byte geneCode){
		return kmerFactory.shiftKmerWithPreCode(vertexId, geneCode);
	}
	
	public VKmerBytesWritable getNextDestVertexIdFromBitmap(KmerBytesWritable chainVertexId, byte adjMap){
		return getDestVertexIdFromChain(chainVertexId, adjMap);
	}
	
	public VKmerBytesWritable getDestVertexIdFromChain(KmerBytesWritable chainVertexId, byte adjMap){
		VKmerBytesWritable lastKmer = kmerFactory.getLastKmerFromChain(kmerSize, chainVertexId);
		return getNextDestVertexId(lastKmer, GeneCode.getGeneCodeFromBitMap((byte)(adjMap & 0x0F)));
	}
	/**
	 * head send message to all next nodes
	 */
	public void sendMsgToAllNextNodes(KmerBytesWritable vertexId, byte adjMap){
		for(byte x = GeneCode.A; x<= GeneCode.T ; x++){
			if((adjMap & (1 << x)) != 0){
				sendMsg(getNextDestVertexId(vertexId, x), outgoingMsg);
			}
		}
	}
	/**
	 * head send message to all previous nodes
	 */
	public void sendMsgToAllPreviousNodes(KmerBytesWritable vertexId, byte adjMap){
		for(byte x = GeneCode.A; x<= GeneCode.T ; x++){
			if(((adjMap >> 4) & (1 << x)) != 0){
				sendMsg(getPreDestVertexId(vertexId, x), outgoingMsg);
			}
		}
	}
	/**
	 * start sending message 
	 */
	public void startSendMsg(){
		if(GraphVertexOperation.isHeadVertex(getVertexValue().getAdjMap())){
			outgoingMsg.setMessage(Message.START);
			sendMsgToAllNextNodes(getVertexId(), getVertexValue().getAdjMap());
			voteToHalt();
		}
		if(GraphVertexOperation.isRearVertex(getVertexValue().getAdjMap())){
			outgoingMsg.setMessage(Message.END);
			sendMsgToAllPreviousNodes(getVertexId(), getVertexValue().getAdjMap());
			voteToHalt();
		}
	}
	/**
	 *  initiate head, rear and path node
	 */
	public void initState(Iterator<LogAlgorithmMessageWritable> msgIterator){
		while(msgIterator.hasNext()){
			if(!GraphVertexOperation.isPathVertex(getVertexValue().getAdjMap())){
				msgIterator.next();
				voteToHalt();
			}
			else{
				incomingMsg = msgIterator.next();
				setState();
			}
		}
	}
	/**
	 * set vertex state
	 */
	public void setState(){
		if(incomingMsg.getMessage() == Message.START){
			getVertexValue().setState(State.START_VERTEX);
			getVertexValue().setMergeChain(null);
		}
		else if(incomingMsg.getMessage() == Message.END && getVertexValue().getState() != State.START_VERTEX){
			getVertexValue().setState(State.END_VERTEX);
			getVertexValue().setMergeChain(getVertexId());
			voteToHalt();
		}
		else
			voteToHalt();
	}
	/**
	 * head send message to path
	 */
	public void sendOutMsg(KmerBytesWritable chainVertexId, byte adjMap){
		if(getVertexValue().getState() == State.START_VERTEX){
			outgoingMsg.setMessage(Message.START);
			outgoingMsg.setSourceVertexId(getVertexId());
			sendMsg(getNextDestVertexIdFromBitmap(chainVertexId, adjMap), outgoingMsg);
		}
		else if(getVertexValue().getState() != State.END_VERTEX){
			outgoingMsg.setMessage(Message.NON);
			outgoingMsg.setSourceVertexId(getVertexId());
			sendMsg(getNextDestVertexIdFromBitmap(chainVertexId, adjMap), outgoingMsg);
		}
	}
	/**
	 * head send message to path
	 */
	public void sendMsgToPathVertex(Iterator<LogAlgorithmMessageWritable> msgIterator){
		if(getSuperstep() == 3){
			getVertexValue().setMergeChain(getVertexId());
			sendOutMsg(getVertexId(), getVertexValue().getAdjMap());
		}
		else{
			if(msgIterator.hasNext()){
				incomingMsg = msgIterator.next();
				if(mergeChainVertex(msgIterator)){
					if(incomingMsg.getMessage() == Message.END){
						if(getVertexValue().getState() == State.START_VERTEX){
							getVertexValue().setState(State.FINAL_VERTEX);
							//String source = getVertexValue().getMergeChain().toString();
							//System.out.println();
						}
						else
							getVertexValue().setState(State.END_VERTEX);
					}
					else
						sendOutMsg(getVertexValue().getMergeChain(), getVertexValue().getAdjMap());
				}
			}
		}
	}
	/**
	 * path response message to head
	 */
	public void responseMsgToHeadVertex(Iterator<LogAlgorithmMessageWritable> msgIterator){
		if(msgIterator.hasNext()){		
			incomingMsg = msgIterator.next();
			outgoingMsg.setChainVertexId(getVertexValue().getMergeChain());
			outgoingMsg.setAdjMap(getVertexValue().getAdjMap());
			if(getVertexValue().getState() == State.END_VERTEX)
				outgoingMsg.setMessage(Message.END);
			sendMsg(incomingMsg.getSourceVertexId(),outgoingMsg);
			
			if(incomingMsg.getMessage() == Message.START)
				deleteVertex(getVertexId());
		}
		else{
			if(getVertexValue().getState() != State.START_VERTEX 
					&& getVertexValue().getState() != State.END_VERTEX)
				deleteVertex(getVertexId());//killSelf because it doesn't receive any message
		}
	}
	/**
	 * merge chainVertex and store in vertexVal.chainVertexId
	 */
	public boolean mergeChainVertex(Iterator<LogAlgorithmMessageWritable> msgIterator){
		//merge chain
		lastKmer.set(kmerFactory.getLastKmerFromChain(incomingMsg.getLengthOfChain() - kmerSize + 1, 
				incomingMsg.getChainVertexId()));
		chainVertexId.set(kmerFactory.mergeTwoKmer(getVertexValue().getMergeChain(), 
				lastKmer));
		if(GraphVertexOperation.isCycle(getVertexId(), chainVertexId)){
			getVertexValue().setMergeChain(null);
			getVertexValue().setAdjMap(GraphVertexOperation.reverseAdjMap(getVertexValue().getAdjMap(),
					chainVertexId.getGeneCodeAtPosition(kmerSize)));
			getVertexValue().setState(State.CYCLE);
			return false;
		}
		else
			getVertexValue().setMergeChain(chainVertexId);
		
		byte tmpVertexValue = GraphVertexOperation.updateRightNeighber(getVertexValue().getAdjMap(),
				incomingMsg.getAdjMap());
		getVertexValue().setAdjMap(tmpVertexValue);
		return true;
	}
	@Override
	public void compute(Iterator<LogAlgorithmMessageWritable> msgIterator) {
		initVertex();
		if (getSuperstep() == 1) 
			startSendMsg();
		else if(getSuperstep() == 2)
			initState(msgIterator);
		else if(getSuperstep()%2 == 1 && getSuperstep() <= maxIteration){
			sendMsgToPathVertex(msgIterator);
			voteToHalt();
		}
		else if(getSuperstep()%2 == 0 && getSuperstep() <= maxIteration){
			responseMsgToHeadVertex(msgIterator);
			voteToHalt();
		}
		else
			voteToHalt();
	}
	public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(LogAlgorithmForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(LogAlgorithmForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput~/
         */
        job.setVertexInputFormatClass(LogAlgorithmForPathMergeInputFormat.class); 
        job.setVertexOutputFormatClass(LogAlgorithmForPathMergeOutputFormat.class); 
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        job.setDynamicVertexValueSize(true);
        Client.run(args, job);
	}
}
