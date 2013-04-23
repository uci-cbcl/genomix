package edu.uci.ics.genomix.pregelix.operator;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.NaiveAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.pregelix.util.GraphVertexOperation;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: NaiveAlgorithmMessageWritable
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
/**
 * Naive Algorithm for path merge graph
 */
public class NaiveAlgorithmForPathMergeVertex extends Vertex<KmerBytesWritable, ValueStateWritable, NullWritable, NaiveAlgorithmMessageWritable>{
	
	public static final String KMER_SIZE = "NaiveAlgorithmForPathMergeVertex.kmerSize";
	public static final String ITERATIONS = "NaiveAlgorithmForPathMergeVertex.iteration";
	public static int kmerSize = -1;
	private int maxIteration = -1;
	
	private ValueStateWritable vertexVal = new ValueStateWritable();

	private NaiveAlgorithmMessageWritable msg = new NaiveAlgorithmMessageWritable();

	private VKmerBytesWritableFactory kmerFactory = new VKmerBytesWritableFactory(1);
	private VKmerBytesWritable vertexId = new VKmerBytesWritable(1); 
	private VKmerBytesWritable destVertexId = new VKmerBytesWritable(1); 
	private VKmerBytesWritable chainVertexId = new VKmerBytesWritable(1);
	private VKmerBytesWritable lastKmer = new VKmerBytesWritable(1);

	/**
	 * initiate kmerSize, maxIteration
	 */
	public void initVertex(){
		if(kmerSize == -1)
			kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0) 
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 100);
		vertexId.set(getVertexId());
		vertexVal = getVertexValue();
	}
	public void findDestination(){
		destVertexId.set(msg.getSourceVertexId());
	}
	/**
	 * get destination vertex
	 */
	public VKmerBytesWritable getDestVertexId(VKmerBytesWritable vertexId, byte geneCode){
		return kmerFactory.shiftKmerWithNextCode(vertexId, geneCode);
	}

	public VKmerBytesWritable getDestVertexIdFromChain(VKmerBytesWritable chainVertexId, byte adjMap){
		lastKmer.set(kmerFactory.getLastKmerFromChain(kmerSize, chainVertexId));
		return getDestVertexId(lastKmer, GeneCode.getGeneCodeFromBitMap((byte)(adjMap & 0x0F)));
	}
	/**
	 * head send message to all next nodes
	 */
	public void sendMsgToAllNextNodes(VKmerBytesWritable vertexId, byte adjMap){
		for(byte x = GeneCode.A; x<= GeneCode.T ; x++){
			if((adjMap & (1 << x)) != 0){
				destVertexId.set(getDestVertexId(vertexId, x));
				sendMsg(destVertexId, msg);
			}
		}
	}
	/**
	 * initiate chain vertex
	 */
	public void initChainVertex(){
		if(!msg.isRear()){
			findDestination();
			if(GraphVertexOperation.isPathVertex(vertexVal.getAdjMap())){
				chainVertexId.set(vertexId);
				msg.set(vertexId, chainVertexId, vertexId, vertexVal.getAdjMap(), false);
				sendMsg(destVertexId,msg);
			}else if(GraphVertexOperation.isRearVertex(vertexVal.getAdjMap()))
				voteToHalt();
		}
	}
	/**
	 * head node sends message to path node
	 */
	public void sendMsgToPathVertex(){
		if(!msg.isRear()){
			destVertexId.set(getDestVertexIdFromChain(msg.getChainVertexId(), msg.getAdjMap()));
		}else{
			destVertexId.set(msg.getHeadVertexId());
		}
		msg.set(vertexId, msg.getChainVertexId(), msg.getHeadVertexId(), (byte)0, msg.isRear());
		sendMsg(destVertexId,msg);
	}
	/**
	 * path node sends message back to head node
	 */
	public void responseMsgToHeadVertex(){
		if(!msg.isRear()){
			findDestination();
			if(GraphVertexOperation.isPathVertex(vertexVal.getAdjMap())){
				chainVertexId = kmerFactory.mergeKmerWithNextCode(msg.getChainVertexId(),
						vertexId.getGeneCodeAtPosition(kmerSize - 1));
				deleteVertex(getVertexId());
				//vertexVal.setState(State.NON_EXIST); 
				//setVertexValue(vertexVal);
				msg.set(vertexId, chainVertexId, msg.getHeadVertexId(), vertexVal.getAdjMap(), false);
				sendMsg(destVertexId,msg);
			}
			else if(GraphVertexOperation.isRearVertex(vertexVal.getAdjMap())){
				msg.set(vertexId, msg.getChainVertexId(), msg.getHeadVertexId(), (byte)0, true);
				sendMsg(destVertexId,msg);
			}
		}else{// is Rear
			chainVertexId.set(msg.getSourceVertexId());
			vertexVal.set(GraphVertexOperation.updateRightNeighberByVertexId(vertexVal.getAdjMap(), chainVertexId, kmerSize),
					State.START_VERTEX, msg.getChainVertexId());
			setVertexValue(vertexVal);
			//String source = msg.getChainVertexId().toString();
			//System.out.print("");
		}
	}
	
	@Override
	public void compute(Iterator<NaiveAlgorithmMessageWritable> msgIterator) {
		initVertex();
		if(vertexVal.getState() != State.NON_EXIST){
			if (getSuperstep() == 1) {
				if(GraphVertexOperation.isHeadVertex(vertexVal.getAdjMap())){ 
					msg.set(vertexId, chainVertexId, vertexId, (byte)0, false);
					sendMsgToAllNextNodes(vertexId, vertexVal.getAdjMap());
				}
			}
			else if(getSuperstep() == 2){
				if(msgIterator.hasNext()){
					msg = msgIterator.next();
					initChainVertex();
				}
			}
			//head node sends message to path node
			else if(getSuperstep()%2 == 1 && getSuperstep() <= maxIteration){
				while (msgIterator.hasNext()){
					msg = msgIterator.next();
					sendMsgToPathVertex();
				}
			}
			//path node sends message back to head node
			else if(getSuperstep()%2 == 0 && getSuperstep() > 2 && getSuperstep() <= maxIteration){
				 while(msgIterator.hasNext()){
					msg = msgIterator.next();
					responseMsgToHeadVertex();
				}
			}
		}
		voteToHalt();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(NaiveAlgorithmForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(NaiveAlgorithmForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class); 
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class); 
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
	}
}
