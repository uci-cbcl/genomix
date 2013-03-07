package edu.uci.ics.pregelix;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.pregelix.dataflow.util.IterationUtils;
import edu.uci.ics.pregelix.example.client.Client;
import edu.uci.ics.pregelix.example.io.LogAlgorithmMessageWritable;
import edu.uci.ics.pregelix.example.io.ValueStateWritable;
import edu.uci.ics.pregelix.type.Message;
import edu.uci.ics.pregelix.type.State;

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
public class LogAlgorithmForMergeGraphVertex extends Vertex<BytesWritable, ValueStateWritable, NullWritable, LogAlgorithmMessageWritable>{
	
	private byte[] tmpSourceVertextId;
	private byte[] tmpDestVertexId;
	private byte[] tmpChainVertexId;
	private byte tmpVertexValue;
	private int tmpVertexState;
	private int tmpMessage;
	private ValueStateWritable tmpVal = new ValueStateWritable();
	private LogAlgorithmMessageWritable tmpMsg = new LogAlgorithmMessageWritable();
	public static final int k = 3; //kmer, k = 3
	/**
	 * For test, in compute method, make each vertexValue shift 1 to left.
	 * It will be modified when going forward to next step.
	 */
	@Override
	public void compute(Iterator<LogAlgorithmMessageWritable> msgIterator) {
		if (getSuperstep() == 1) {
			tmpVal = getVertexValue();
			tmpVertexValue = tmpVal.getValue();
			if(GraphVertexOperation.isHead(new ByteWritable(tmpVertexValue))){
				tmpMsg.setMessage(Message.START);
				tmpDestVertexId = GraphVertexOperation.getDestVertexId(getVertexId().getBytes(), tmpVertexValue);
				sendMsg(new BytesWritable(tmpDestVertexId),tmpMsg);
				voteToHalt();
			}
			else if(GraphVertexOperation.isRear(new ByteWritable(tmpVertexValue))){
				tmpMsg.setMessage(Message.END);
				tmpDestVertexId = GraphVertexOperation.getDestVertexId(getVertexId().getBytes(), tmpVertexValue);
				sendMsg(new BytesWritable(tmpDestVertexId),tmpMsg);
				voteToHalt();
			}
			else if(GraphVertexOperation.isPathVertex(new ByteWritable(tmpVertexValue))){
				tmpVal.setState(State.MID_VERTEX);
				setVertexValue(tmpVal);
			}
			else
				voteToHalt();
		}
		else if(getSuperstep() == 2){
			if(msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
				tmpMessage = tmpMsg.getMessage();
				tmpVertexState = getVertexValue().getState();
				if(tmpMessage == Message.START && tmpVertexState == State.MID_VERTEX){
					tmpVal.setState(State.START_VERTEX);
					setVertexValue(tmpVal);
				}
				else if(tmpMessage == Message.END && tmpVertexState == State.MID_VERTEX){
					tmpVal.setState(State.END_VERTEX);
					setVertexValue(tmpVal);
				}
			}
		}
		//head node sends message to path node
		else if(getSuperstep()%3 == 0){
			tmpVertexState = getVertexValue().getState();
			tmpSourceVertextId = getVertexId().getBytes();
			tmpDestVertexId = GraphVertexOperation.getDestVertexId(tmpSourceVertextId, 
					getVertexValue().getValue());
			if(tmpVertexState == State.START_VERTEX){
				tmpMsg.setMessage(Message.START);
				tmpMsg.setSourceVertexIdOrNeighberInfo(tmpSourceVertextId);
				sendMsg(new BytesWritable(tmpDestVertexId),tmpMsg);
			}
			else if(tmpVertexState == State.END_VERTEX){
				tmpMsg.setMessage(Message.NON);
				tmpMsg.setSourceVertexIdOrNeighberInfo(tmpSourceVertextId);
				sendMsg(new BytesWritable(tmpDestVertexId),tmpMsg);
			}
		}
		//path node sends message back to head node
		else if(getSuperstep()%3 == 1){
			if(msgIterator.hasNext()){
				tmpVal = getVertexValue();
				tmpMsg = msgIterator.next();
				tmpMessage = tmpMsg.getMessage();
				tmpSourceVertextId = getVertexId().getBytes();
				tmpMsg.setChainVertexId(tmpSourceVertextId);
				byte[] tmpBytes = GraphVertexOperation.getDestVertexId(tmpSourceVertextId, 
						tmpVal.getValue());
				tmpMsg.setSourceVertexIdOrNeighberInfo(tmpBytes); //set neighber
				tmpMsg.setSourceVertexState(tmpVal.getState());
				sendMsg(new BytesWritable(tmpSourceVertextId),tmpMsg);
				//kill Message because it has been merged by the head
				if(tmpMessage == Message.START){
					tmpVal.setState(State.TODELETE);
					setVertexValue(tmpVal);
				}
			}
			else
				deleteVertex(new BytesWritable(tmpSourceVertextId)); //killSelf because it doesn't receive any message
		}
		else if(getSuperstep()%3 == 2){
			if(msgIterator.hasNext()){
				tmpMsg = msgIterator.next();
				tmpVertexState = getVertexValue().getState();
				if(tmpVertexState == State.TODELETE)
					deleteVertex(new BytesWritable(tmpSourceVertextId)); //killSelf
				setVertexId(GraphVertexOperation.mergeTwoChainVertex(getVertexId().getBytes(), 
						tmpMsg.getChainVertexId(), tmpMsg.getLengthOfChain()));
				byte[] tmpByte = tmpMsg.getSourceVertexIdOrNeighberInfo();
				tmpVertexValue = GraphVertexOperation.updateRightNeighber(getVertexValue().getValue(),tmpByte[0]);
				tmpVal = getVertexValue();
				tmpVal.setValue(tmpVertexValue);
				setVertexValue(tmpVal);
				if(tmpVertexState == State.END_VERTEX)
					voteToHalt();
			}
		}
	}
	
   private void signalTerminate() {
        Configuration conf = getContext().getConfiguration();
        try {
            IterationUtils.writeForceTerminationState(conf, BspUtils.getJobId(conf));
            writeMergeGraphResult(conf, true);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
   
   private void writeMergeGraphResult(Configuration conf, boolean terminate) {
       try {
           FileSystem dfs = FileSystem.get(conf);
           String pathStr = IterationUtils.TMP_DIR + BspUtils.getJobId(conf) + "MergeGraph";
           Path path = new Path(pathStr);
           if (!dfs.exists(path)) {
               FSDataOutputStream output = dfs.create(path, true);
               output.writeBoolean(terminate);
               output.flush();
               output.close();
           }
       } catch (IOException e) {
           throw new IllegalStateException(e);
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
        job.setVertexInputFormatClass(BinaryLoadGraphInputFormat.class); 
        job.setVertexOutputFormatClass(BinaryLoadGraphOutputFormat.class); 
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(ByteWritable.class);
        Client.run(args, job);
	}
}
