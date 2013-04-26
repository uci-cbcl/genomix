package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.operator.ThreeStepLogAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.pregelix.type.CheckMessage;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class LogAlgorithmMessageWritable implements WritableComparable<LogAlgorithmMessageWritable>{
	/**
	 * sourceVertexId stores source vertexId when headVertex sends the message
	 * 				  stores neighber vertexValue when pathVertex sends the message
	 * chainVertexId stores the chains of connected DNA
	 * file stores the point to the file that stores the chains of connected DNA
	 */
	private KmerBytesWritable sourceVertexId;
	private VKmerBytesWritable chainVertexId;
	private byte adjMap;
	private byte message;
	
	private byte checkMessage;
	
	public LogAlgorithmMessageWritable(){
		sourceVertexId = new VKmerBytesWritable(ThreeStepLogAlgorithmForPathMergeVertex.kmerSize);
		chainVertexId = new VKmerBytesWritable(ThreeStepLogAlgorithmForPathMergeVertex.kmerSize);
		checkMessage = 0;
	}
	
	public void set(KmerBytesWritable sourceVertexId, VKmerBytesWritable chainVertexId, byte adjMap, byte message){
		checkMessage = 0;
		if(sourceVertexId != null){
			checkMessage |= CheckMessage.SOURCE;
			this.sourceVertexId.set(sourceVertexId);
		}
		if(chainVertexId != null){
			checkMessage |= CheckMessage.CHAIN;
			this.chainVertexId.set(chainVertexId);
		}
		if(adjMap != 0){
			checkMessage |= CheckMessage.ADJMAP;
			this.adjMap = adjMap;
		}
		this.message = message;
	}
	
	public void reset(){
		checkMessage = 0;
		chainVertexId.reset(ThreeStepLogAlgorithmForPathMergeVertex.kmerSize);
		adjMap = (byte)0;
		message = 0;
		//sourceVertexState = 0;
	}

	public KmerBytesWritable getSourceVertexId() {
		return sourceVertexId;
	}

	public void setSourceVertexId(KmerBytesWritable sourceVertexId) {
		if(sourceVertexId != null){
			checkMessage |= CheckMessage.SOURCE;
			this.sourceVertexId.set(sourceVertexId);
		}
	}

	public byte getAdjMap() {
		return adjMap;
	}

	public void setAdjMap(byte adjMap) {
		this.adjMap = adjMap;
	}

	public VKmerBytesWritable getChainVertexId() {
		return chainVertexId;
	}

	public void setChainVertexId(VKmerBytesWritable chainVertexId) {
		this.chainVertexId.set(chainVertexId);
	}

	public byte getMessage() {
		return message;
	}

	public void setMessage(byte message) {
		this.message = message;
	}

	public int getLengthOfChain() {
		return chainVertexId.getKmerLength();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(checkMessage);
		if((checkMessage & CheckMessage.SOURCE) != 0)
			sourceVertexId.write(out);
		if((checkMessage & CheckMessage.CHAIN) != 0)
			chainVertexId.write(out);
		if((checkMessage & CheckMessage.ADJMAP) != 0)
			out.write(adjMap);
		out.writeByte(message);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		checkMessage = in.readByte();
		if((checkMessage & CheckMessage.SOURCE) != 0)
			sourceVertexId.readFields(in);
		if((checkMessage & CheckMessage.CHAIN) != 0)
			chainVertexId.readFields(in);
		if((checkMessage & CheckMessage.ADJMAP) != 0)
			adjMap = in.readByte();
		message = in.readByte();
	}

	 @Override
    public int hashCode() {
        return chainVertexId.hashCode();
    }
	 
    @Override
    public boolean equals(Object o) {
        if (o instanceof NaiveAlgorithmMessageWritable) {
        	LogAlgorithmMessageWritable tp = (LogAlgorithmMessageWritable) o;
            return chainVertexId.equals(tp.chainVertexId);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return chainVertexId.toString();
    }
    
	@Override
	public int compareTo(LogAlgorithmMessageWritable tp) {
		return chainVertexId.compareTo(tp.chainVertexId);
	}
}
