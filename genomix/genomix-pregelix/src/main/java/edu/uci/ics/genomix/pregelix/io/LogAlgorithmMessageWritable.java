package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.operator.LogAlgorithmForPathMergeVertex;
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
	private int message;
	private int sourceVertexState;
	
	public LogAlgorithmMessageWritable(){
		sourceVertexId = new VKmerBytesWritable(LogAlgorithmForPathMergeVertex.kmerSize);
		chainVertexId = new VKmerBytesWritable(LogAlgorithmForPathMergeVertex.kmerSize);
	}
	
	public void set(KmerBytesWritable sourceVertexId, VKmerBytesWritable chainVertexId, byte adjMap, int message, int sourceVertexState){
		this.sourceVertexId.set(sourceVertexId);
		this.chainVertexId.set(chainVertexId);
		this.adjMap = adjMap;
		this.message = message;
		this.sourceVertexState = sourceVertexState;
	}
	
	public void reset(){
		//sourceVertexId.reset(LogAlgorithmForPathMergeVertex.kmerSize);
		chainVertexId.reset(LogAlgorithmForPathMergeVertex.kmerSize);
		adjMap = (byte)0;
		message = 0;
		sourceVertexState = 0;
	}

	public KmerBytesWritable getSourceVertexId() {
		return sourceVertexId;
	}

	public void setSourceVertexId(KmerBytesWritable sourceVertexId) {
		this.sourceVertexId.set(sourceVertexId);
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

	public int getMessage() {
		return message;
	}

	public void setMessage(int message) {
		this.message = message;
	}

	public int getSourceVertexState() {
		return sourceVertexState;
	}

	public void setSourceVertexState(int sourceVertexState) {
		this.sourceVertexState = sourceVertexState;
	}

	public int getLengthOfChain() {
		return chainVertexId.getKmerLength();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		sourceVertexId.write(out);
		chainVertexId.write(out);
		out.write(adjMap);
		out.writeInt(message);
		out.writeInt(sourceVertexState);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sourceVertexId.readFields(in);
		chainVertexId.readFields(in);
		adjMap = in.readByte();
		message = in.readInt();
		sourceVertexState = in.readInt();
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
