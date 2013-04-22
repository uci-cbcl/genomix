package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.operator.NaiveAlgorithmForPathMergeVertex;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class NaiveAlgorithmMessageWritable implements WritableComparable<NaiveAlgorithmMessageWritable>{
	/**
	 * sourceVertexId stores source vertexId when headVertex sends the message
	 * 				  stores neighber vertexValue when pathVertex sends the message
	 * chainVertexId stores the chains of connected DNA
	 * file stores the point to the file that stores the chains of connected DNA
	 */
	private KmerBytesWritable sourceVertexId;
	private VKmerBytesWritable chainVertexId;
	private KmerBytesWritable headVertexId;
	private byte adjMap;
	private boolean isRear;
	
	public NaiveAlgorithmMessageWritable(){
		sourceVertexId = new VKmerBytesWritable(NaiveAlgorithmForPathMergeVertex.kmerSize);
		chainVertexId = new VKmerBytesWritable(NaiveAlgorithmForPathMergeVertex.kmerSize);
		headVertexId = new VKmerBytesWritable(NaiveAlgorithmForPathMergeVertex.kmerSize);
	}
	
	public void set(KmerBytesWritable sourceVertex, VKmerBytesWritable chainVertex, KmerBytesWritable headVertex , byte adjMap, boolean isRear){
		this.sourceVertexId.set(sourceVertex);
		this.chainVertexId.set(chainVertex);
		this.headVertexId.set(headVertex);
		this.adjMap = adjMap;
		this.isRear = isRear;
	}

	public KmerBytesWritable getSourceVertexId() {
		return sourceVertexId;
	}

	public void setSourceVertexId(KmerBytesWritable source) {
		this.sourceVertexId.set(source);
	}

	public byte getAdjMap() {
		return adjMap;
	}

	public void setAdjMap(byte adjMap) {
		this.adjMap = adjMap;
	}

	public void setChainVertexId(VKmerBytesWritable chainVertex) {
		this.chainVertexId.set(chainVertex);
	}

	public VKmerBytesWritable getChainVertexId() {
		return chainVertexId;
	}

	public boolean isRear() {
		return isRear;
	}

	public void setRear(boolean isRear) {
		this.isRear = isRear;
	}

	public int getLengthOfChain() {
		return this.chainVertexId.getKmerLength();
	}
	
	
	public KmerBytesWritable getHeadVertexId() {
		return headVertexId;
	}

	public void setHeadVertexId(KmerBytesWritable headVertexId) {
		this.headVertexId.set(headVertexId);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sourceVertexId.write(out);
		headVertexId.write(out);
		chainVertexId.write(out);
		out.write(adjMap);
		out.writeBoolean(isRear);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sourceVertexId.readFields(in);
		headVertexId.readFields(in);
		chainVertexId.readFields(in);
		adjMap = in.readByte();
		isRear = in.readBoolean();
	}

    @Override
    public int hashCode() {
        return chainVertexId.hashCode();
    }
    @Override
    public boolean equals(Object o) {
        if (o instanceof NaiveAlgorithmMessageWritable) {
        	NaiveAlgorithmMessageWritable tp = (NaiveAlgorithmMessageWritable) o;
            return chainVertexId.equals( tp.chainVertexId);
        }
        return false;
    }
    @Override
    public String toString() {
        return chainVertexId.toString();
    }
    
	@Override
	public int compareTo(NaiveAlgorithmMessageWritable tp) {
		return chainVertexId.compareTo(tp.chainVertexId);
	}

}
