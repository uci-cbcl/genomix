package edu.uci.ics.genomix.pregelix.example.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.GraphVertexOperation;

public class LogAlgorithmMessageWritable implements WritableComparable<LogAlgorithmMessageWritable>{
	/**
	 * sourceVertexId stores source vertexId when headVertex sends the message
	 * 				  stores neighber vertexValue when pathVertex sends the message
	 * chainVertexId stores the chains of connected DNA
	 * file stores the point to the file that stores the chains of connected DNA
	 */
	private byte[] sourceVertexId;
	private byte neighberInfo;
	private int lengthOfChain;
	private byte[] chainVertexId;
	private File file;
	private int message;
	private int sourceVertexState;
	
	public LogAlgorithmMessageWritable(){
		sourceVertexId = new byte[(GraphVertexOperation.k-1)/4 + 1];
	}
	
	public void set(byte[] sourceVertexId,byte neighberInfo, byte[] chainVertexId, File file){
		this.sourceVertexId = sourceVertexId;
		this.chainVertexId = chainVertexId;
		this.file = file;
		this.message = 0;
		this.lengthOfChain = 0;
	}
	
	public void reset(){
		sourceVertexId = null;
		neighberInfo = (Byte) null;
		message = 0;
		sourceVertexState = 0;
	}

	public byte[] getSourceVertexId() {
		return sourceVertexId;
	}

	public void setSourceVertexId(byte[] sourceVertexId) {
		this.sourceVertexId = sourceVertexId;
	}

	public byte getNeighberInfo() {
		return neighberInfo;
	}

	public void setNeighberInfo(byte neighberInfo) {
		this.neighberInfo = neighberInfo;
	}

	public byte[] getChainVertexId() {
		return chainVertexId;
	}

	public void setChainVertexId(byte[] chainVertexId) {
		this.chainVertexId = chainVertexId;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
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
		return lengthOfChain;
	}

	public void setLengthOfChain(int lengthOfChain) {
		this.lengthOfChain = lengthOfChain;
	}

	public void incrementLength(){
		this.lengthOfChain++;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(lengthOfChain);
		if(lengthOfChain != 0)
			out.write(chainVertexId);
		out.write(sourceVertexId);
		out.write(neighberInfo);
		out.writeInt(message);
		out.writeInt(sourceVertexState);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		lengthOfChain = in.readInt();
		if(lengthOfChain != 0){
			chainVertexId = new byte[(lengthOfChain-1)/4 + 1];
			in.readFully(chainVertexId);
		}
		else
			chainVertexId = new byte[0];
		sourceVertexId = new byte[(GraphVertexOperation.k-1)/4 + 1];
		in.readFully(sourceVertexId);
		neighberInfo = in.readByte();
		message = in.readInt();
		sourceVertexState = in.readInt();
	}

    @Override
    public int hashCode() {
    	int hashCode = 0;
    	for(int i = 0; i < chainVertexId.length; i++)
    		hashCode = (int)chainVertexId[i];
        return hashCode;
    }
    @Override
    public boolean equals(Object o) {
        if (o instanceof LogAlgorithmMessageWritable) {
        	LogAlgorithmMessageWritable tp = (LogAlgorithmMessageWritable) o;
            return chainVertexId == tp.chainVertexId && file == tp.file;
        }
        return false;
    }
    @Override
    public String toString() {
        return chainVertexId.toString() + "\t" + file.getAbsolutePath();
    }
    @Override
	public int compareTo(LogAlgorithmMessageWritable tp) {
		// TODO Auto-generated method stub
        int cmp;
        if (chainVertexId == tp.chainVertexId)
            cmp = 0;
        else
            cmp = 1;
        if (cmp != 0)
            return cmp;
        if (file == tp.file)
            return 0;
        else
            return 1;
	}


}
