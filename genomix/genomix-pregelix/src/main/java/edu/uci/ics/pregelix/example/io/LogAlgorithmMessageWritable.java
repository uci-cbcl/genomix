package edu.uci.ics.pregelix.example.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LogAlgorithmMessageWritable implements WritableComparable<LogAlgorithmMessageWritable>{
	/**
	 * sourceVertexId stores source vertexId when headVertex sends the message
	 * 				  stores neighber vertexValue when pathVertex sends the message
	 * chainVertexId stores the chains of connected DNA
	 * file stores the point to the file that stores the chains of connected DNA
	 */
	private byte[] sourceVertexIdOrNeighberInfo;
	private int lengthOfChain;
	private byte[] chainVertexId;
	private File file;
	private int message;
	private int sourceVertexState;
	private static int k = 3;
	
	public LogAlgorithmMessageWritable(){		
	}
	
	public void set(byte[] sourceVertexIdOrNeighberInfo, byte[] chainVertexId, File file){
		this.sourceVertexIdOrNeighberInfo = sourceVertexIdOrNeighberInfo;
		this.chainVertexId = chainVertexId;
		this.file = file;
		this.message = 0;
		this.lengthOfChain = 0;
	}

	public byte[] getSourceVertexIdOrNeighberInfo() {
		return sourceVertexIdOrNeighberInfo;
	}

	public void setSourceVertexIdOrNeighberInfo(byte[] sourceVertexIdOrNeighberInfo) {
		this.sourceVertexIdOrNeighberInfo = sourceVertexIdOrNeighberInfo;
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
		out.write(sourceVertexIdOrNeighberInfo);
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
		if(lengthOfChain % 2 == 0)
			sourceVertexIdOrNeighberInfo = new byte[(k-1)/4 + 1];
		else
			sourceVertexIdOrNeighberInfo = new byte[1];
		in.readFully(sourceVertexIdOrNeighberInfo);
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
