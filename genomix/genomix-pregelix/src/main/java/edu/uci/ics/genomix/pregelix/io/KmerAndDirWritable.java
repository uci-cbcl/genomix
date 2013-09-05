package edu.uci.ics.genomix.pregelix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class KmerAndDirWritable implements Writable{

    public VKmerBytesWritable getDeleteKmer() {
		return deleteKmer;
	}

	public void setDeleteKmer(VKmerBytesWritable deleteKmer) {
		this.deleteKmer = deleteKmer;
	}

	public byte getDeleteDir() {
		return deleteDir;
	}

	public void setDeleteDir(byte deleteDir) {
		this.deleteDir = deleteDir;
	}

	private VKmerBytesWritable deleteKmer;
    private byte deleteDir;
    
    public KmerAndDirWritable(){
    	deleteKmer = new VKmerBytesWritable();
    	deleteDir = 0;
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		deleteKmer.readFields(in);
		deleteDir = in.readByte();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		deleteKmer.write(out);
		out.writeByte(deleteDir);
	}
    
    
}
