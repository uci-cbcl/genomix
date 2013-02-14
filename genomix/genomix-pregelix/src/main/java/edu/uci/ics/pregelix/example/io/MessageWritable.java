package edu.uci.ics.pregelix.example.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MessageWritable implements WritableComparable<MessageWritable>{
	/**
	 * bytes stores the chains of connected DNA
	 * file stores the point to the file that stores the chains of connected DNA
	 */
	private byte[] bytes;
	private File file;
	
	public MessageWritable(){		
	}
	
	public MessageWritable(byte[] bytes, File file){
		set(bytes,file);
	}
	
	public void set(byte[] bytes, File file){
		this.bytes = bytes;
		this.file = file;
	}
			
	public byte[] getBytes() {
	    return bytes;
	}
	
	public File getFile(){
		return file;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.write(bytes);
		out.writeUTF(file.getAbsolutePath()); 
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		in.readFully(bytes);
		String absolutePath = in.readUTF();
		file = new File(absolutePath);
	}

    @Override
    public int hashCode() {
    	int hashCode = 0;
    	for(int i = 0; i < bytes.length; i++)
    		hashCode = (int)bytes[i];
        return hashCode;
    }
    @Override
    public boolean equals(Object o) {
        if (o instanceof MessageWritable) {
        	MessageWritable tp = (MessageWritable) o;
            return bytes == tp.bytes && file == tp.file;
        }
        return false;
    }
    @Override
    public String toString() {
        return bytes.toString() + "\t" + file.getAbsolutePath();
    }
    
	@Override
	public int compareTo(MessageWritable tp) {
		// TODO Auto-generated method stub
        int cmp;
        if (bytes == tp.bytes)
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
