package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.*;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.type.Kmer;

public class NaiveAlgorithmLogFormatter extends Formatter {
	//
    // Create a DateFormat to format the logger timestamp.
    //
    //private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
    private long step;
    private byte[] sourceVertexId;
    private byte[] destVertexId;
    private MessageWritable msg;
    private int k;

    public void set(long step, byte[] sourceVertexId, 
    		byte[] destVertexId, MessageWritable msg, int k){
    	this.step = step;
    	this.sourceVertexId = sourceVertexId;
    	this.destVertexId = destVertexId;
    	this.msg = msg;
    	this.k = k;
    }
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        String source = Kmer.recoverKmerFrom(k, sourceVertexId, 0, sourceVertexId.length);
        
        String chain = "";
        
        builder.append("Step: " + step + "\r\n");
        builder.append("Source Code: " + source + "\r\n");
        
        if(destVertexId != null){
        	builder.append("Send message to " + "\r\n");
        	String dest = Kmer.recoverKmerFrom(k, destVertexId, 0, destVertexId.length);
        	builder.append("Destination Code: " + dest + "\r\n");
        }
        if(msg.getLengthOfChain() != 0){
        	chain = Kmer.recoverKmerFrom(msg.getLengthOfChain(), msg.getChainVertexId(), 0, msg.getChainVertexId().length);
        	builder.append("Chain Message: " + chain + "\r\n");
        	builder.append("Chain Length: " + msg.getLengthOfChain() + "\r\n");
        }
        if(!formatMessage(record).equals(""))
        	builder.append(formatMessage(record) + "\r\n");
        builder.append("\n");
        return builder.toString();
    }

    public String getHead(Handler h) {
        return super.getHead(h);
    }

    public String getTail(Handler h) {
        return super.getTail(h);
    }
}