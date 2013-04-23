package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.*;

import edu.uci.ics.genomix.pregelix.io.NaiveAlgorithmMessageWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class NaiveAlgorithmLogFormatter extends Formatter {
	//
    // Create a DateFormat to format the logger timestamp.
    //
    //private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
    private long step;
    private VKmerBytesWritable sourceVertexId;
    private VKmerBytesWritable destVertexId;
    private NaiveAlgorithmMessageWritable msg;

    public void set(long step, VKmerBytesWritable sourceVertexId, 
    		VKmerBytesWritable destVertexId, NaiveAlgorithmMessageWritable msg){
    	this.step = step;
    	this.sourceVertexId.set(sourceVertexId);
    	this.destVertexId.set(destVertexId);
    	this.msg = msg;
    }
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        String source = sourceVertexId.toString();
        
        String chain = "";
        
        builder.append("Step: " + step + "\r\n");
        builder.append("Source Code: " + source + "\r\n");
        
        if(destVertexId != null){
        	builder.append("Send message to " + "\r\n");
        	String dest = destVertexId.toString();
        	builder.append("Destination Code: " + dest + "\r\n");
        }
        if(msg.getLengthOfChain() != 0){
        	chain = msg.getChainVertexId().toString();
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