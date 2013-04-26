package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.*;

import edu.uci.ics.genomix.pregelix.io.LogAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class LogAlgorithmLogFormatter extends Formatter {
	//
    // Create a DateFormat to format the logger timestamp.
    //
    //private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
    private long step;
    private VKmerBytesWritable sourceVertexId = new VKmerBytesWritable(1);
    private VKmerBytesWritable destVertexId = new VKmerBytesWritable(1);
    private LogAlgorithmMessageWritable msg = new LogAlgorithmMessageWritable();
    private byte state;
    private VKmerBytesWritable mergeChain = new VKmerBytesWritable(1);;
    //private boolean testDelete = false;
    /** 0: general operation 
     *  1: testDelete 
     *  2: testMergeChain
     *  3: testVoteToHalt
     */ 
    private int operation; 
    
    public LogAlgorithmLogFormatter(){
    }

    public void set(long step, VKmerBytesWritable sourceVertexId, 
    		VKmerBytesWritable destVertexId, LogAlgorithmMessageWritable msg, byte state){
    	this.step = step;
    	this.sourceVertexId.set(sourceVertexId);
    	this.destVertexId.set(destVertexId);
    	this.msg = msg;
    	this.state = state;
    	this.operation = 0;
    }
    public void setMergeChain(long step, VKmerBytesWritable sourceVertexId, 
    		VKmerBytesWritable mergeChain){
    	this.reset();
    	this.step = step;
    	this.sourceVertexId.set(sourceVertexId);
    	this.mergeChain.set(mergeChain);
    	this.operation = 2;
    }
    public void setVotoToHalt(long step, VKmerBytesWritable sourceVertexId){
    	this.reset();
    	this.step = step;
    	this.sourceVertexId.set(sourceVertexId);
    	this.operation = 3;
    }
    public void reset(){
    	this.sourceVertexId = new VKmerBytesWritable(1);
    	this.destVertexId = new VKmerBytesWritable(1);
    	this.msg = new LogAlgorithmMessageWritable();
    	this.state = 0;
    	this.mergeChain = new VKmerBytesWritable(1);
    }
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        String source = sourceVertexId.toString();
        String chain = "";
        
        builder.append("Step: " + step + "\r\n");
        builder.append("Source Code: " + source + "\r\n");
        if(operation == 0){
	        if(destVertexId.getKmerLength() != -1){
	        	String dest = destVertexId.toString();
		        builder.append("Send message to " + "\r\n");
		        builder.append("Destination Code: " + dest + "\r\n");
	        }
	        builder.append("Message is: " + Message.MESSAGE_CONTENT.getContentFromCode(msg.getMessage()) + "\r\n");
	        	
	        if(msg.getLengthOfChain() != -1){
	        	chain = msg.getChainVertexId().toString();
	        	builder.append("Chain Message: " + chain + "\r\n");
	        	builder.append("Chain Length: " + msg.getLengthOfChain() + "\r\n");
	        }
	        
	        builder.append("State is: " + State.STATE_CONTENT.getContentFromCode(state) + "\r\n");
        }
        if(operation == 2){
        	chain = mergeChain.toString();
        	builder.append("Merge Chain: " + chain + "\r\n");
        	builder.append("Merge Chain Length: " + mergeChain.getKmerLength() + "\r\n");
        }
        if(operation == 3)
        	builder.append("Vote to halt!");
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
	public int getOperation() {
		return operation;
	}
	public void setOperation(int operation) {
		this.operation = operation;
	}
}