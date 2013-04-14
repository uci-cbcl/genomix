package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.*;

import edu.uci.ics.genomix.pregelix.io.LogAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.Kmer;

public class LogAlgorithmLogFormatter extends Formatter {
	//
    // Create a DateFormat to format the logger timestamp.
    //
    //private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
    private long step;
    private byte[] sourceVertexId;
    private byte[] destVertexId;
    private LogAlgorithmMessageWritable msg;
    private int state;
    private int k;
    private byte[] mergeChain;
    private int lengthOfMergeChain;
    //private boolean testDelete = false;
    /** 0: general operation 
     *  1: testDelete 
     *  2: testMergeChain
     *  3: testVoteToHalt
     */ 
    private int operation; 

    public void set(long step, byte[] sourceVertexId, 
    		byte[] destVertexId, LogAlgorithmMessageWritable msg, int state, int k){
    	this.step = step;
    	this.sourceVertexId = sourceVertexId;
    	this.destVertexId = destVertexId;
    	this.msg = msg;
    	this.state = state;
    	this.k = k;
    	this.operation = 0;
    }
    public void setMergeChain(long step, byte[] sourceVertexId, 
    		int lengthOfMergeChain, byte[] mergeChain, int k){
    	this.reset();
    	this.step = step;
    	this.sourceVertexId = sourceVertexId;
    	this.lengthOfMergeChain = lengthOfMergeChain;
    	this.mergeChain = mergeChain;
    	this.k = k;
    	this.operation = 2;
    }
    public void setVotoToHalt(long step, byte[] sourceVertexId, int k){
    	this.reset();
    	this.step = step;
    	this.sourceVertexId = sourceVertexId;
    	this.k = k;
    	this.operation = 3;
    }
    public void reset(){
    	this.sourceVertexId = null;
    	this.destVertexId = null;
    	this.msg = null;
    	this.state = 0;
    	this.k = 0;
    	this.mergeChain = null;
    	this.lengthOfMergeChain = 0;
    }
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        String source = Kmer.recoverKmerFrom(k, sourceVertexId, 0, sourceVertexId.length);
        String chain = "";
        
        builder.append("Step: " + step + "\r\n");
        builder.append("Source Code: " + source + "\r\n");
        if(operation == 0){
	        if(destVertexId != null){
	        	String dest = Kmer.recoverKmerFrom(k, destVertexId, 0, destVertexId.length);
		        builder.append("Send message to " + "\r\n");
		        builder.append("Destination Code: " + dest + "\r\n");
	        }
	        builder.append("Message is: " + Message.MESSAGE_CONTENT.getContentFromCode(msg.getMessage()) + "\r\n");
	        	
	        if(msg.getLengthOfChain() != 0){
	        	chain = Kmer.recoverKmerFrom(msg.getLengthOfChain(), msg.getChainVertexId(), 0, msg.getChainVertexId().length);
	        	builder.append("Chain Message: " + chain + "\r\n");
	        	builder.append("Chain Length: " + msg.getLengthOfChain() + "\r\n");
	        }
	        
	        builder.append("State is: " + State.STATE_CONTENT.getContentFromCode(state) + "\r\n");
        }
        if(operation == 2){
        	chain = Kmer.recoverKmerFrom(lengthOfMergeChain, mergeChain, 0, mergeChain.length);
        	builder.append("Merge Chain: " + chain + "\r\n");
        	builder.append("Merge Chain Length: " + lengthOfMergeChain + "\r\n");
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