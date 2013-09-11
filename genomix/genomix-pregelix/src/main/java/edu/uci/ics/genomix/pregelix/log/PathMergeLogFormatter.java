package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class PathMergeLogFormatter extends Formatter{

    private long step;
    private VKmerBytesWritable vertexId;
    private VertexValueWritable vertexValue;
    
    private PathMergeMessageWritable msg;
    
    private byte loggingType; 
    
    public PathMergeLogFormatter(){
        step = -1;
        vertexId = new VKmerBytesWritable();
        vertexValue = new VertexValueWritable();
        msg = new PathMergeMessageWritable();
        loggingType = -1;
    }
    
    public void setVertexLog(byte loggingType, long step, VKmerBytesWritable vertexId, VertexValueWritable vertexValue){
        setLoggingType(loggingType);
        setStep(step);
        setVertexId(vertexId);
        setVertexValue(vertexValue);
    }
    
    public void setMessageLog(byte loggingType, long step, VKmerBytesWritable vertexId, PathMergeMessageWritable msg){
        setLoggingType(loggingType);
        setStep(step);
        setVertexId(vertexId);
        setMsg(msg);
    }
    
    @Override
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder();
        builder.append("Step: " + step + "\r\n");
        if (!formatMessage(record).equals(""))
            builder.append(formatMessage(record) + "\r\n");
        switch(loggingType){
            case LoggingType.ORIGIN:
            case LoggingType.AFTER_UPDATE:
                builder.append("VertexId: " + vertexId.toString() + "\r\n");
                builder.append("VertexValue: " + vertexValue.toString() + "\r\n");
                break;
            case LoggingType.RECEIVE_MSG:
                builder.append("VertexId: " + vertexId.toString() + "\r\n");
                builder.append("Message: " + msg.toString() + "\r\n");
                break;
        }
       
        builder.append("\n");
        return builder.toString();
    }

    public long getStep() {
        return step;
    }

    public void setStep(long step) {
        this.step = step;
    }

    public VKmerBytesWritable getVertexId() {
        return vertexId;
    }

    public void setVertexId(VKmerBytesWritable vertexId) {
        this.vertexId.setAsCopy(vertexId);
    }

    public VertexValueWritable getVertexValue() {
        return vertexValue;
    }

    public void setVertexValue(VertexValueWritable vertexValue) {
        this.vertexValue.setAsCopy(vertexValue);
    }

    public PathMergeMessageWritable getMsg() {
        return msg;
    }

    public void setMsg(PathMergeMessageWritable msg) {
        this.msg.setAsCopy(msg);
    }

    public byte getLoggingType() {
        return loggingType;
    }

    public void setLoggingType(byte loggingType) {
        this.loggingType = loggingType;
    }
    
}
