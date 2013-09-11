package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class PathMergeLogFormatter extends Formatter{

    private long step;
    private VKmerBytesWritable vertexId;
    private VertexValueWritable vertexValue;
    
    public PathMergeLogFormatter(){
        step = -1;
        vertexId = new VKmerBytesWritable();
        vertexValue = new VertexValueWritable();
    }
    
    public void set(long step, VKmerBytesWritable vertexId, VertexValueWritable vertexValue){
        setStep(step);
        setVertexId(vertexId);
        setVertexValue(vertexValue);
    }
    
    @Override
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder();
        
        builder.append("Step: " + step + "\r\n");
        if (!formatMessage(record).equals(""))
            builder.append(formatMessage(record) + "\r\n");
        builder.append("VertexId: " + vertexId.toString() + "\r\n");
        builder.append("VertexValue: " + vertexValue.toString() + "\r\n");
        
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
    
}
