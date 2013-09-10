package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class PathMergeLogFormatter extends Formatter{

    private long step;
    private VKmerBytesWritable vertexId;
    private NodeWritable vertexValue;
    
    @Override
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder();
        
        builder.append("Step: " + step + "\r\n");
        builder.append("VertexId: " + vertexId.toString() + "\r\n");
        builder.append("VertexValue: " + vertexValue.toString() + "\r\n");
        return null;
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
        this.vertexId = vertexId;
    }

    public NodeWritable getVertexValue() {
        return vertexValue;
    }

    public void setVertexValue(NodeWritable vertexValue) {
        this.vertexValue = vertexValue;
    }
    
}
