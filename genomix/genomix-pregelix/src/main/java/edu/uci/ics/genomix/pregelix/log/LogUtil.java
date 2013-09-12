package edu.uci.ics.genomix.pregelix.log;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.message.PathMergeMessageWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class LogUtil {
    
    public static String getVertexLog(byte loggingType, long step, VKmerBytesWritable vertexId, VertexValueWritable vertexValue){
        StringBuilder builder = new StringBuilder();
        builder.append("Step: " + step + "\r\n");
        builder.append(LoggingType.getContent(loggingType) + "\r\n");
        builder.append("VertexId: " + vertexId.toString() + "\r\n");
        builder.append("VertexValue: " + vertexValue.toString() + "\r\n");
        builder.append("\n");
        return builder.toString();
    }
    
    public static String getMessageLog(byte loggingType, long step, VKmerBytesWritable vertexId, PathMergeMessageWritable msg, VKmerBytesWritable destinationId){
        StringBuilder builder = new StringBuilder();
        builder.append("Step: " + step + "\r\n");
        builder.append("VertexId: " + vertexId.toString() + "\r\n");
        builder.append(LoggingType.getContent(loggingType));
        switch(loggingType){
            case LoggingType.RECEIVE_MSG:
                builder.append(" from " + msg.getSourceVertexId().toString() + "\r\n");
                builder.append("Message: " + msg.toString() + "\r\n");
                break;
            case LoggingType.SEND_MSG:
                builder.append(" to " + destinationId.toString() + "\r\n");
                builder.append("Message: " + msg.toString() + "\r\n");
                break;
        }
        builder.append("\n");
        return builder.toString();
    }
}
