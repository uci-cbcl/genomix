package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.*;

import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class NaiveAlgorithmLogFormatter extends Formatter {
    //
    // Create a DateFormat to format the logger timestamp.
    //
    //private static final DateFormat df = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss.SSS");
    private long step;
    private VKmerBytesWritable sourceVertexId;
    private VKmerBytesWritable destVertexId;

    public void set(long step, VKmerBytesWritable sourceVertexId, VKmerBytesWritable destVertexId) {
        this.step = step;
        this.sourceVertexId.set(sourceVertexId);
        this.destVertexId.set(destVertexId);
    }

    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        String source = sourceVertexId.toString();

        builder.append("Step: " + step + "\r\n");
        builder.append("Source Code: " + source + "\r\n");

        if (destVertexId != null) {
            builder.append("Send message to " + "\r\n");
            String dest = destVertexId.toString();
            builder.append("Destination Code: " + dest + "\r\n");
        }
        if (!formatMessage(record).equals(""))
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