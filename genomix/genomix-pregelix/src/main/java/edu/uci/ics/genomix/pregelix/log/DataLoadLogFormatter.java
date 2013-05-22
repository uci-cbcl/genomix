package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import edu.uci.ics.genomix.type.NodeWritable;

public class DataLoadLogFormatter extends Formatter {
    private NodeWritable key;

    public void set(NodeWritable key) {
        this.key.set(key);
    }

    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);

        builder.append(key.toString() + "\r\n");

        if (!formatMessage(record).equals(""))
            builder.append(formatMessage(record) + "\r\n");
        return builder.toString();
    }

    public String getHead(Handler h) {
        return super.getHead(h);
    }

    public String getTail(Handler h) {
        return super.getTail(h);
    }
}
