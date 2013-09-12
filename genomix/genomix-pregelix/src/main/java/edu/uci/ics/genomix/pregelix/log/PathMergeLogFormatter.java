package edu.uci.ics.genomix.pregelix.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class PathMergeLogFormatter extends Formatter {

    @Override
    public String format(LogRecord record) {
        if (record.getLevel() == Level.FINE) {
            StringBuilder sb = new StringBuilder();
            sb.append(formatMessage(record)).append(System.getProperty("line.separator"));
            if (null != record.getThrown()) {
                sb.append("Throwable occurred: ");
                Throwable t = record.getThrown();
                PrintWriter pw = null;
                try {
                    StringWriter sw = new StringWriter();
                    pw = new PrintWriter(sw);
                    t.printStackTrace(pw);
                    sb.append(sw.toString());
                } finally {
                    if (pw != null) {
                        try {
                            pw.close();
                        } catch (Exception e) {
                            // ignore
                        }
                    }
                }
            }
            return sb.toString();
        } else {
            return "";
        }
    }

}
