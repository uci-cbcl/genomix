package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import edu.uci.ics.genomix.type.KmerCountValue;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

public class DataLoadLogFormatter extends Formatter{
    private VKmerBytesWritable key;
    private KmerCountValue value;

    public void set(VKmerBytesWritable key, 
    		KmerCountValue value){
    	this.key.set(key);
    	this.value = value;
    }
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000); 
        
        builder.append(key.toString()
							+ "\t" + value.toString() + "\r\n");

        if(!formatMessage(record).equals(""))
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
