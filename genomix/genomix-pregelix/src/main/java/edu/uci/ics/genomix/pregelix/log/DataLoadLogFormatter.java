package edu.uci.ics.genomix.pregelix.log;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.hadoop.io.BytesWritable;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerCountValue;

public class DataLoadLogFormatter extends Formatter{
    private BytesWritable key;
    private KmerCountValue value;
    private int k;

    public void set(BytesWritable key, 
    		KmerCountValue value, int k){
    	this.key = key;
    	this.value = value;
    	this.k = k;
    }
    public String format(LogRecord record) {
        StringBuilder builder = new StringBuilder(1000);
        
        builder.append(Kmer.recoverKmerFrom(k, key.getBytes(), 0,
							key.getLength())
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
