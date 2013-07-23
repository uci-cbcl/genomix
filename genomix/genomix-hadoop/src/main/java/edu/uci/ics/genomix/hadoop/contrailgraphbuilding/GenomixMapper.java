package edu.uci.ics.genomix.hadoop.contrailgraphbuilding;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;
import edu.uci.ics.genomix.type.KmerListWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class GenomixMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, KmerBytesWritable, NodeWritable>{
    
    public static enum KmerDir{
        FORWARD,
        REVERSE,
    }
    
    public static int KMER_SIZE;
    private KmerBytesWritable preForwardKmer;
    private KmerBytesWritable preReverseKmer;
    private KmerBytesWritable curForwardKmer;
    private KmerBytesWritable curReverseKmer;
    private KmerBytesWritable nextForwardKmer;
    private KmerBytesWritable nextReverseKmer;
    private NodeWritable outputNode;
    private PositionWritable nodeId;
    private KmerListWritable kmerList;
    
    private KmerBytesWritableFactory kmerFactory;
    private KmerDir preKmerDir;
    private KmerDir curKmerDir;
    private KmerDir nextKmerDir;
    
    byte mateId = (byte)0;
    
    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
        preForwardKmer = new KmerBytesWritable(KMER_SIZE);
        preReverseKmer = new KmerBytesWritable(KMER_SIZE);
        curForwardKmer = new KmerBytesWritable(KMER_SIZE);
        curReverseKmer = new KmerBytesWritable(KMER_SIZE);
        nextForwardKmer = new KmerBytesWritable(KMER_SIZE);
        nextReverseKmer = new KmerBytesWritable(KMER_SIZE);
        outputNode = new NodeWritable();
        nodeId = new PositionWritable();
        kmerList = new KmerListWritable();
        kmerFactory = new KmerBytesWritableFactory(KMER_SIZE);
        preKmerDir = KmerDir.FORWARD;
        curKmerDir = KmerDir.FORWARD;
        nextKmerDir = KmerDir.FORWARD;
    }
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<KmerBytesWritable, NodeWritable> output,
            Reporter reporter) throws IOException {
        /** first kmer */
        String[] rawLine = value.toString().split("\\t"); // Read the Real Gene Line
        if (rawLine.length != 2) {
            throw new IOException("invalid data");
        }
        int readID = 0;
        readID = Integer.parseInt(rawLine[0]);
        String geneLine = rawLine[1];
        Pattern genePattern = Pattern.compile("[AGCT]+");
        Matcher geneMatcher = genePattern.matcher(geneLine);
        boolean isValid = geneMatcher.matches();
        if (isValid == true) {
            byte[] array = geneLine.getBytes();
            if (KMER_SIZE >= array.length) {
                throw new IOException("short read");
            }
            curForwardKmer.setByRead(array, 0);
            curReverseKmer.set(kmerFactory.reverse(curForwardKmer));
            if(curForwardKmer.compareTo(curReverseKmer) >= 0)
                curKmerDir = KmerDir.FORWARD;
            else
                curKmerDir = KmerDir.REVERSE;
            nodeId.set(mateId, readID, 0);
            setNextKmer(array[KMER_SIZE]);
        }
    }
    
    public void setNextKmer(byte nextChar){
        nextForwardKmer.set(curForwardKmer);
        nextForwardKmer.shiftKmerWithNextChar(nextChar);
        nextForwardKmer.set(kmerFactory.reverse(nextForwardKmer));
    }
}
