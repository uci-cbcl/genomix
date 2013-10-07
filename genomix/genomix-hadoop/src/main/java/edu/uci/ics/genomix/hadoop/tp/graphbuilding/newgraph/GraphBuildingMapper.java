package edu.uci.ics.genomix.hadoop.tp.graphbuilding.newgraph;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class GraphBuildingMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, KmerBytesWritable, NodeWritable>{
    
    public static enum KmerDir{
        FORWARD,
        REVERSE,
    }
    
    private int kmerSize;
    private PositionWritable readId = new PositionWritable();
    private PositionListWritable readIdList = new PositionListWritable();
    private NodeWritable curNode = new NodeWritable();
    private NodeWritable nextNode = new NodeWritable();

    private KmerBytesWritable curForwardKmer;
    private KmerBytesWritable curReverseKmer;
    private KmerBytesWritable nextForwardKmer;
    private KmerBytesWritable nextReverseKmer;
    private EdgeWritable tempEdge = new EdgeWritable();
    private KmerDir curKmerDir = KmerDir.FORWARD;
    private KmerDir nextKmerDir = KmerDir.FORWARD;
    
    byte mateId = (byte) 0;
    boolean fastqFormat = false;
    
    @Override
    public void configure(JobConf job) {

        kmerSize = Integer.parseInt(job.get("sizeKmer"));
        KmerBytesWritable.setGlobalKmerLength(kmerSize);
        curForwardKmer = new KmerBytesWritable();
        curReverseKmer = new KmerBytesWritable();
        nextForwardKmer = new KmerBytesWritable();
        nextReverseKmer = new KmerBytesWritable();
        // paired-end reads should be named something like dsm3757.01-31-2011.ln6_1.fastq
        // when we have a proper driver, we will set a config field instead of reading in the filename
        String filename = job.get("map.input.file");
        String[] tokens = filename.split("\\.(?=[^\\.]+$)");  // split on the last "." to get the basename and the extension
        if (tokens.length > 2) 
            throw new IllegalStateException("Parse error trying to parse filename... split extension tokens are: " + tokens.toString());
        String basename = tokens[0];
        String extension = tokens.length == 2 ? tokens[1] : ""; 
        
        if (basename.endsWith("_2")) {
            mateId = (byte) 1;
        } else {
            mateId = (byte) 0;
        }
        
        if (extension.equals("fastq") || extension.equals("fq")) {
/*            if (! (job.getInputFormat() instanceof NLineInputFormat)) {
                throw new IllegalStateException("Fastq files require the NLineInputFormat (was " + job.getInputFormat() + " ).");
            }
            if (job.getInt("mapred.line.input.format.linespermap", -1) % 4 != 0) {
                throw new IllegalStateException("Fastq files require the `mapred.line.input.format.linespermap` option to be divisible by 4 (was " + job.get("mapred.line.input.format.linespermap") + ").");
            }*/
            fastqFormat = true;
        }
    }
    
    @Override
    public void map(LongWritable key, Text value, OutputCollector<KmerBytesWritable, NodeWritable> output,
            Reporter reporter) throws IOException {
//        lineCount++;
        long readID = 0;
        String geneLine;
        
        if (fastqFormat) {
//            if ((lineCount - 1) % 4 == 1) {
                readID = key.get();   //TODO test this
                geneLine = value.toString().trim();
//            } else {
//                return;  //skip all other lines
//            }
        } else {
            String[] rawLine = value.toString().split("\\t"); // Read the Real Gene Line
            if (rawLine.length != 2) {
                throw new IOException("invalid data");
            }
            readID = Long.parseLong(rawLine[0]);
            geneLine = rawLine[1];
        }
        
        Pattern genePattern = Pattern.compile("[AGCT]+");
        Matcher geneMatcher = genePattern.matcher(geneLine);
        boolean isValid = geneMatcher.matches();
        if (isValid == true) {
            SplitReads(readID, geneLine.getBytes(), output);
        }
    }
    private void SplitReads(long readID, byte[] array, OutputCollector<KmerBytesWritable, NodeWritable> output) throws IOException {
        /*first kmer*/
        if (kmerSize >= array.length) {
            throw new IllegalArgumentException("kmersize (k="+kmerSize+") is larger than the read length (" + array.length + ")");
        }
        curNode.reset();
        nextNode.reset();
        tempEdge.reset();
        curNode.setAvgCoverage(1);
        nextNode.setAvgCoverage(1);
        curForwardKmer.setFromStringBytes(array, 0);
        curReverseKmer.setReversedFromStringBytes(array, 0);
        curKmerDir = curForwardKmer.compareTo(curReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
        nextForwardKmer.setAsCopy(curForwardKmer);
        nextKmerDir = setNextKmer(nextForwardKmer, nextReverseKmer, array[kmerSize]);
        setThisReadId(readIdList, readId, mateId, readID, 0);
        if(curKmerDir == KmerDir.FORWARD)
            curNode.getStartReads().append(readId);
        else
            curNode.getEndReads().append(readId);
        setEdgeAndThreadListForCurAndNextKmer(curKmerDir, curNode, nextKmerDir, nextNode, readIdList, tempEdge);
        spillRecordOut(curForwardKmer, curReverseKmer, curKmerDir, curNode, output);
        /*middle kmer*/
        int i = kmerSize + 1;
        for (; i < array.length; i++) {
            curForwardKmer.setAsCopy(nextForwardKmer);
            curReverseKmer.setAsCopy(nextReverseKmer);
            curKmerDir = nextKmerDir;
            curNode.setAsCopy(nextNode);
            nextNode.reset();
            nextNode.setAvgCoverage(1);
            nextKmerDir = setNextKmer(nextForwardKmer, nextReverseKmer, array[i]);
            setEdgeAndThreadListForCurAndNextKmer(curKmerDir, curNode, nextKmerDir, nextNode, readIdList, tempEdge);
            spillRecordOut(curForwardKmer, curReverseKmer, curKmerDir, curNode, output);
        }

        /*last kmer*/
        spillRecordOut(nextForwardKmer, nextReverseKmer, nextKmerDir, nextNode, output);
    }

    public void setThisReadId(PositionListWritable readIdList, PositionWritable readId, byte mateId, long readID, int posId) {
        readId.set(mateId, readID, posId);
        readIdList.reset();
        readIdList.append(readId);
    }

    public KmerDir setNextKmer(KmerBytesWritable forwardKmer, KmerBytesWritable ReverseKmer,
            byte nextChar) {
        forwardKmer.shiftKmerWithNextChar(nextChar);
        ReverseKmer.setReversedFromStringBytes(forwardKmer.toString().getBytes(), forwardKmer.getOffset());
        return forwardKmer.compareTo(ReverseKmer) <= 0 ? KmerDir.FORWARD : KmerDir.REVERSE;
    }

    public void spillRecordOut(KmerBytesWritable forwardKmer, KmerBytesWritable reverseKmer, KmerDir curKmerDir,
            NodeWritable node, OutputCollector<KmerBytesWritable, NodeWritable> output) throws IOException {
        switch (curKmerDir) {
            case FORWARD:
                output.collect(forwardKmer, node);
                break;
            case REVERSE:
                output.collect(reverseKmer, node);
                break;
        }
    }

    public void setEdgeAndThreadListForCurAndNextKmer(KmerDir curKmerDir, NodeWritable curNode, KmerDir nextKmerDir,
            NodeWritable nextNode, PositionListWritable readIdList, EdgeWritable tempEdge) {
        if (curKmerDir == KmerDir.FORWARD && nextKmerDir == KmerDir.FORWARD) {
            tempEdge.setAsCopy(nextForwardKmer, readIdList);
            curNode.getEdgeList(EDGETYPE.FF).add(tempEdge);
            tempEdge.setAsCopy(curForwardKmer, readIdList);
            nextNode.getEdgeList(EDGETYPE.RR).add(tempEdge);
        }
        if (curKmerDir == KmerDir.FORWARD && nextKmerDir == KmerDir.REVERSE) {
            tempEdge.setAsCopy(nextReverseKmer, readIdList);
            curNode.getEdgeList(EDGETYPE.FR).add(tempEdge);
            tempEdge.setAsCopy(curForwardKmer, readIdList);
            nextNode.getEdgeList(EDGETYPE.FR).add(tempEdge);
        }
        if (curKmerDir == KmerDir.REVERSE && nextKmerDir == KmerDir.FORWARD) {
            tempEdge.setAsCopy(nextForwardKmer, readIdList);
            curNode.getEdgeList(EDGETYPE.RF).add(tempEdge);
            tempEdge.setAsCopy(curReverseKmer, readIdList);
            nextNode.getEdgeList(EDGETYPE.RF).add(tempEdge);
        }
        if (curKmerDir == KmerDir.REVERSE && nextKmerDir == KmerDir.REVERSE) {
            tempEdge.setAsCopy(nextReverseKmer, readIdList);
            curNode.getEdgeList(EDGETYPE.RR).add(tempEdge);
            tempEdge.setAsCopy(curReverseKmer, readIdList);
            nextNode.getEdgeList(EDGETYPE.FF).add(tempEdge);
        }
    }
}
