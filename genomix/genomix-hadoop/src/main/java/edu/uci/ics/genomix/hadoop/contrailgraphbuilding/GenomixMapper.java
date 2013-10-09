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
import org.apache.hadoop.mapred.lib.NLineInputFormat;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.type.EdgeMap;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.Node.DIR;
import edu.uci.ics.genomix.type.ReadIdSet;
import edu.uci.ics.genomix.type.SimpleEntry;
import edu.uci.ics.genomix.type.VKmer;
import edu.uci.ics.genomix.type.Node;
import edu.uci.ics.genomix.type.ReadHeadSet;
import edu.uci.ics.genomix.type.ReadHeadInfo;
import edu.uci.ics.genomix.type.Node.EDGETYPE;

/**
 * GenomixMapper the 1st step of graph building
 * 
 * @author anbangx
 */
@SuppressWarnings({"deprecation"}) 
public class GenomixMapper extends MapReduceBase implements Mapper<LongWritable, Text, VKmer, Node> {

	public enum KMERTYPE{
		PREVIOUS,
		CURRENT,
		NEXT,
	}
	
    public static int KMER_SIZE;
    
    private VKmer curForwardKmer;
    private VKmer curReverseKmer;
    private VKmer nextForwardKmer;
    private VKmer nextReverseKmer;
    private SimpleEntry<VKmer, DIR> curKmerAndDir;
    private SimpleEntry<VKmer, DIR> nextKmerAndDir;
    
    private ReadIdSet readIdSet;
    
    private ReadHeadInfo readHeadInfo;
    private ReadHeadSet readHeadSet;
    
    private Node curNode;
    private Node nextNode;

    byte mateId = (byte) 0;
    boolean fastqFormat = false;
    int lineCount = 0;

    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get(GenomixJobConf.KMER_LENGTH));
        Kmer.setGlobalKmerLength(KMER_SIZE);
        curForwardKmer = new VKmer();
        curReverseKmer = new VKmer();
        nextForwardKmer = new VKmer();
        nextReverseKmer = new VKmer();
        readHeadInfo = new ReadHeadInfo(0);
        readHeadSet = new ReadHeadSet();
        readIdSet = new ReadIdSet();
        curNode = new Node();
        nextNode = new Node();
        lineCount = 0;
        
        // paired-end reads should be named something like dsm3757.01-31-2011.ln6_1.fastq
        // when we have a proper driver, we will set a config field instead of reading in the filename
        String filename = job.get("map.input.file");
        String[] tokens = filename.split("\\.(?=[^\\.]+$)"); // split on the last "." to get the basename and the extension
        if (tokens.length > 2)
            throw new IllegalStateException("Parse error trying to parse filename... split extension tokens are: "
                    + tokens.toString());
        String basename = tokens[0];
        String extension = tokens.length == 2 ? tokens[1] : "";

        if (basename.endsWith("_2")) {
            mateId = (byte) 1;
        } else {
            mateId = (byte) 0;
        }

        if (extension.equals("fastq") || extension.equals("fq")) {
            if (!(job.getInputFormat() instanceof NLineInputFormat)) {
                throw new IllegalStateException("Fastq files require the NLineInputFormat (was " + job.getInputFormat()
                        + " ).");
            }
            if (job.getInt("mapred.line.input.format.linespermap", -1) % 4 != 0) {
                throw new IllegalStateException(
                        "Fastq files require the `mapred.line.input.format.linespermap` option to be divisible by 4 (was "
                                + job.get("mapred.line.input.format.linespermap") + ").");
            }
            fastqFormat = true;
        }
    }

	@Override
    public void map(LongWritable key, Text value, OutputCollector<VKmer, Node> output, Reporter reporter)
            throws IOException {
        lineCount++;
        long readID = 0;
        String geneLine;

        // TODO remember to set NLineInputFormat 
        // TODO relax the input file name restrict
        // TODO current lineCount is incorrect, if we have multiple input files
        if (fastqFormat) {
            if ((lineCount - 1) % 4 == 1) {
                readID = key.get(); // this is actually the offset into the file... will it be the same across all files?? //TODO test this
                geneLine = value.toString().trim();
            } else {
                return; //skip all other lines
            }
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
        if (geneMatcher.matches()) {
            byte[] readLetters = geneLine.getBytes();
            if (KMER_SIZE >= readLetters.length) {
                throw new IOException("short read");
            }
            
            curNode.reset();
            nextNode.reset();
            for(int i = 0; i < readLetters.length - KMER_SIZE; i++){
            	curNode.setAsCopy(nextNode);
                curKmerAndDir = getKmerAndDir(curForwardKmer, curReverseKmer, readLetters, i); 
                nextKmerAndDir = getKmerAndDir(nextForwardKmer, nextReverseKmer, readLetters, i + 1);
                //set node.EdgeMap in meToNext dir of curNode and preToMe dir of nextNode
                setCurAndNextEdgeMap(readID, curKmerAndDir, nextKmerAndDir);
                //only set node.ReadHeadInfo for the first kmer
                if(i == 0){
                	setReadHeadInfo(mateId, readID);
                }
                //set value.coverage = 1
                curNode.setAvgCoverage(1);
                //output mapper result
                output.collect(curKmerAndDir.getKey(), curNode);
            }
            
            output.collect(nextKmerAndDir.getKey(), nextNode);
        }
    }
	
	public SimpleEntry<VKmer, DIR> getKmerAndDir(VKmer forwardKmer, VKmer reverseKmer,
			byte[] readLetters, int startIdx){
        forwardKmer.setFromStringBytes(KMER_SIZE, readLetters, startIdx);
        reverseKmer.setReversedFromStringBytes(KMER_SIZE, readLetters, startIdx);
        boolean forwardIsSmaller = forwardKmer.compareTo(reverseKmer) <= 0;
        
        return new SimpleEntry<VKmer, DIR>(forwardIsSmaller ? forwardKmer : reverseKmer,
        				forwardIsSmaller ? DIR.FORWARD : DIR.REVERSE);
    }
	
	public void setCurAndNextEdgeMap(long readID, SimpleEntry<VKmer, DIR> me, SimpleEntry<VKmer, DIR> neighbor) {
        //set readId
        readIdSet.clear();
        readIdSet.add(readID);
        
        EDGETYPE et = EDGETYPE.getEdgeTypeFromDirToDir(me.getValue(), neighbor.getValue());
        curNode.getEdgeMap(et).put(neighbor.getKey(), readIdSet);
        nextNode.reset();
        nextNode.getEdgeMap(et.mirror()).put(new VKmer(me.getKey()), readIdSet);
    }
	
	public void setReadHeadInfo(byte mateId, long readID){
        readHeadInfo.set(mateId, readID, 0);
        readHeadSet.clear();
        readHeadSet.add(readHeadInfo);
        if (curKmerAndDir.getValue() == DIR.FORWARD)
            curNode.setStartReads(readHeadSet); 
        else
            curNode.setEndReads(readHeadSet);
	}
	
}
