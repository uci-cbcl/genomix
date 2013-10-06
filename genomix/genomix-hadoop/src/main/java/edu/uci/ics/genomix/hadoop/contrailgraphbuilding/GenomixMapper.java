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
	
	public enum MEMORY{
		BLOCK1,
		BLOCK2,
		BLOCK3;
		
		public static MEMORY getFreeBlock(int i){
			switch(i % 3){
				case 1:
					return MEMORY.BLOCK1;
				case 2:
					return MEMORY.BLOCK2;
				case 3:
					return MEMORY.BLOCK3;
				default:
					throw new IllegalStateException("Programmer error!!!");
			}
		}
	}
	
    public static int KMER_SIZE;
    
    private VKmer preForwardKmer;
    private VKmer preReverseKmer;
    private VKmer curForwardKmer;
    private VKmer curReverseKmer;
    private VKmer nextForwardKmer;
    private VKmer nextReverseKmer;
    private SimpleEntry<VKmer, DIR> preKmerAndDir;
    private SimpleEntry<VKmer, DIR> curKmerAndDir;
    private SimpleEntry<VKmer, DIR> nextKmerAndDir;
    
    private ReadIdSet readIdSet;
    private EdgeMap edgeMap;
    
    private ReadHeadInfo readHeadInfo;
    private ReadHeadSet readHeadSet;
    
    private Node outputNode;

    byte mateId = (byte) 0;
    boolean fastqFormat = false;
    int lineCount = 0;

    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get(GenomixJobConf.KMER_LENGTH));
        Kmer.setGlobalKmerLength(KMER_SIZE);
        preForwardKmer = new VKmer();
        preReverseKmer = new VKmer();
        curForwardKmer = new VKmer();
        curReverseKmer = new VKmer();
        nextForwardKmer = new VKmer();
        nextReverseKmer = new VKmer();
        readHeadInfo = new ReadHeadInfo(0);
        readHeadSet = new ReadHeadSet();
        readIdSet = new ReadIdSet();
        edgeMap = new EdgeMap();
        outputNode = new Node();
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
        boolean isValid = geneMatcher.matches();
        if (isValid == true) {
            byte[] array = geneLine.getBytes();
            if (KMER_SIZE >= array.length) {
                throw new IOException("short read");
            }
            /** first kmer **/
            outputNode.reset();
            curKmerAndDir = getKmerAndDir(array, 0, MEMORY.BLOCK2);
            
            nextKmerAndDir = getKmerAndDir(array, 1, MEMORY.BLOCK3);
            //set node.EdgeMap in meToNext dir
            setEdgeMap(readID, curKmerAndDir, nextKmerAndDir, KMERTYPE.NEXT);
            //set value.ReadHeadInfo because this is the first kmer in read
            setReadHeadInfo(mateId, readID);
            //set value.coverage = 1
            outputNode.setAvgCoverage(1);
            //output mapper result
            output.collect(curKmerAndDir.getKey(), outputNode);

            /** middle kmer **/
            for (int i = KMER_SIZE + 1; i < array.length; i++) {
                outputNode.reset();
                preKmerAndDir = curKmerAndDir; //old curKmer becomes current preKmer
                curKmerAndDir = nextKmerAndDir; //old nextKmer becomes current curKmer
                
                nextKmerAndDir = getKmerAndDir(array, i - KMER_SIZE + 1, MEMORY.getFreeBlock(i - KMER_SIZE));
                //set node.EdgeMap in meToPrev and meToNext dir
                setEdgeMap(readID, curKmerAndDir, preKmerAndDir, KMERTYPE.PREVIOUS);
                setEdgeMap(readID, curKmerAndDir, nextKmerAndDir, KMERTYPE.NEXT);
                //set coverage = 1
                outputNode.setAvgCoverage(1);
                //output mapper result
                output.collect(curKmerAndDir.getKey(), outputNode);
            }

            /** last kmer **/
            outputNode.reset();
            preKmerAndDir = curKmerAndDir; //old curKmer becomes current preKmer
            curKmerAndDir = nextKmerAndDir; //old nextKmer becomes current curKmer
            //set node.EdgeMap in meToPrev dir
            setEdgeMap(readID, curKmerAndDir, preKmerAndDir, KMERTYPE.PREVIOUS);
            //set coverage = 1
            outputNode.setAvgCoverage(1);
            //output mapper result
            output.collect(curKmerAndDir.getKey(), outputNode);
        }
    }
	
	public SimpleEntry<VKmer, DIR> getKmerAndDir(byte[] array, int startIdx, MEMORY block){
		VKmer forwardKmer;
		VKmer reverseKmer;
        switch(block){
	    	case BLOCK1:
	    		forwardKmer = preForwardKmer;
	    		reverseKmer = preReverseKmer;
	    		break;
	    	case BLOCK2:
	    		forwardKmer = curForwardKmer;
	    		reverseKmer = curReverseKmer;
	    		break;
	    	case BLOCK3:
	    		forwardKmer = nextForwardKmer;
	    		reverseKmer = nextReverseKmer;
	    		break;
			default:
				throw new IllegalStateException("In setKmerAndDir, kmer type can only be TEMP or CURRENT or NEXT!");
        }
        forwardKmer.setFromStringBytes(KMER_SIZE, array, startIdx);
        reverseKmer.setReversedFromStringBytes(KMER_SIZE, array, startIdx);
        boolean forwardIsSmaller = forwardKmer.compareTo(reverseKmer) <= 0;
        
        return new SimpleEntry<VKmer, DIR>(forwardIsSmaller ? forwardKmer : reverseKmer,
        				forwardIsSmaller ? DIR.FORWARD : DIR.REVERSE);
    }
	
	public void setEdgeMap(long readID, SimpleEntry<VKmer, DIR> me, 
    		SimpleEntry<VKmer, DIR> neighbor, KMERTYPE kmerType) {
        //set readId
        readIdSet.clear();
        readIdSet.add(readID);
        edgeMap.clear();
        edgeMap.put(neighbor.getKey(), readIdSet);
        
        EDGETYPE et = null;
        switch(kmerType){
        	case NEXT:
        		et = EDGETYPE.getEdgeTypeFromDirToDir(me.getValue(), neighbor.getValue());
        		break;
        	case PREVIOUS:
        		et = EDGETYPE.getEdgeTypeFromDirToDir(neighbor.getValue(), me.getValue()).mirror();
        		break;
    		default:
    			throw new IllegalStateException("Invalid input kmer type!");
        }
        
        outputNode.setEdgeMap(et, edgeMap);
    }
	
	public void setReadHeadInfo(byte mateId, long readID){
        readHeadInfo.set(mateId, readID, 0);
        readHeadSet.clear();
        readHeadSet.add(readHeadInfo);
        if (curKmerAndDir.getValue() == DIR.FORWARD)
            outputNode.setStartReads(readHeadSet);
        else
            outputNode.setEndReads(readHeadSet);
	}
	
}
