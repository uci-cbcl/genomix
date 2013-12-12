package edu.uci.ics.genomix.hadoop.buildgraph;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
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

import edu.uci.ics.genomix.data.config.GenomixJobConf;
import edu.uci.ics.genomix.data.types.DIR;
import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.data.types.Kmer;
import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.ReadHeadInfo;
import edu.uci.ics.genomix.data.types.VKmer;
import edu.uci.ics.genomix.data.utils.GeneCode;

/**
 * GenomixMapper the 1st step of graph building
 * 
 * @author anbangx
 */
@SuppressWarnings({ "deprecation" })
public class GenomixMapper extends MapReduceBase implements Mapper<LongWritable, Text, VKmer, Node> {

    public enum KMERTYPE {
        PREVIOUS,
        CURRENT,
        NEXT,
    }

    public static int KMER_SIZE;

    private VKmer curForwardKmer = new VKmer();
    private VKmer curReverseKmer = new VKmer();
    private VKmer nextForwardKmer = new VKmer();
    private VKmer nextReverseKmer = new VKmer();

    private VKmer thisReadSequence = new VKmer();
    private VKmer mateReadSequence = new VKmer();

    private SimpleEntry<VKmer, DIR> curKmerAndDir;
    private SimpleEntry<VKmer, DIR> nextKmerAndDir;

    private ReadHeadInfo readHeadInfo = new ReadHeadInfo();

    private Node curNode = new Node();
    private Node nextNode = new Node();

    boolean fastqFormat = false;
    int lineCount = 0;

    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get(GenomixJobConf.KMER_LENGTH));
        Kmer.setGlobalKmerLength(KMER_SIZE);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<VKmer, Node> output, Reporter reporter)
            throws IOException {
        lineCount++;
        long readID = 0;
        String mate0GeneLine = null;
        String mate1GeneLine = null;

        String[] rawLine = value.toString().split("\\t"); // Read
        if (rawLine.length == 2) {
            readID = Long.parseLong(rawLine[0]);
            mate0GeneLine = rawLine[1];
        } else if (rawLine.length == 3) {
            readID = Long.parseLong(rawLine[0]);
            mate0GeneLine = rawLine[1];
            mate1GeneLine = rawLine[2];
        } else {
            throw new IllegalStateException(
                    "input format is incorrect! only support id'\t'readSeq'\t'mateReadSeq or id'\t'readSeq' but saw"
                    + value.toString() + "which has " + value.toString().split("\\t").length + " elements");
        }

        Pattern genePattern = Pattern.compile("[AGCT]+");
        if (mate0GeneLine != null) {
            Matcher geneMatcher = genePattern.matcher(mate0GeneLine);
            if (geneMatcher.matches()) {
                thisReadSequence.setAsCopy(mate0GeneLine);
                if (mate1GeneLine != null) {
                    mateReadSequence.setAsCopy(mate1GeneLine);
                    readHeadInfo.set((byte) 0, (byte) 0, readID, 0, thisReadSequence, mateReadSequence); // FIXME Hadoop version doesn't record the library
                } else {
                    readHeadInfo.set((byte) 0, (byte) 0, readID, 0, thisReadSequence, null); // FIXME Hadoop version doesn't record the library
                }
                SplitReads(readID, mate0GeneLine.getBytes(), output);
            }
        } else {
            throw new IllegalStateException("thisReadSequence doesn't exist which is not allowed!");
        }
        if (mate1GeneLine != null) {
            Matcher geneMatcher = genePattern.matcher(mate1GeneLine);
            if (geneMatcher.matches()) {
                thisReadSequence.setAsCopy(mate1GeneLine);
                mateReadSequence.setAsCopy(mate0GeneLine);
                readHeadInfo.set((byte) 1, (byte) 0, readID, 0, thisReadSequence, mateReadSequence); // FIXME Hadoop version doesn't record the library
                SplitReads(readID, mate1GeneLine.getBytes(), output);
            }
        }
    }

    private void SplitReads(long readID, byte[] readLetters, OutputCollector<VKmer, Node> output) throws IOException {
        if (KMER_SIZE >= readLetters.length) {
            throw new IOException("short read");
        }
        curNode.reset();
        nextNode.reset();
        //set readId once per line
        curKmerAndDir = getKmerAndDir(curForwardKmer, curReverseKmer, readLetters, 0);
        nextKmerAndDir = getKmerAndDir(nextForwardKmer, nextReverseKmer, readLetters, 1);
        //set node.Edges in meToNext dir of curNode and preToMe dir of nextNode
        setCurAndNextEdges(curKmerAndDir, nextKmerAndDir);
        //set value.coverage = 1
        curNode.setAverageCoverage(1);
        //only set node.ReadHeadInfo for the first kmer
        setReadHeadInfo();
        //output mapper result
        output.collect(curKmerAndDir.getKey(), curNode);

        for (int i = KMER_SIZE; i < readLetters.length - 1; i++) {
            curNode.setAsCopy(nextNode);
            curKmerAndDir = getKmerAndDir(curForwardKmer, curReverseKmer, readLetters[i]);
            nextKmerAndDir = getKmerAndDir(nextForwardKmer, nextReverseKmer, readLetters[i + 1]);
            //set node.Edges in meToNext dir of curNode and preToMe dir of nextNode
            setCurAndNextEdges(curKmerAndDir, nextKmerAndDir);
            //set value.coverage = 1
            curNode.setAverageCoverage(1);
            //output mapper result
            output.collect(curKmerAndDir.getKey(), curNode);
        }

        output.collect(nextKmerAndDir.getKey(), nextNode);
    }

    public SimpleEntry<VKmer, DIR> getKmerAndDir(VKmer forwardKmer, VKmer reverseKmer, byte[] readLetters, int startIdx) {
        forwardKmer.setFromStringBytes(KMER_SIZE, readLetters, startIdx);
        reverseKmer.setReversedFromStringBytes(KMER_SIZE, readLetters, startIdx);
        boolean forwardIsSmaller = forwardKmer.compareTo(reverseKmer) <= 0;

        return new SimpleEntry<VKmer, DIR>(forwardIsSmaller ? forwardKmer : reverseKmer, forwardIsSmaller ? DIR.FORWARD
                : DIR.REVERSE);
    }

    public SimpleEntry<VKmer, DIR> getKmerAndDir(VKmer forwardKmer, VKmer reverseKmer, byte nextChar) {
        forwardKmer.shiftKmerWithNextChar(nextChar);
        reverseKmer.shiftKmerWithPreChar(GeneCode.getComplimentSymbolFromSymbol(nextChar));
        boolean forwardIsSmaller = forwardKmer.compareTo(reverseKmer) <= 0;

        return new SimpleEntry<VKmer, DIR>(forwardIsSmaller ? forwardKmer : reverseKmer, forwardIsSmaller ? DIR.FORWARD
                : DIR.REVERSE);
    }

    public void setCurAndNextEdges(SimpleEntry<VKmer, DIR> curKmerAndDir, SimpleEntry<VKmer, DIR> neighborKmerAndDir) {
        EDGETYPE et = EDGETYPE.getEdgeTypeFromDirToDir(curKmerAndDir.getValue(), neighborKmerAndDir.getValue());
        curNode.getEdges(et).append(neighborKmerAndDir.getKey());
        nextNode.reset();
        nextNode.setAverageCoverage(1);
        nextNode.getEdges(et.mirror()).append(new VKmer(curKmerAndDir.getKey()));
    }

    public void setReadHeadInfo() {
        if (curKmerAndDir.getValue() == DIR.FORWARD) {
            curNode.getUnflippedReadIds().add(readHeadInfo);
        } else {
            curNode.getFlippedReadIds().add(readHeadInfo);
        }
    }

}
