package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionWritable;

@SuppressWarnings("deprecation")
public class GraphInvertedIndexBuildingMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, KmerBytesWritable, PositionWritable> {

    public static int KMER_SIZE;
    public PositionWritable outputVertexID;
    public KmerBytesWritable outputKmer;

    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
        outputVertexID = new PositionWritable();
        outputKmer = new KmerBytesWritable(KMER_SIZE);
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<KmerBytesWritable, PositionWritable> output,
            Reporter reporter) throws IOException {
        /** first kmer */
        String[] rawLine = value.toString().split("\\t"); // Read the Real Gene Line
        if (rawLine.length != 2) {
            throw new IOException("invalid data");
        }
        int readID = 0;
        readID = Integer.parseInt(rawLine[0]);
        String geneLine = rawLine[1];
        byte[] array = geneLine.getBytes();
        if (KMER_SIZE >= array.length) {
            throw new IOException("short read");
        }
        outputKmer.setByRead(array, 0);
        outputVertexID.set(readID, (byte) 1);
        output.collect(outputKmer, outputVertexID);
        /** middle kmer */
        for (int i = KMER_SIZE; i < array.length; i++) {
            outputKmer.shiftKmerWithNextChar(array[i]);
            outputVertexID.set(readID, (byte) (i - KMER_SIZE + 2));
            output.collect(outputKmer, outputVertexID);
        }
        /** reverse first kmer */
        outputKmer.setByReadReverse(array, 0);
        outputVertexID.set(readID, (byte) -1);
        /** reverse middle kmer */
        for (int i = KMER_SIZE; i < array.length; i++) {
            outputKmer.shiftKmerWithPreCode(GeneCode.getPairedCodeFromSymbol(array[i]));
            outputVertexID.set(readID, (byte) (-2 + KMER_SIZE - i));
            output.collect(outputKmer, outputVertexID);
        }
    }
}
