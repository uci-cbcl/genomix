package edu.uci.ics.genomix.hadoop.experiments;

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
import edu.uci.ics.genomix.experiment.KmerVertexID;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

public class GraphKmerInvertedIndexMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, KmerBytesWritable, KmerVertexID> {
    
    public static int KMER_SIZE;
    public KmerVertexID outputVertexID;
    public KmerBytesWritable outputKmer;

    @Override
    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
        outputVertexID = new KmerVertexID();
        outputKmer = new KmerBytesWritable(KMER_SIZE);
    }
    @Override
    public void map(LongWritable key, Text value, OutputCollector<KmerBytesWritable, KmerVertexID> output,
            Reporter reporter) throws IOException {
        
        String geneLine = value.toString();
        /** first kmer */
//        byte count = 1;
        byte[] array = geneLine.getBytes();
        outputKmer.setByRead(array, 0);
        byte pre = 0;
        //byte next = GeneCode.getAdjBit(array[KMER_SIZE]);
        //byte adj = GeneCode.mergePreNextAdj(pre, next);
        outputVertexID.set((int)key.get(), (byte)0);
        output.collect(outputKmer, outputVertexID);
        /** middle kmer */
        for (int i = KMER_SIZE; i < array.length - 1; i++) {
            pre = GeneCode.getBitMapFromGeneCode(outputKmer.shiftKmerWithNextChar(array[i]));
//            next = GeneCode.getAdjBit(array[i + 1]);
//            adj = GeneCode.mergePreNextAdj(pre, next);
            outputVertexID.set((int)key.get(), (byte)(i - KMER_SIZE + 1));
            output.collect(outputKmer, outputVertexID);
        }
        /** last kmer */
        pre = GeneCode.getBitMapFromGeneCode(outputKmer.shiftKmerWithNextChar(array[array.length - 1]));
//        next = 0;
//        adj = GeneCode.mergePreNextAdj(pre, next);
        outputVertexID.set((int)key.get(), (byte)(array.length - 1 + 1));
        output.collect(outputKmer, outputVertexID);
    }
}
