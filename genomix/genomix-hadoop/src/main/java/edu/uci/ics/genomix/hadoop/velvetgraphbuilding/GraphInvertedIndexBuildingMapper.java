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
        String geneLine = value.toString();
        /** first kmer */
        byte[] array = geneLine.getBytes();
        outputKmer.setByRead(array, 0);
        System.out.println(key.get());
        outputVertexID.set((int)key.get(), (byte)0);
        output.collect(outputKmer, outputVertexID);
        /** middle kmer */
        int i = 0; 
        for (i = KMER_SIZE; i < array.length; i++) {
            outputKmer.shiftKmerWithNextChar(array[i]);
            System.out.println((int)key.get());
            outputVertexID.set((int)key.get(), (byte)(i - KMER_SIZE + 1));
            output.collect(outputKmer, outputVertexID);
        }
    }
}
