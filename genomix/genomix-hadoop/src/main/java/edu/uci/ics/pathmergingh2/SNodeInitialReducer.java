package edu.uci.ics.pathmergingh2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.KmerUtil;

@SuppressWarnings("deprecation")
public class SNodeInitialReducer extends MapReduceBase implements
        Reducer<BytesWritable, MergePathValueWritable, BytesWritable, MergePathValueWritable> {
    public BytesWritable outputKmer = new BytesWritable();
    public MergePathValueWritable outputAdjList = new MergePathValueWritable();
    public static int KMER_SIZE;

    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
    }
    @Override
    public void reduce(BytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<BytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputAdjList = values.next();
        outputKmer.set(key);
        byte[] kmerValue = key.getBytes();
        int kmerLength = key.getLength();
        if (values.hasNext() == true) {
            if (outputAdjList.getFlag() == 2) {
                byte bitFlag = 1;
                outputAdjList.set(null, 0, 0, outputAdjList.getAdjBitMap(), bitFlag, outputAdjList.getKmerSize());
                output.collect(outputKmer, outputAdjList);
            } else {
                boolean flag = false;
                while (values.hasNext()) {
                    outputAdjList = values.next();
                    if (outputAdjList.getFlag() == 2) {
                        flag = true;
                        break;
                    }
                    else{
                        
                    }
                }
                if (flag == true) {
                    byte bitFlag = 1;
                    outputAdjList.set(null, 0, 0, outputAdjList.getAdjBitMap(), bitFlag, outputAdjList.getKmerSize());
                    output.collect(outputKmer, outputAdjList);
                }
            }
        } else {
            if (outputAdjList.getFlag() == 2) {
                byte bitFlag = 0;
                outputAdjList.set(null, 0, 0, outputAdjList.getAdjBitMap(), bitFlag, outputAdjList.getKmerSize());
                output.collect(outputKmer, outputAdjList);
            }
            else {
                byte bitFlag = outputAdjList.getFlag();
                byte adjBitMap = outputAdjList.getAdjBitMap();
                byte precurCode = (byte) ((0xC0 & bitFlag) >> 6);
                bitFlag = 0;
                byte[] newKmer = KmerUtil.shiftKmerWithPreCode(KMER_SIZE, kmerValue, 0, kmerLength, precurCode);
                outputKmer.set(newKmer, 0, kmerLength);
                outputAdjList.set(null, 0, 0, adjBitMap, bitFlag, kmerLength);
                output.collect(outputKmer, outputAdjList);
            }
        }
    }
}
