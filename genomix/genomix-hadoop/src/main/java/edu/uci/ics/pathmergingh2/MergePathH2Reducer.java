package edu.uci.ics.pathmergingh2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.pathmerging.MergePathValueWritable;

public class MergePathH2Reducer extends MapReduceBase implements
        Reducer<BytesWritable, MergePathValueWritable, BytesWritable, MergePathValueWritable> {
    public BytesWritable outputKmer = new BytesWritable();
    public static int KMER_SIZE;
    public MergePathValueWritable outputAdjList = new MergePathValueWritable();
    MultipleOutputs mos = null;
    public static int I_MERGE;

    public void configure(JobConf job) {
        mos = new MultipleOutputs(job);
        I_MERGE = Integer.parseInt(job.get("iMerge"));
        KMER_SIZE = job.getInt("sizeKmer", 0);
    }

    public void reduce(BytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<BytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputKmer.set(key);
        outputAdjList = values.next();
        if (values.hasNext() == true) {
            byte[] keyBytes = key.getBytes();
            int keyLength = key.getLength();
            byte bitFlag = outputAdjList.getFlag();
            byte bitStartEnd = (byte) (0x81 & bitFlag);
            byte bitPosiNegative = (byte) (0x18 & bitFlag);
            byte succeed = (byte) 0x0F;
            byte adjBitMap = outputAdjList.getAdjBitMap();

            switch (bitPosiNegative) {
                case (byte) 0x08:
                    succeed = (byte) (succeed & adjBitMap);
                    if (bitStartEnd == 0x10) {
                        output.collect(outputKmer, outputAdjList);
                    }
                    int kmerSize = outputAdjList.getKmerSize();
                    int mergeByteNum = Kmer.getByteNumFromK(KMER_SIZE + kmerSize);
                    byte[] valueBytes = outputAdjList.getBytes();
                    int valueLength = outputAdjList.getLength();
                    byte[] mergeKmer = KmerUtil.mergeTwoKmer(outputAdjList.getKmerSize(), valueBytes, 0, valueLength,
                            KMER_SIZE, keyBytes, 0, keyLength);
                    outputAdjList = values.next();
                    valueBytes = outputAdjList.getBytes();
                    valueLength = outputAdjList.getLength();
                    adjBitMap = outputAdjList.getAdjBitMap();
                    mergeKmer = KmerUtil.mergeTwoKmer(outputAdjList.getKmerSize(), valueBytes, 0, valueLength,
                            KMER_SIZE + kmerSize, mergeKmer, 0, mergeByteNum);
                    outputKmer.set(mergeKmer, 0, mergeByteNum);

                    adjBitMap = (byte) (adjBitMap & 0xF0);
                    adjBitMap = (byte) (adjBitMap | succeed);
                    byte flag = (byte) (bitFlag | 0x80);//zhe you wen ti
                    outputAdjList.set(null, 0, 0, adjBitMap, flag, KMER_SIZE + kmerSize + outputAdjList.getKmerSize());
                    mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputAdjList);
                    break;
                case (byte) 0x10:
                    byte[] haha = new byte[outputAdjList.getLength()];
                    for (int i = 0; i < haha.length; i++) {
                        haha[i] = outputAdjList.getBytes()[i];
                    }
                    byte trueadj = outputAdjList.getAdjBitMap();
                    int trueKmersize = outputAdjList.getKmerSize();
                    outputAdjList = values.next();
                    succeed = (byte) (succeed & adjBitMap);
                    if (bitStartEnd == 0x10) {
                        output.collect(outputKmer, outputAdjList);
                    }
                    kmerSize = outputAdjList.getKmerSize();
                    mergeByteNum = Kmer.getByteNumFromK(KMER_SIZE + kmerSize);
                    valueBytes = outputAdjList.getBytes();
                    valueLength = outputAdjList.getLength();
                    mergeKmer = KmerUtil.mergeTwoKmer(outputAdjList.getKmerSize(), valueBytes, 0, valueLength,
                            KMER_SIZE, keyBytes, 0, keyLength);
                    mergeKmer = KmerUtil.mergeTwoKmer(trueKmersize, haha, 0, haha.length,
                            KMER_SIZE + kmerSize, mergeKmer, 0, mergeByteNum);
                    outputKmer.set(mergeKmer, 0, mergeByteNum);
                    
                    trueadj = (byte) (adjBitMap & 0xF0);
                    trueadj = (byte) (adjBitMap | succeed);
//                    byte flag = (byte) (bitFlag | 0x80);
                    outputAdjList.set(null, 0, 0, trueadj, bitFlag, KMER_SIZE + kmerSize + outputAdjList.getKmerSize());
                    mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputAdjList);
                    break;
            }
        }
        else{
            //shi bu shi yijing jieshu ---> complete
        }
    }
}
