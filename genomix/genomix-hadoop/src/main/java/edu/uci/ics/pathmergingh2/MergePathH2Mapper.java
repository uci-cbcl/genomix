package edu.uci.ics.pathmergingh2;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.genomix.type.Kmer.GENE_CODE;
import edu.uci.ics.pathmerging.MergePathValueWritable;

public class MergePathH2Mapper extends MapReduceBase implements
        Mapper<BytesWritable, MergePathValueWritable, BytesWritable, MergePathValueWritable> {

    public static int KMER_SIZE;
    public BytesWritable outputKmer = new BytesWritable();
    public MergePathValueWritable outputAdjList = new MergePathValueWritable();

    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", 0);
    }

    @Override
    public void map(BytesWritable key, MergePathValueWritable value,
            OutputCollector<BytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        byte bitFlag = value.getFlag();
        byte bitStartEnd = (byte) (0x81 & bitFlag);
        byte succeed = (byte) 0x0F;
        byte adjBitMap = value.getAdjBitMap();
        succeed = (byte) (succeed & adjBitMap);
        byte[] kmerValue = key.getBytes();
        int kmerLength = key.getLength();
        switch (bitStartEnd) {
            case 0x01:
                byte succeedCode = GENE_CODE.getGeneCodeFromBitMap(succeed);
                int originalByteNum = Kmer.getByteNumFromK(KMER_SIZE);
                byte[] tmpKmer = KmerUtil.getLastKmerFromChain(KMER_SIZE, value.getKmerSize(), kmerValue, 0, kmerLength);
                byte[] newKmer = KmerUtil.shiftKmerWithNextCode(KMER_SIZE, tmpKmer, 0, tmpKmer.length, succeedCode);
                outputKmer.set(newKmer, 0, originalByteNum);

                int mergeByteNum = Kmer.getByteNumFromK(value.getKmerSize() - (KMER_SIZE - 1));
                byte[] mergeKmer = KmerUtil.getFirstKmerFromChain(value.getKmerSize() - (KMER_SIZE - 1),
                        value.getKmerSize(), kmerValue, 0, kmerLength);

                bitFlag = (byte) (bitFlag | 0x08);
                outputAdjList.set(mergeKmer, 0, mergeByteNum, adjBitMap, bitFlag, value.getKmerSize() - (KMER_SIZE - 1));
                output.collect(outputKmer, outputAdjList);
                break;
            case (byte) 0x80:
                outputKmer.set(key);
                outputAdjList.set(value);
                output.collect(outputKmer, outputAdjList);
                break;
            case 0x00:
                succeedCode = GENE_CODE.getGeneCodeFromBitMap(succeed);
                originalByteNum = Kmer.getByteNumFromK(KMER_SIZE);
                tmpKmer = KmerUtil.getLastKmerFromChain(KMER_SIZE, value.getKmerSize(), kmerValue, 0, kmerLength);
                newKmer = KmerUtil.shiftKmerWithNextCode(KMER_SIZE, tmpKmer, 0, tmpKmer.length, succeedCode);
                outputKmer.set(newKmer, 0, originalByteNum);

                mergeByteNum = Kmer.getByteNumFromK(value.getKmerSize() - (KMER_SIZE - 1));
                mergeKmer = KmerUtil.getFirstKmerFromChain(value.getKmerSize() - (KMER_SIZE - 1), value.getKmerSize(),
                        kmerValue, 0, kmerLength);

                bitFlag = (byte) (bitFlag | 0x08);
                outputAdjList
                        .set(mergeKmer, 0, mergeByteNum, adjBitMap, bitFlag, value.getKmerSize() - (KMER_SIZE - 1));
                output.collect(outputKmer, outputAdjList);

                outputKmer.set(key);
                outputAdjList.set(value);
                output.collect(outputKmer, outputAdjList);
                break;
            case (byte) 0x81:
                break;
        }
    }
}
