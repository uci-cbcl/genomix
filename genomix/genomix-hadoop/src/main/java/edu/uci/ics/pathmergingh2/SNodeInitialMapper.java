package edu.uci.ics.pathmergingh2;

import java.io.IOException;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.genomix.type.Kmer.GENE_CODE;

@SuppressWarnings("deprecation")
public class SNodeInitialMapper extends MapReduceBase implements
        Mapper<BytesWritable, ByteWritable, BytesWritable, MergePathValueWritable> {

    public static int KMER_SIZE;
    public BytesWritable outputKmer = new BytesWritable();
    public MergePathValueWritable outputAdjList = new MergePathValueWritable();

    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
    }

    boolean measureDegree(byte adjacent) {
        boolean result = true;
        switch (adjacent) {
            case 0:
                result = true;
                break;
            case 1:
                result = false;
                break;
            case 2:
                result = false;
                break;
            case 3:
                result = true;
                break;
            case 4:
                result = false;
                break;
            case 5:
                result = true;
                break;
            case 6:
                result = true;
                break;
            case 7:
                result = true;
                break;
            case 8:
                result = false;
                break;
            case 9:
                result = true;
                break;
            case 10:
                result = true;
                break;
            case 11:
                result = true;
                break;
            case 12:
                result = true;
                break;
            case 13:
                result = true;
                break;
            case 14:
                result = true;
                break;
            case 15:
                result = true;
                break;
        }
        return result;
    }

    @Override
    public void map(BytesWritable key, ByteWritable value,
            OutputCollector<BytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        byte precursor = (byte) 0xF0;
        byte succeed = (byte) 0x0F;
        byte adjBitMap = value.get();
        byte bitFlag = (byte) 0;
        precursor = (byte) (precursor & adjBitMap);
        precursor = (byte) ((precursor & 0xff) >> 4);
        succeed = (byte) (succeed & adjBitMap);
        boolean inDegree = measureDegree(precursor);
        boolean outDegree = measureDegree(succeed);
        byte[] kmerValue = key.getBytes();
        int kmerLength = key.getLength();
        if (inDegree == false && outDegree == false) {//bitflag = 2, it means that it has no need to store the precurCode.
            outputKmer.set(key);
            bitFlag = (byte) 2;
            outputAdjList.set(null, 0, 0, adjBitMap, bitFlag, KMER_SIZE);
            output.collect(outputKmer, outputAdjList);
        }
        else{
            for(int i = 0 ; i < 4; i ++){
                byte temp = 0x01;
                temp  = (byte)(temp << i);
                temp = (byte) (succeed & temp);
                if(temp != 0 ){
                    byte succeedCode = GENE_CODE.getGeneCodeFromBitMap(temp);
                    byte[] newKmer = KmerUtil.shiftKmerWithNextCode(KMER_SIZE, kmerValue, 0, kmerLength, succeedCode);
                    outputKmer.set(newKmer, 0, kmerLength);
                    bitFlag = (byte) 0x80;
                    outputAdjList.set(null, 0, 0, (byte)0, bitFlag, KMER_SIZE);
                    output.collect(outputKmer, outputAdjList);
                }
            }
            for(int i = 0; i < 4;i ++){
                byte temp = 0x01;
                temp = (byte)(temp << i);
                temp = (byte)(precursor & temp);
                if(temp != 0){
                    byte precurCode = GENE_CODE.getGeneCodeFromBitMap(temp);
                    byte[] newKmer = KmerUtil.shiftKmerWithPreCode(KMER_SIZE, kmerValue, 0, kmerLength, precurCode);
                    outputKmer.set(newKmer, 0, kmerLength);
                    bitFlag = (byte)0x40;
                    outputAdjList.set(null, 0, 0, (byte)0, bitFlag, KMER_SIZE);
                    output.collect(outputKmer, outputAdjList);
                }
            }
        }
    }
}
