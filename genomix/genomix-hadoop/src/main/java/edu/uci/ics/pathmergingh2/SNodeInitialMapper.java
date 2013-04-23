package edu.uci.ics.pathmergingh2;

import java.io.IOException;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;

@SuppressWarnings("deprecation")
public class SNodeInitialMapper extends MapReduceBase implements
        Mapper<KmerBytesWritable, ByteWritable, KmerBytesWritable, MergePathValueWritable> {

    public int KMER_SIZE;
    public KmerBytesWritable outputKmer;
    public MergePathValueWritable outputAdjList;

    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
        outputKmer = new KmerBytesWritable(KMER_SIZE);
        outputAdjList = new MergePathValueWritable();
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
    public void map(KmerBytesWritable key, ByteWritable value,
            OutputCollector<KmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        byte precursor = (byte) 0xF0;
        byte succeed = (byte) 0x0F;
        byte adjBitMap = value.get();
        byte bitFlag = (byte) 0;
        precursor = (byte) (precursor & adjBitMap);
        precursor = (byte) ((precursor & 0xff) >> 4);
        succeed = (byte) (succeed & adjBitMap);
        boolean inDegree = measureDegree(precursor);
        boolean outDegree = measureDegree(succeed);
        
        if (inDegree == false && outDegree == false) {
            outputKmer.set(key);
            bitFlag = (byte) 2;
            outputAdjList.set(null, 0, 0, adjBitMap, bitFlag, 0);
            output.collect(outputKmer, outputAdjList);
        }
        else{
            for(int i = 0 ; i < 4; i ++){
                byte temp = 0x01;
                byte shiftedCode = 0;
                temp  = (byte)(temp << i);
                temp = (byte) (succeed & temp);
                if(temp != 0 ){
                    byte succeedCode = GeneCode.getGeneCodeFromBitMap(temp);
                    shiftedCode = key.shiftKmerWithNextCode(succeedCode);
                    outputKmer.set(key);
                    bitFlag = (byte)0x01;
                    outputAdjList.set(null, 0, 0, (byte)0, bitFlag, 0);
                    output.collect(outputKmer, outputAdjList);
                    key.shiftKmerWithPreCode(shiftedCode);
                }
            }
            for(int i = 0; i < 4;i ++){
                byte temp = 0x01;
                byte shiftedCode = 0;
                temp = (byte)(temp << i);
                temp = (byte)(precursor & temp);
                if(temp != 0){
                    byte precurCode = GeneCode.getGeneCodeFromBitMap(temp);
                    shiftedCode = key.shiftKmerWithPreCode(precurCode);
                    outputKmer.set(key);
                    bitFlag = (byte)0x80;
                    outputAdjList.set(null, 0, 0, (byte)0, bitFlag, 0);
                    output.collect(outputKmer, outputAdjList);
                    key.shiftKmerWithNextCode(shiftedCode);
                }
            }
        }
    }
}
