package edu.uci.ics.pathmergingh2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

@SuppressWarnings("deprecation")
public class MergePathH2Reducer extends MapReduceBase implements
        Reducer<VKmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {
    private VKmerBytesWritableFactory kmerFactory;
    private VKmerBytesWritable outputKmer;
    private VKmerBytesWritable tmpKmer1;
    private VKmerBytesWritable tmpKmer2;
    private int KMER_SIZE;
    private MergePathValueWritable outputValue;
    private MergePathValueWritable tmpOutputValue;

    MultipleOutputs mos = null;
    private int I_MERGE;

    public void configure(JobConf job) {
        mos = new MultipleOutputs(job);
        I_MERGE = Integer.parseInt(job.get("iMerge"));
        KMER_SIZE = job.getInt("sizeKmer", 0);
        outputValue = new MergePathValueWritable();
        tmpOutputValue = new MergePathValueWritable();
        kmerFactory = new VKmerBytesWritableFactory(KMER_SIZE);
        outputKmer = new VKmerBytesWritable(KMER_SIZE);
        tmpKmer1 = new VKmerBytesWritable(KMER_SIZE);
        tmpKmer2 = new VKmerBytesWritable(KMER_SIZE);
    }

    @SuppressWarnings("unchecked")
    public void reduce(VKmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputValue = values.next();
        if (values.hasNext() == true) {
            byte bitFlag = outputValue.getFlag();
            byte bitStartEnd = (byte) (0x81 & bitFlag);
            byte bitPosiNegative = (byte) (0x18 & bitFlag);
            byte succeed = (byte) 0x0F;            

            switch (bitPosiNegative) {
                case (byte) 0x08:
                    tmpKmer1.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), key));
                byte adjBitMap = outputValue.getAdjBitMap();
                    outputValue = values.next();
                    if (bitStartEnd == 0x80) {
                        tmpKmer2.set(kmerFactory.mergeTwoKmer(key, outputValue.getKmer()));
                        tmpOutputValue.set(null, 0, 0, outputValue.getAdjBitMap(), outputValue.getFlag(), 0);
                        mos.getCollector("uncomplete" + I_MERGE, reporter).collect(tmpKmer2, tmpOutputValue);
                    }

                    outputKmer.set(kmerFactory.mergeTwoKmer(tmpKmer1, outputValue.getKmer()));
                    succeed = (byte) (succeed & outputValue.getFlag());
                    adjBitMap = (byte) (adjBitMap & 0xF0);
                    adjBitMap = (byte) (adjBitMap | succeed);
                    byte outputFlag = (byte) (0x81 & bitFlag);
                    outputFlag = (byte) (outputFlag & outputValue.getFlag());
                    outputValue.set(null, 0, 0, adjBitMap, outputFlag, 0);
                    mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputValue);
                    break;
                case (byte) 0x10:
                    tmpKmer1.set(kmerFactory.mergeTwoKmer(key, outputValue.getKmer()));
                    if (bitStartEnd == 0x80) {
                        tmpOutputValue.set(null, 0, 0, outputValue.getAdjBitMap(), outputValue.getFlag(), 0);
                        mos.getCollector("uncomplete" + I_MERGE, reporter).collect(tmpKmer1, tmpOutputValue);
                    }
                    succeed = (byte) (succeed & outputValue.getFlag());
                    outputValue = values.next();
                    outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), tmpKmer1));
                    adjBitMap = outputValue.getAdjBitMap();
                    adjBitMap = (byte) (adjBitMap & 0xF0);
                    adjBitMap = (byte) (adjBitMap | succeed);
                    outputFlag = (byte) (0x81 & bitFlag);
                    outputFlag = (byte) (outputFlag & outputValue.getFlag());
                    outputValue.set(null, 0, 0, adjBitMap, outputFlag, 0);
                    mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputValue);
                    break;
            }
        } else {
            byte bitFlag = outputValue.getFlag();
            byte bitStartEnd = (byte) (0x81 & bitFlag);
            if(bitStartEnd == 0x81) {
                outputKmer.set(key);
                mos.getCollector("complete" + I_MERGE, reporter).collect(outputKmer, outputValue);
            }
        }
    }
}
