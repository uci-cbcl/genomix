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
public class ENodeInitialReducer extends MapReduceBase implements
        Reducer<BytesWritable, MergePathValueWritable, BytesWritable, MergePathValueWritable> {
    public BytesWritable outputKmer = new BytesWritable();
    public MergePathValueWritable outputAdjList = new MergePathValueWritable();
    

    @Override
    public void reduce(BytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<BytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputAdjList = values.next();
        outputKmer.set(key);
        if (values.hasNext() == true) {
            byte bitFlag = outputAdjList.getFlag();
            bitFlag = (byte) (bitFlag & 0xFE);
            if (bitFlag == 2) {
                bitFlag =  (byte) (0x80 | outputAdjList.getFlag());
                outputAdjList.set(null, 0, 0, outputAdjList.getAdjBitMap(), bitFlag, outputAdjList.getKmerLength());
                output.collect(outputKmer, outputAdjList);

            } else {
                boolean flag = false;
                while (values.hasNext()) {
                    outputAdjList = values.next();
                    if (outputAdjList.getFlag() == 2) {
                        flag = true;
                        break;
                    }
                }
                if (flag == true) {
                    bitFlag =  (byte) (0x80 | outputAdjList.getFlag());
                    outputAdjList.set(null, 0, 0, outputAdjList.getAdjBitMap(), bitFlag, outputAdjList.getKmerLength());
                    output.collect(outputKmer, outputAdjList);
                }
            }
        } else {
            byte bitFlag = outputAdjList.getFlag();
            bitFlag = (byte) (bitFlag & 0xFE);
            if (bitFlag == 2) {
                bitFlag = 0;
                outputAdjList.set(null, 0, 0, outputAdjList.getAdjBitMap(), bitFlag, outputAdjList.getKmerLength());
                output.collect(outputKmer, outputAdjList);
            } 
        }
    }
}

