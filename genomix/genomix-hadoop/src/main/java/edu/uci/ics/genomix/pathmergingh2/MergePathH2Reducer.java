/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.genomix.pathmergingh2;

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
        outputKmer.set(key);
        if (values.hasNext() == true) {
            byte bitFlag = outputValue.getFlag();
            byte bitStartEnd = (byte) (0x81 & bitFlag);
            byte bitPosiNegative = (byte) (0x18 & bitFlag);
            byte succeed = (byte) 0x0F;
            switch (bitPosiNegative) {
                case (byte) 0x08:
                    if (outputValue.getKmerLength() != 0)
                        tmpKmer1.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), key));
                    else
                        tmpKmer1.set(key);
                    byte adjBitMap = outputValue.getAdjBitMap();
                    outputValue = values.next();
                    bitStartEnd = (byte) (0x81 & outputValue.getFlag());
                    if (bitStartEnd == (byte) 0x80) {
                        if (outputValue.getKmerLength() != 0)
                            tmpKmer2.set(kmerFactory.mergeTwoKmer(key, outputValue.getKmer()));
                        else
                            tmpKmer2.set(key);
                        byte tmpFlag = (byte) 0x80;
                        tmpOutputValue.set(outputValue.getAdjBitMap(), tmpFlag, null);
                        mos.getCollector("uncomplete" + I_MERGE, reporter).collect(tmpKmer2, tmpOutputValue);
                    }
                    if (outputValue.getKmerLength() != 0)
                        outputKmer.set(kmerFactory.mergeTwoKmer(tmpKmer1, outputValue.getKmer()));
                    else
                        outputKmer.set(tmpKmer1);
                    succeed = (byte) (succeed & outputValue.getAdjBitMap());
                    adjBitMap = (byte) (adjBitMap & 0xF0);
                    adjBitMap = (byte) (adjBitMap | succeed);
                    byte outputFlag = (byte) (0x81 & bitFlag);
                    outputFlag = (byte) (outputFlag | ((byte) 0x81 & outputValue.getFlag()));
                    outputValue.set(adjBitMap, outputFlag, null);
                    mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputValue);
                    break;
                case (byte) 0x10:
                    if (outputValue.getKmerLength() != 0)
                        tmpKmer1.set(kmerFactory.mergeTwoKmer(key, outputValue.getKmer()));
                    else
                        tmpKmer1.set(key);
                    if (bitStartEnd == (byte) 0x80) {
                        byte tmpFlag = (byte) 0x80;
                        tmpOutputValue.set(outputValue.getAdjBitMap(), tmpFlag, null);
                        mos.getCollector("uncomplete" + I_MERGE, reporter).collect(tmpKmer1, tmpOutputValue);
                    }
                    succeed = (byte) (succeed & outputValue.getAdjBitMap());
                    outputValue = values.next();
                    if (outputValue.getKmerLength() != 0)
                        outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), tmpKmer1));
                    else
                        outputKmer.set(tmpKmer1);
                    adjBitMap = outputValue.getAdjBitMap();
                    adjBitMap = (byte) (adjBitMap & 0xF0);
                    adjBitMap = (byte) (adjBitMap | succeed);                    
                    outputFlag = (byte) (0x81 & bitFlag);
                    outputFlag = (byte) (outputFlag | ((byte) 0x81 & outputValue.getFlag()));
                    outputValue.set(adjBitMap, outputFlag, null);
                    mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputValue);
                    break;
            }
        } else {
            byte bitFlag = outputValue.getFlag();
            byte bitStartEnd = (byte) (0x81 & bitFlag);
            if (bitStartEnd == (byte) 0x81) {
                outputKmer.set(key);
                mos.getCollector("complete" + I_MERGE, reporter).collect(outputKmer, outputValue);
            }
        }
    }
    public void close() throws IOException {
        // TODO Auto-generated method stub
        mos.close();
    }
}
