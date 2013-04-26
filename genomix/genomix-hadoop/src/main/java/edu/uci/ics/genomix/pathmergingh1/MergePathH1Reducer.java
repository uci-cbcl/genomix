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
package edu.uci.ics.genomix.pathmergingh1;

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
public class MergePathH1Reducer extends MapReduceBase implements
        Reducer<VKmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {
    private VKmerBytesWritableFactory kmerFactory;
    private VKmerBytesWritable outputKmer;
    private VKmerBytesWritable tmpKmer;
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
        tmpKmer = new VKmerBytesWritable(KMER_SIZE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(VKmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputValue = values.next();
        if (values.hasNext() == true) {
            if (outputValue.getFlag() != 1) {
                byte nextAdj = outputValue.getAdjBitMap();
                byte succeed = (byte) 0x0F;
                succeed = (byte) (succeed & nextAdj);

                outputValue = values.next();
                byte adjBitMap = outputValue.getAdjBitMap();
                byte flag = outputValue.getFlag();
                if (outputValue.getKmerLength() != 0)
                    outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), key));
                else
                    outputKmer.set(key);

                adjBitMap = (byte) (adjBitMap & 0xF0);
                adjBitMap = (byte) (adjBitMap | succeed);
                outputValue.set(adjBitMap, flag, null);
                mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputValue);
            } else {
                tmpOutputValue.set(outputValue);
                byte tmpAdjMap = tmpOutputValue.getAdjBitMap();

                outputValue = values.next();
                if (outputValue.getFlag() != 1) {
                    if (tmpOutputValue.getKmerLength() != 0)
                        outputKmer.set(kmerFactory.mergeTwoKmer(tmpOutputValue.getKmer(), key));
                    else
                        outputKmer.set(key);

                    byte nextAdj = outputValue.getAdjBitMap();
                    byte succeed = (byte) 0x0F;
                    succeed = (byte) (succeed & nextAdj);
                    tmpAdjMap = (byte) (tmpAdjMap & 0xF0);
                    tmpAdjMap = (byte) (tmpAdjMap | succeed);
                    outputValue.set(tmpAdjMap, tmpOutputValue.getFlag(), null);
                    mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputValue);
                } else {

                    tmpKmer.set(kmerFactory.getFirstKmerFromChain(KMER_SIZE - 1, key));
                    if (tmpOutputValue.getKmerLength() != 0)
                        outputKmer.set(kmerFactory.mergeTwoKmer(tmpOutputValue.getKmer(), tmpKmer));
                    else
                        outputKmer.set(tmpKmer);
                    tmpOutputValue.set(tmpAdjMap, tmpOutputValue.getFlag(), null);
                    mos.getCollector("complete" + I_MERGE, reporter).collect(outputKmer, tmpOutputValue);

                    tmpKmer.set(kmerFactory.getFirstKmerFromChain(KMER_SIZE - 1, key));
                    if (outputValue.getKmerLength() != 0)
                        outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), tmpKmer));
                    else
                        outputKmer.set(tmpKmer);
                    outputValue.set(outputValue.getAdjBitMap(), outputValue.getFlag(), null);
                    mos.getCollector("complete" + I_MERGE, reporter).collect(outputKmer, outputValue);

                    while (values.hasNext()) {
                        outputValue = values.next();
                        tmpKmer.set(kmerFactory.getFirstKmerFromChain(KMER_SIZE - 1, key));
                        if (outputValue.getKmerLength() != 0)
                            outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), tmpKmer));
                        else
                            outputKmer.set(tmpKmer);
                        outputValue.set(outputValue.getAdjBitMap(), outputValue.getFlag(), null);
                        mos.getCollector("complete" + I_MERGE, reporter).collect(outputKmer, outputValue);
                    }
                }
            }
        } else {
            if (outputValue.getFlag() != 0) {
                tmpKmer.set(kmerFactory.getFirstKmerFromChain(KMER_SIZE - 1, key));
                if (outputValue.getKmerLength() != 0)
                    outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), tmpKmer));
                else
                    outputKmer.set(tmpKmer);
                outputValue.set(outputValue.getAdjBitMap(), outputValue.getFlag(), null);
                mos.getCollector("complete" + I_MERGE, reporter).collect(outputKmer, outputValue);

            } else
                mos.getCollector("uncomplete" + I_MERGE, reporter).collect(key, outputValue);
        }
    }

    public void close() throws IOException {
        // TODO Auto-generated method stub
        mos.close();
    }
}
