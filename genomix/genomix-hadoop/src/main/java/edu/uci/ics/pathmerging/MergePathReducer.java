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
package edu.uci.ics.pathmerging;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

@SuppressWarnings("deprecation")
public class MergePathReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, MergePathValueWritable, KmerBytesWritable, MergePathValueWritable> {
    private VKmerBytesWritableFactory kmerFactory;
    private VKmerBytesWritable outputKmer;
    private VKmerBytesWritable tmpKmer;
    private int KMER_SIZE;
    private MergePathValueWritable outputAdjList;
    MultipleOutputs mos = null;
    private int I_MERGE;

    public void configure(JobConf job) {
        mos = new MultipleOutputs(job);
        I_MERGE = Integer.parseInt(job.get("iMerge"));
        KMER_SIZE = job.getInt("sizeKmer", 0);
        outputAdjList = new MergePathValueWritable();
        kmerFactory = new VKmerBytesWritableFactory(KMER_SIZE);
        outputKmer = new VKmerBytesWritable(KMER_SIZE);
        tmpKmer = new VKmerBytesWritable(KMER_SIZE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(KmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<KmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputAdjList = values.next();

        
        if (values.hasNext() == true) {

            if (outputAdjList.getFlag() == 1) {
                byte adjBitMap = outputAdjList.getAdjBitMap();
                byte bitFlag = outputAdjList.getFlag();
                outputKmer.set(kmerFactory.mergeTwoKmer(outputAdjList.getKmer(), key));
                
                outputAdjList = values.next();
                byte nextAdj = outputAdjList.getAdjBitMap();
                byte succeed = (byte) 0x0F;
                succeed = (byte) (succeed & nextAdj);
                adjBitMap = (byte) (adjBitMap & 0xF0);
                adjBitMap = (byte) (adjBitMap | succeed);
                outputAdjList.set(null, 0, 0, adjBitMap, bitFlag, KMER_SIZE + outputAdjList.getKmerSize());

                mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputAdjList);
            } else {
                byte nextAdj = outputAdjList.getAdjBitMap();
                byte succeed = (byte) 0x0F;
                succeed = (byte) (succeed & nextAdj);
                outputAdjList = values.next();
                byte adjBitMap = outputAdjList.getAdjBitMap();
                byte flag = outputAdjList.getFlag();
                int kmerSize = outputAdjList.getKmerSize();
                
                outputKmer.set(kmerFactory.mergeTwoKmer(outputAdjList.getKmer(), key));
                adjBitMap = (byte) (adjBitMap & 0xF0);
                adjBitMap = (byte) (adjBitMap | succeed);
                outputAdjList.set(null, 0, 0, adjBitMap, flag, KMER_SIZE + kmerSize);

                mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputAdjList);
            }
        } else {
            if (outputAdjList.getFlag() != 0) {
                byte adjBitMap = outputAdjList.getAdjBitMap();
                byte flag = outputAdjList.getFlag();
                int kmerSize = outputAdjList.getKmerSize();
                
                tmpKmer.set(kmerFactory.getFirstKmerFromChain(KMER_SIZE - 1, key));
                outputKmer.set(kmerFactory.mergeTwoKmer(outputAdjList.getKmer(), tmpKmer));
                outputAdjList.set(null, 0, 0, adjBitMap, flag, KMER_SIZE + kmerSize);
                mos.getCollector("complete" + I_MERGE, reporter).collect(outputKmer, outputAdjList);
            } else
                mos.getCollector("uncomplete" + I_MERGE, reporter).collect(key, outputAdjList);
        }
    }

    public void close() throws IOException {
        // TODO Auto-generated method stub
        mos.close();
    }
}
