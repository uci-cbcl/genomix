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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;

@SuppressWarnings("deprecation")
public class MergePathReducer extends MapReduceBase implements
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

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(BytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<BytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputAdjList = values.next();

        if (values.hasNext() == true) {

            byte[] keyBytes = new byte[key.getLength()];
            for (int i = 0; i < keyBytes.length; i++) {
                keyBytes[i] = key.getBytes()[i];
            }
            if (outputAdjList.getFlag() == 1) {
                byte adjBitMap = outputAdjList.getAdjBitMap();
                byte bitFlag = outputAdjList.getFlag();
                int kmerSize = outputAdjList.getKmerSize();
                int mergeByteNum = Kmer.getByteNumFromK(KMER_SIZE + kmerSize);
                byte[] valueBytes = new byte[outputAdjList.getLength()];
                for (int i = 0; i < valueBytes.length; i++) {
                    valueBytes[i] = outputAdjList.getBytes()[i];
                }
                byte[] mergeKmer = KmerUtil.mergeTwoKmer(outputAdjList.getKmerSize(), valueBytes, KMER_SIZE, keyBytes);
                outputKmer.set(mergeKmer, 0, mergeByteNum);

                outputAdjList = values.next();
                byte nextAdj = outputAdjList.getAdjBitMap();
                byte succeed = (byte) 0x0F;
                succeed = (byte) (succeed & nextAdj);
                adjBitMap = (byte) (adjBitMap & 0xF0);
                adjBitMap = (byte) (adjBitMap | succeed);
                outputAdjList.set(null, 0, 0, adjBitMap, bitFlag, KMER_SIZE + kmerSize);

                mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputAdjList);
            } else {
                byte nextAdj = outputAdjList.getAdjBitMap();
                byte succeed = (byte) 0x0F;
                succeed = (byte) (succeed & nextAdj);
                outputAdjList = values.next();
                byte adjBitMap = outputAdjList.getAdjBitMap();
                byte flag = outputAdjList.getFlag();
                int kmerSize = outputAdjList.getKmerSize();
                int mergeByteNum = Kmer.getByteNumFromK(KMER_SIZE + kmerSize);
                byte[] valueBytes = new byte[outputAdjList.getLength()];
                for (int i = 0; i < valueBytes.length; i++) {
                    valueBytes[i] = outputAdjList.getBytes()[i];
                }
                byte[] mergeKmer = KmerUtil.mergeTwoKmer(outputAdjList.getKmerSize(), valueBytes, KMER_SIZE, keyBytes);
                outputKmer.set(mergeKmer, 0, mergeByteNum);

                adjBitMap = (byte) (adjBitMap & 0xF0);
                adjBitMap = (byte) (adjBitMap | succeed);
                outputAdjList.set(null, 0, 0, adjBitMap, flag, KMER_SIZE + kmerSize);

                mos.getCollector("uncomplete" + I_MERGE, reporter).collect(outputKmer, outputAdjList);
            }
        } else {
            byte[] keyBytes = new byte[key.getLength()];
            for (int i = 0; i < keyBytes.length; i++) {
                keyBytes[i] = key.getBytes()[i];
            }
            if (outputAdjList.getFlag() != 0) {
                byte adjBitMap = outputAdjList.getAdjBitMap();
                byte flag = outputAdjList.getFlag();
                int kmerSize = outputAdjList.getKmerSize();
                int mergeByteNum = Kmer.getByteNumFromK(KMER_SIZE - 1 + kmerSize);
                byte[] tmpKmer = KmerUtil.getFirstKmerFromChain(KMER_SIZE - 1, KMER_SIZE, keyBytes);
                byte[] valueBytes = new byte[outputAdjList.getLength()];
                for (int i = 0; i < valueBytes.length; i++) {
                    valueBytes[i] = outputAdjList.getBytes()[i];
                }
                byte[] mergeKmer = KmerUtil.mergeTwoKmer(outputAdjList.getKmerSize(), valueBytes, KMER_SIZE - 1,
                        tmpKmer);
                outputKmer.set(mergeKmer, 0, mergeByteNum);
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
