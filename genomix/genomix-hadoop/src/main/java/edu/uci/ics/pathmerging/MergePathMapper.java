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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.Kmer;
import edu.uci.ics.genomix.type.KmerUtil;
import edu.uci.ics.genomix.type.Kmer.GENE_CODE;

@SuppressWarnings("deprecation")
public class MergePathMapper extends MapReduceBase implements
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

        byte precursor = (byte) 0xF0;
        byte succeed = (byte) 0x0F;
        byte adjBitMap = value.getAdjBitMap();
        byte bitFlag = value.getFlag();
        precursor = (byte) (precursor & adjBitMap);
        precursor = (byte) ((precursor & 0xff) >> 4);
        succeed = (byte) (succeed & adjBitMap);

        byte[] kmerValue = key.getBytes();
        int kmerLength = key.getLength();
        if (bitFlag == 1) {
            byte succeedCode = GENE_CODE.getGeneCodeFromBitMap(succeed);
            int originalByteNum = Kmer.getByteNumFromK(KMER_SIZE);
            byte[] tmpKmer = KmerUtil.getLastKmerFromChain(KMER_SIZE, value.getKmerSize(), kmerValue, 0, kmerLength);
            byte[] newKmer = KmerUtil.shiftKmerWithNextCode(KMER_SIZE, tmpKmer,0, tmpKmer.length, succeedCode);
            outputKmer.set(newKmer, 0, originalByteNum);

            int mergeByteNum = Kmer.getByteNumFromK(value.getKmerSize() - (KMER_SIZE - 1));
            byte[] mergeKmer = KmerUtil.getFirstKmerFromChain(value.getKmerSize() - (KMER_SIZE - 1),
                    value.getKmerSize(), kmerValue, 0, kmerLength);
            outputAdjList.set(mergeKmer, 0, mergeByteNum, adjBitMap, bitFlag, value.getKmerSize() - (KMER_SIZE - 1));
            output.collect(outputKmer, outputAdjList);
        } else {
            outputKmer.set(key);
            outputAdjList.set(value);
            output.collect(outputKmer, outputAdjList);
        }
    }
}
