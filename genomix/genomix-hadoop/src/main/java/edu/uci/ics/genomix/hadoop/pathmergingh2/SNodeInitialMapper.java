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
package edu.uci.ics.genomix.hadoop.pathmergingh2;

import java.io.IOException;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.MergePathValueWritable;

@SuppressWarnings("deprecation")
public class SNodeInitialMapper extends MapReduceBase implements
        Mapper<KmerBytesWritable, ByteWritable, KmerBytesWritable, MergePathValueWritable> {

    private int KMER_SIZE;
    private KmerBytesWritable outputKmer;
    private MergePathValueWritable outputAdjList;

    public void configure(JobConf job) {
        KMER_SIZE = Integer.parseInt(job.get("sizeKmer"));
        outputKmer = new KmerBytesWritable(KMER_SIZE);
        outputAdjList = new MergePathValueWritable();
    }

    /**
     * @param adjacent the high 4 bits are useless, we just use the lower 4 bits
     * @return if the degree == 1 then return false, else return true
     */
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
        //if indegree == 1 and outdegree == 1, then it assigns these records' flag to 2
        if (inDegree == false && outDegree == false) {
            outputKmer.set(key);
            bitFlag = (byte) 0x02;
            outputAdjList.set(adjBitMap, bitFlag, null);
            output.collect(outputKmer, outputAdjList);
        } else {
            // other records maps its precursor neighbors
            /**
             * eg. ACT  CTA|CA, it maps CAC, TAC, ACA, all the 3 pairs marked  0x80
             */
            for (int i = 0; i < 4; i++) {
                byte temp = (byte) 0x01;
                byte shiftedCode = 0;
                temp = (byte) (temp << i);
                temp = (byte) (precursor & temp);
                if (temp != 0) {
                    byte precurCode = GeneCode.getGeneCodeFromBitMap(temp);
                    shiftedCode = key.shiftKmerWithPreCode(precurCode);
                    outputKmer.set(key);
                    bitFlag = (byte) 0x80;
                    outputAdjList.set((byte) 0, bitFlag, null);
                    output.collect(outputKmer, outputAdjList);
                    key.shiftKmerWithNextCode(shiftedCode);
                }
            }
            //and also maps its succeeding neighbors
            /**
             * eg. kmer:ACT  bitMap: CTA|CA, it maps CTC, CTA, all the 2 pairs marked 0x01
             */
            for (int i = 0; i < 4; i++) {
                byte temp = (byte) 0x01;
                byte shiftedCode = 0;
                temp = (byte) (temp << i);
                temp = (byte) (succeed & temp);
                if (temp != 0) {
                    byte succeedCode = GeneCode.getGeneCodeFromBitMap(temp);
                    shiftedCode = key.shiftKmerWithNextCode(succeedCode);
                    outputKmer.set(key);
                    bitFlag = (byte) 0x01;
                    outputAdjList.set((byte) 0, bitFlag, null);
                    output.collect(outputKmer, outputAdjList);
                    key.shiftKmerWithPreCode(shiftedCode);
                }
            }
        }
    }
}
