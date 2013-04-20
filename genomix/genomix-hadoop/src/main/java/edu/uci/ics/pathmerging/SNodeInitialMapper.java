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

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

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
        byte flag = (byte) 0;
        precursor = (byte) (precursor & adjBitMap);
        precursor = (byte) ((precursor & 0xff) >> 4);
        succeed = (byte) (succeed & adjBitMap);
        boolean inDegree = measureDegree(precursor);
        boolean outDegree = measureDegree(succeed);
        byte initial = 0;
        if (inDegree == true && outDegree == false) {
            flag = (byte) 2;
            switch (succeed) {
                case 1:
                    initial = (byte) 0x00;
                    break;
                case 2:
                    initial = (byte) 0x01;
                    break;
                case 4:
                    initial = (byte) 0x02;
                    break;
                case 8:
                    initial = (byte) 0x03;
                    break;
            }
            outputKmer.set(key);
            outputKmer.shiftKmerWithNextCode(initial);
            adjBitMap = (byte) (adjBitMap & 0xF0);
            outputAdjList.set(null, 0, 0, adjBitMap, flag, KMER_SIZE);
            output.collect(outputKmer, outputAdjList);
        }
        if (inDegree == false && outDegree == false) {
            outputKmer.set(key);
            outputAdjList.set(null, 0, 0, adjBitMap, flag, KMER_SIZE);
            output.collect(outputKmer, outputAdjList);
        }
    }
}
