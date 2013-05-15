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
package edu.uci.ics.genomix.hadoop.pathmergingh1;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;
import edu.uci.ics.genomix.type.MergePathValueWritable;

@SuppressWarnings("deprecation")
public class MergePathH1Reducer extends MapReduceBase implements
        Reducer<VKmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {
    private VKmerBytesWritableFactory kmerFactory;
    private VKmerBytesWritable outputKmer;
    private int KMER_SIZE;
    private MergePathValueWritable outputValue;
    MultipleOutputs mos = null;
    private int I_MERGE;

    public void configure(JobConf job) {
        mos = new MultipleOutputs(job);
        I_MERGE = Integer.parseInt(job.get("iMerge"));
        KMER_SIZE = job.getInt("sizeKmer", 0);
        outputValue = new MergePathValueWritable();
        kmerFactory = new VKmerBytesWritableFactory(KMER_SIZE);
        outputKmer = new VKmerBytesWritable(KMER_SIZE);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void reduce(VKmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputValue = values.next();
        if (values.hasNext() == true) {
            byte bitFlag = outputValue.getFlag();
            byte bitStartEnd = (byte) (0x01 & outputValue.getFlag());
            if (bitStartEnd == 0) {
                /**
                 * eg. if 2 records go into same group, the first is start-point: (GTG, null, A|T, 0) the second is: (GTG, AGC, C|G, 1)
                 *     the results of combing: AGCGTG, null, C|T, 1
                 */
                //first record is non-start point
               
                byte nextAdj = outputValue.getAdjBitMap();
                byte succeed = (byte) 0x0F;
                succeed = (byte) (succeed & nextAdj);
                //second record must be start point
                outputValue = values.next();
                byte adjBitMap = outputValue.getAdjBitMap();
                if (outputValue.getKmerLength() != 0)
                    outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), key));
                else
                    outputKmer.set(key);
                byte outputFlag = (byte) (0x81 & bitFlag);
                outputFlag = (byte) (outputFlag | ((byte) 0x81 & outputValue.getFlag()));
                adjBitMap = (byte) (adjBitMap & 0xF0);
                adjBitMap = (byte) (adjBitMap | succeed);
                outputValue.set(adjBitMap, outputFlag, null);               
                //judge whether the node after merging has contain the start-point and end-point
                bitFlag = outputValue.getFlag();
                bitStartEnd = (byte) (0x81 & bitFlag);
                if (bitStartEnd == (byte) 0x81) {
                    mos.getCollector("comSinglePath" + I_MERGE, reporter).collect(outputKmer, outputValue);
                } else
                    mos.getCollector("uncompSinglePath" + I_MERGE, reporter).collect(outputKmer, outputValue);
            } else {
                /**
                 * eg. if 2 records go into same group, the first is start-point:(GTG, AGC, C|G, 1)  the second is: (GTG, null, A|T, 0)
                 *     the results of combing: AGCGTG, null, C|T, 1
                 */
                //first record is start point
                byte adjBitMap = outputValue.getAdjBitMap();
                if (outputValue.getKmerLength() != 0)
                    outputKmer.set(kmerFactory.mergeTwoKmer(outputValue.getKmer(), key));
                else
                    outputKmer.set(key);
                //second record is non start point
                outputValue = values.next();
                byte nextAdj = outputValue.getAdjBitMap();
                byte succeed = (byte) 0x0F;
                succeed = (byte) (succeed & nextAdj);
                //set outputFlag for first record
                byte outputFlag = (byte) (0x81 & bitFlag);
                outputFlag = (byte) (outputFlag | ((byte) 0x81 & outputValue.getFlag()));
                adjBitMap = (byte) (adjBitMap & 0xF0);
                adjBitMap = (byte) (adjBitMap | succeed);
                outputValue.set(adjBitMap, outputFlag, null);
                //judge whether the node after merging has contain the start-point and end-point
                bitFlag = outputValue.getFlag();
                bitStartEnd = (byte) (0x81 & bitFlag);
                if (bitStartEnd == (byte) 0x81) {
                    mos.getCollector("comSinglePath" + I_MERGE, reporter).collect(outputKmer, outputValue);
                } else
                    mos.getCollector("uncompSinglePath" + I_MERGE, reporter).collect(outputKmer, outputValue);
            }
        } else {
            mos.getCollector("uncompSinglePath" + I_MERGE, reporter).collect(key, outputValue);
        }
    }

    public void close() throws IOException {
        // TODO Auto-generated method stub
        mos.close();
    }
}
