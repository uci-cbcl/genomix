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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

@SuppressWarnings("deprecation")
public class MergePathH1Mapper extends MapReduceBase implements
        Mapper<VKmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {
    private int KMER_SIZE;
    private VKmerBytesWritableFactory outputKmerFactory;
    private MergePathValueWritable outputValue;
    private VKmerBytesWritable tmpKmer;
    private VKmerBytesWritable outputKmer;

    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", 0);
        outputKmerFactory = new VKmerBytesWritableFactory(KMER_SIZE);
        outputValue = new MergePathValueWritable();
        tmpKmer = new VKmerBytesWritable(KMER_SIZE);
        outputKmer = new VKmerBytesWritable(KMER_SIZE);
    }

    @Override
    public void map(VKmerBytesWritable key, MergePathValueWritable value,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        byte precursor = (byte) 0xF0;
        byte succeed = (byte) 0x0F;
        byte adjBitMap = value.getAdjBitMap();
        byte bitFlag = value.getFlag();
        precursor = (byte) (precursor & adjBitMap);
        precursor = (byte) ((precursor & 0xff) >> 4);
        succeed = (byte) (succeed & adjBitMap);
        byte bitStartEnd = (byte) (0x01 & bitFlag);
        if (bitStartEnd == 1) {
            /**
             * eg. the kmer: AGCGT(already merge 3 kmers sizeof 3), adjMap C|G
             *     succeedCode -> G then tmpKmer store the succeding neighbor: GTG ->outputKmer
             *     then we store the AGC in the tmpKmer -> outputValue
             */
            byte succeedCode = GeneCode.getGeneCodeFromBitMap(succeed);
            tmpKmer.set(outputKmerFactory.getLastKmerFromChain(KMER_SIZE, key));
            outputKmer.set(outputKmerFactory.shiftKmerWithNextCode(tmpKmer, succeedCode));
            tmpKmer.set(outputKmerFactory.getFirstKmerFromChain(key.getKmerLength() - (KMER_SIZE - 1), key));
            outputValue.set(adjBitMap, bitFlag, tmpKmer);
            output.collect(outputKmer, outputValue);
        } else {
            output.collect(key, value);
        }
    }
}
