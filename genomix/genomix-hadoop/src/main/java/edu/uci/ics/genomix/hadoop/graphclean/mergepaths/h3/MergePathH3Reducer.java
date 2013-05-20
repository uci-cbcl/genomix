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
package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.hadoop.pmcommon.MergePathValueWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

@SuppressWarnings("deprecation")
public class MergePathH3Reducer extends MapReduceBase implements
        Reducer<VKmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {

    private int KMER_SIZE;
    MultipleOutputs mos = null;
    private int I_MERGE;

    private VKmerBytesWritableFactory kmerFactory;
    private MergePathValueWritable inputValue;
    private VKmerBytesWritable outputKmer;
    private MergePathValueWritable outputValue;

    private VKmerBytesWritable headKmer;
    private VKmerBytesWritable tailKmer;
    private byte outputAdjMap;

    public void configure(JobConf job) {
        mos = new MultipleOutputs(job);
        I_MERGE = Integer.parseInt(job.get("iMerge"));
        KMER_SIZE = job.getInt("sizeKmer", 0);

        inputValue = new MergePathValueWritable();
        headKmer = new VKmerBytesWritable(KMER_SIZE);
        tailKmer = new VKmerBytesWritable(KMER_SIZE);

        outputValue = new MergePathValueWritable();
        kmerFactory = new VKmerBytesWritableFactory(KMER_SIZE);
        outputKmer = new VKmerBytesWritable(KMER_SIZE);
    }

    public void reduce(VKmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {

        inputValue = values.next();
        if (!values.hasNext()) {
            // all single nodes must be remapped
            if (inputValue.getFlag() == State.FROM_SELF) {
                // FROM_SELF => remap self
                unmergedPathCollector(reporter).collect(key, inputValue);
            } else {
                // FROM_SUCCESSOR => remap successor
                outputKmer.set(outputValue.getKmer());
                outputValue.set(inputValue.getAdjBitMap(), State.EMPTY_MESSAGE, null);
                unmergedPathCollector(reporter).collect(outputKmer, outputValue);
            }
        } else {
            // multiple inputs => a merge will take place. Aggregate both, then collect the merged path
            outputAdjMap = (byte) 0;
            do {
                if (inputValue.getFlag() == State.FROM_SELF) {
                    // this is the head node.
                    outputAdjMap |= inputValue.getAdjBitMap() & 0xF0; // keep head's predecessor  
                    headKmer.set(inputValue.getKmer());
                } else {
                    // this is the tail node. 
                    outputAdjMap |= inputValue.getAdjBitMap() & 0x0F; // keep tail's successor
                    tailKmer.set(inputValue.getKmer());
                }
            } while (values.hasNext());
            outputKmer.set(kmerFactory.mergeTwoKmer(headKmer, tailKmer));
            outputValue.set(outputAdjMap, State.EMPTY_MESSAGE, null);
            mergedPathCollector(reporter).collect(outputKmer, outputValue);
        }
    }

    public void close() throws IOException {
        mos.close();
    }

    @SuppressWarnings("unchecked")
    public OutputCollector<VKmerBytesWritable, MergePathValueWritable> mergedPathCollector(Reporter reporter)
            throws IOException {
        return mos.getCollector("mergedSinglePath" + I_MERGE, reporter);
    }

    @SuppressWarnings("unchecked")
    public OutputCollector<VKmerBytesWritable, MergePathValueWritable> unmergedPathCollector(Reporter reporter)
            throws IOException {
        return mos.getCollector("unmergedSinglePath" + I_MERGE, reporter);
    }
}
