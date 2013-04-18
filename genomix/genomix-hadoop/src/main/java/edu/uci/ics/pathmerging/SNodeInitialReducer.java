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

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.KmerBytesWritable;

@SuppressWarnings("deprecation")
public class SNodeInitialReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, MergePathValueWritable, KmerBytesWritable, MergePathValueWritable> {
    private MergePathValueWritable outputAdjList = new MergePathValueWritable();

    @Override
    public void reduce(KmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<KmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputAdjList = values.next();
        if (values.hasNext() == true) {
            if (outputAdjList.getFlag() != 2) {
                byte adjBitMap = outputAdjList.getAdjBitMap();
                int kmerSize = outputAdjList.getKmerSize();
                byte bitFlag = 1;
                outputAdjList.set(null, 0, 0, adjBitMap, bitFlag, kmerSize);
                output.collect(key, outputAdjList);

            } else {
                boolean flag = false;
                while (values.hasNext()) {
                    outputAdjList = values.next();
                    if (outputAdjList.getFlag() != 2) {
                        flag = true;
                        break;
                    }
                }
                if (flag == true) {
                    byte adjBitMap = outputAdjList.getAdjBitMap();
                    int kmerSize = outputAdjList.getKmerSize();
                    byte bitFlag = 1;
                    outputAdjList.set(null, 0, 0, adjBitMap, bitFlag, kmerSize);
                    output.collect(key, outputAdjList);
                }
            }
        } else {
            output.collect(key, outputAdjList);
        }
    }
}
