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
import edu.uci.ics.genomix.type.VKmerBytesWritable;

@SuppressWarnings("deprecation")
public class SNodeInitialReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {
    private VKmerBytesWritable outputKmer = new VKmerBytesWritable();
    private MergePathValueWritable outputValue = new MergePathValueWritable();

    @Override
    public void reduce(KmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputKmer.set(key);
        outputValue = values.next();
        if (values.hasNext() == true) {
            if (outputValue.getFlag() == 2) {
                byte bitFlag = 1;
                outputValue.set(null, 0, 0, outputValue.getAdjBitMap(), bitFlag, outputValue.getKmerLength());///outputValue.getKmerLength()
                output.collect(outputKmer, outputValue);
            } else {
                boolean flag = false;
                while (values.hasNext()) {
                    outputValue = values.next();
                    if (outputValue.getFlag() == 2) {
                        flag = true;
                        break;
                    }
                }
                if (flag == true) {
                    byte bitFlag = 1;
                    outputValue.set(null, 0, 0, outputValue.getAdjBitMap(), bitFlag, outputValue.getKmerLength());
                    output.collect(outputKmer, outputValue);
                }
            }
        } else {
            if (outputValue.getFlag() == 2) {
                byte bitFlag = 0;
                outputValue.set(null, 0, 0, outputValue.getAdjBitMap(), bitFlag, outputValue.getKmerLength());
                output.collect(outputKmer, outputValue);
            }
        }
    }
}
