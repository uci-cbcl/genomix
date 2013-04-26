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
package edu.uci.ics.genomix.pathmergingh2;

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
        byte startFlag = 0x00;
        byte endFlag = 0x00;
        byte targetPointFlag = 0x00;
        byte targetAdjList = 0x00;
        byte outputFlag = 0x00;
        if (values.hasNext() == true) {
            switch (outputValue.getFlag()) {
                case (byte) 0x01:
                    startFlag = (byte) 0x01;
                    break;
                case (byte) 0x80:
                    endFlag = (byte) 0x80;
                    break;
                case (byte) 0x02:
                    targetPointFlag = (byte) 0x02;
                    targetAdjList = outputValue.getAdjBitMap();
                    break;
            }
            while (values.hasNext()) {
                outputValue = values.next();
                switch (outputValue.getFlag()) {
                    case (byte) 0x01:
                        startFlag = (byte) 0x01;
                        break;
                    case (byte) 0x80:
                        endFlag = (byte) 0x80;
                        break;
                    case (byte) 0x02:
                        targetPointFlag = (byte) 0x02;
                        targetAdjList = outputValue.getAdjBitMap();
                        break;
                }
                if(startFlag != (byte) 0x00 && endFlag!= (byte) 0x00 && targetPointFlag != (byte) 0x00)
                    break;
            }
            if(targetPointFlag == (byte) 0x02) {
                if(startFlag == (byte) 0x01) {
                    outputFlag = (byte) (outputFlag | startFlag);
                }
                if(endFlag == (byte) 0x80) {
                    outputFlag = (byte) (outputFlag | endFlag);
                }
                outputValue.set(targetAdjList, outputFlag, null);
                output.collect(outputKmer, outputValue);
            }
        } else {
            if (outputValue.getFlag() == 2) {
                byte bitFlag = 0;
                outputValue.set(outputValue.getAdjBitMap(), bitFlag, null);
                output.collect(outputKmer, outputValue);
            }
        }
    }
}
