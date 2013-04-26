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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.KmerUtil;

@SuppressWarnings("deprecation")
public class ENodeInitialReducer extends MapReduceBase implements
        Reducer<BytesWritable, MergePathValueWritable, BytesWritable, MergePathValueWritable> {
    public BytesWritable outputKmer = new BytesWritable();
    public MergePathValueWritable outputAdjList = new MergePathValueWritable();
    
    @Override
    public void reduce(BytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<BytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputAdjList = values.next();
        outputKmer.set(key);
        if (values.hasNext() == true) {
            byte bitFlag = outputAdjList.getFlag();
            bitFlag = (byte) (bitFlag & 0xFE);
            if (bitFlag == 2) {
                bitFlag =  (byte) (0x80 | outputAdjList.getFlag());
                outputAdjList.set(outputAdjList.getAdjBitMap(), bitFlag, null);
                output.collect(outputKmer, outputAdjList);

            } else {
                boolean flag = false;
                while (values.hasNext()) {
                    outputAdjList = values.next();
                    if (outputAdjList.getFlag() == 2) {
                        flag = true;
                        break;
                    }
                }
                if (flag == true) {
                    bitFlag =  (byte) (0x80 | outputAdjList.getFlag());
                    outputAdjList.set(outputAdjList.getAdjBitMap(), bitFlag, null);
                    output.collect(outputKmer, outputAdjList);
                }
            }
        } else {
            byte bitFlag = outputAdjList.getFlag();
            bitFlag = (byte) (bitFlag & 0xFE);
            if (bitFlag == 2) {
                bitFlag = 0;
                outputAdjList.set(outputAdjList.getAdjBitMap(), bitFlag, null);
                output.collect(outputKmer, outputAdjList);
            } 
        }
    }
}

