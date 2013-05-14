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
import java.util.Iterator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.MergePathValueWritable;

@SuppressWarnings("deprecation")
public class SNodeInitialReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, MergePathValueWritable, VKmerBytesWritable, MergePathValueWritable> {
    private VKmerBytesWritable outputKmer = new VKmerBytesWritable();
    private MergePathValueWritable outputValue = new MergePathValueWritable();
    MultipleOutputs mos = null;
    public void configure(JobConf job) {
        mos = new MultipleOutputs(job);
    }
    @SuppressWarnings("unchecked")
    @Override
    public void reduce(KmerBytesWritable key, Iterator<MergePathValueWritable> values,
            OutputCollector<VKmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {
        outputKmer.set(key);
        outputValue = values.next();
        byte startPointFlag = 0x00;
        byte endPointFlag = 0x00;
        /**
         * the targetPoint means that we want find the record which 1 indegree and 1 outdegree in the group which has multi-records
         */
        byte targetPointFlag = 0x00;
        byte targetAdjList = 0x00;
        //if we find the start or end point, we will use outputFlag to mark them
        byte outputFlag = 0x00;
        if (values.hasNext() == true) {
            //find startPointFlag, endPointFlag, targetPointFlag
            switch (outputValue.getFlag()) {
                case (byte) 0x01:
                    startPointFlag = (byte) 0x01;
                    break;
                case (byte) 0x80:
                    endPointFlag = (byte) 0x80;
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
                        startPointFlag = (byte) 0x01;
                        break;
                    case (byte) 0x80:
                        endPointFlag = (byte) 0x80;
                        break;
                    case (byte) 0x02:
                        targetPointFlag = (byte) 0x02;
                        targetAdjList = outputValue.getAdjBitMap();
                        break;
                }
                if(startPointFlag != (byte) 0x00 && endPointFlag!= (byte) 0x00 && targetPointFlag != (byte) 0x00)
                    break;
            }
            //find the start-point or end-point
            if(targetPointFlag == (byte) 0x02) {
                //remove the single point path
                if(startPointFlag == (byte) 0x01 && endPointFlag == (byte) 0x80) {
                    outputFlag = (byte) (outputFlag | startPointFlag);
                    outputFlag = (byte) (outputFlag | endPointFlag);
                    outputValue.set(targetAdjList, outputFlag, null);
                    mos.getCollector("comSinglePath0", reporter).collect(outputKmer, outputValue);
                }
                else {
                    if(startPointFlag == (byte) 0x01) {
                        outputFlag = (byte) (outputFlag | startPointFlag);
                    }
                    if(endPointFlag == (byte) 0x80) {
                        outputFlag = (byte) (outputFlag | endPointFlag);
                    }
                    outputValue.set(targetAdjList, outputFlag, null);
                    output.collect(outputKmer, outputValue);
                }
            }
        } else {
            //keep the non-start/end single point into the input files
            if (outputValue.getFlag() == (byte)0x02) {
                byte bitFlag = 0;
                outputValue.set(outputValue.getAdjBitMap(), bitFlag, null);
                output.collect(outputKmer, outputValue);
            }
        }
    }
    public void close() throws IOException {
        // TODO Auto-generated method stub
        mos.close();
    }
}
