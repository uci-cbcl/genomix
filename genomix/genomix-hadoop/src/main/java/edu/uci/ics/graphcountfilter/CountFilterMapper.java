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
package edu.uci.ics.graphcountfilter;

import java.io.IOException;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;


@SuppressWarnings({ "unused", "deprecation" })
public class CountFilterMapper extends MapReduceBase implements
        Mapper<BytesWritable, KmerCountValue, BytesWritable, ByteWritable> {
    public static int THRESHOLD;
    public BytesWritable outputKmer = new BytesWritable();
    public ByteWritable outputAdjList = new ByteWritable();
    @Override
    public void configure(JobConf job) {
        THRESHOLD = Integer.parseInt(job.get("countThreshold"));
    }
    public void map(BytesWritable key, KmerCountValue value, OutputCollector<BytesWritable, ByteWritable> output,
            Reporter reporter) throws IOException {
        if(value.getCount() >= THRESHOLD){
            outputKmer.set(key);
            outputAdjList.set(value.getAdjBitMap());
            output.collect(outputKmer, outputAdjList);
        }
    }
}
