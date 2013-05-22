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
package edu.uci.ics.genomix.hadoop.graphcountfilter;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.oldtype.*;
@SuppressWarnings("deprecation")
public class CountFilterReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, ByteWritable, KmerBytesWritable, ByteWritable> {
    @Override
    public void reduce(KmerBytesWritable key, Iterator<ByteWritable> values,
            OutputCollector<KmerBytesWritable, ByteWritable> output, Reporter reporter) throws IOException {
        output.collect(key, values.next()); //Output the Pair
    }
}
