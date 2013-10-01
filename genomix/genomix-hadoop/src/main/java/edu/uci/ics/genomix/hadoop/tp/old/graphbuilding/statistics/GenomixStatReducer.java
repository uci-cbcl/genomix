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
package edu.uci.ics.genomix.hadoop.tp.old.graphbuilding.statistics;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.old.type.nonreversed.*;
@SuppressWarnings("deprecation")
public class GenomixStatReducer extends MapReduceBase implements
        Reducer<BytesWritable, KmerCountValue, BytesWritable, KmerCountValue> {
    static enum MyCounters { NUM_RECORDS };
    KmerCountValue valWriter = new KmerCountValue();
    @Override
    public void reduce(BytesWritable key, Iterator<KmerCountValue> values,
            OutputCollector<BytesWritable, KmerCountValue> output, Reporter reporter) throws IOException {
        reporter.incrCounter(MyCounters.NUM_RECORDS, 1);
        valWriter = values.next();
        output.collect(key, valWriter); 
    }
}