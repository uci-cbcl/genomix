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
package edu.uci.ics.genomix.hadoop.gbresultschecking;

import java.io.IOException;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerCountValue;

@SuppressWarnings({ "unused", "deprecation" })
public class ResultsCheckingMapper extends MapReduceBase implements Mapper<KmerBytesWritable, KmerCountValue, Text, Text> {
    KmerBytesWritable valWriter;
    private final static IntWritable one = new IntWritable(1);
    public static Text textkey = new Text();
    public static Text textvalue = new Text();
    public static String INPUT_PATH;
    public static int KMER_SIZE;

    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", 0);
        valWriter= new KmerBytesWritable(KMER_SIZE);
    }

    @Override
    public void map(KmerBytesWritable key, KmerCountValue value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String filename = fileSplit.getPath().getName();
        textkey.set(key.toString() + "\t" + value.toString());
        textvalue.set(filename);
        output.collect(textkey, textvalue);
    }
}
