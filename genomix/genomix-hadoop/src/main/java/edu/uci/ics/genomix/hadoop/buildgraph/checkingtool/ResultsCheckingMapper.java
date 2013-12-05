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

package edu.uci.ics.genomix.hadoop.buildgraph.checkingtool;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.data.types.Node;
import edu.uci.ics.genomix.data.types.VKmer;

@SuppressWarnings("deprecation")
public class ResultsCheckingMapper extends MapReduceBase implements Mapper<VKmer, Node, Text, Text> {
    public static Text textkey = new Text();
    public static Text textvalue = new Text();

    @Override
    public void map(VKmer key, Node value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
        String filename = fileSplit.getPath().getName();
        textkey.set(key.toString() + "\t" + value.toString());
        textvalue.set(filename);
        output.collect(textkey, textvalue);
    }
}