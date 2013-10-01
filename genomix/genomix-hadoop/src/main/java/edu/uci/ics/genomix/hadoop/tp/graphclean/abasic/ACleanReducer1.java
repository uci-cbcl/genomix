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
package edu.uci.ics.genomix.hadoop.tp.graphclean.abasic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.io.GraphCleanNodeOperation;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.Message;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.MessageFlag;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class ACleanReducer1 extends MapReduceBase implements
        Reducer<NodeWritable, MessageWritable, NodeWritable, MessageWritable> {
    private NodeWritable outputNode;
    private MessageWritable message;
//    private GraphCleanNodeOperation gcCleanner;
//    private NullWritable nullWritable = NullWritable.get();
    
    public void configure(JobConf job) {
        outputNode = new NodeWritable();
        message = new MessageWritable();
//        gcCleanner = new GraphCleanNodeOperation();
    }
    
    @Override
    public void reduce(NodeWritable key, Iterator<MessageWritable> values,
            OutputCollector<NodeWritable, MessageWritable> output, Reporter reporter) throws IOException {
        outputNode.set(key);
        message.set(values.next());
        if(values.hasNext()) {
            if(VertexUtil.isPathVertex(outputNode)) { // filter the path vertex
                if(message.getFlag() != Message.NON) {
                    output.collect(outputNode, message);
                }
                else{
                    message.set(values.next());
                    output.collect(outputNode, message);
                }

            }
            else {
                if(message.getFlag() == Message.START) { // filter the major vertex
                    output.collect(outputNode, message);
                }
                else {
                    while(values.hasNext()) {
                        message.set(values.next());
                        if(message.getFlag() == Message.START) {
                            output.collect(outputNode, message);
                        }
                        if(message.getFlag() == Message.NON) {
                            output.collect(outputNode, message);
                        }
                    }
                }
            }
        }
        else {
            output.collect(outputNode, message); // other nodes output directly
        }
    }
}


