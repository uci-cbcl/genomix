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
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.io.GraphCleanNodeOperation;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.Message;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class ACleanMapper2 extends MapReduceBase implements
        Mapper<NodeWritable, MessageWritable, NodeWritable, MessageWritable> {

    private NodeWritable desKeyNode;
    private MessageWritable message;
    private MessageWritable middleMes;
    private GraphCleanNodeOperation gcCleanner;

    public void configure(JobConf job) {
        desKeyNode = new NodeWritable();
        message = new MessageWritable();
        middleMes = new MessageWritable();
    }

    @Override
    public void map(NodeWritable key, MessageWritable value, OutputCollector<NodeWritable, MessageWritable> output,
            Reporter reporter) throws IOException {
        if(value.getFlag() != Message.NON && value.getFlag() != Message.START) {
            desKeyNode.reset();
            message.reset();
            gcCleanner.sendBubbleAndMajorVertexMsgToMinorVertex(key, value, desKeyNode, message);
            output.collect(desKeyNode, message);
            output.collect(key, middleMes); // middle path node, but here not clear, I think can change the flag
        }
        else {
            desKeyNode.set(key);
            message.set(value);
            output.collect(desKeyNode, message);
        }
    }
}
