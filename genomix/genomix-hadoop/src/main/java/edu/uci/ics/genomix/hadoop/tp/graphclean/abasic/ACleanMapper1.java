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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.io.GraphCleanNodeOperation;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.Message;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class ACleanMapper1 extends MapReduceBase implements
        Mapper<NodeWritable, NullWritable, NodeWritable, MessageWritable> {

    private NodeWritable desKeyNode;
    private MessageWritable message;
    private GraphCleanNodeOperation gcCleanner;

    public void configure(JobConf job) {
        desKeyNode = new NodeWritable();
        message = new MessageWritable();
    }

    @Override
    public void map(NodeWritable key, NullWritable value, OutputCollector<NodeWritable, MessageWritable> output,
            Reporter reporter) throws IOException {
        desKeyNode.reset();
        message.reset();
        if(VertexUtil.isHeadVertexWithIndegree(key)    // TODO also still confused with this function
                || VertexUtil.isHeadWithoutIndegree(key)){ //TODO still confused with this function
            gcCleanner.sendSettledMsgToAllNextNodes(key, message, desKeyNode, output);
            message.reset();
            message.setFlag(Message.START); // Mark the major node, and then filter it!
            output.collect(key, message);
        }
        else {
            output.collect(key, message); // initial to Message.NON
        }
    }
}
