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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

import edu.uci.ics.genomix.hadoop.tp.graphclean.io.GraphCleanNodeOperation;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageArrayWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.Message;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.MessageFlag;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.MessageListWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

@SuppressWarnings("deprecation")
public class ACleanReducer2 extends MapReduceBase implements
        Reducer<NodeWritable, MessageWritable, NodeWritable, MessageArrayWritable> {
    private NodeWritable outputNode;
    private MessageWritable message;
    private NullWritable nullWritable = NullWritable.get();
    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
    private MessageArrayWritable outMesList;
    MultipleOutputs mos = null;
    
    public void configure(JobConf job) {
        outputNode = new NodeWritable();
        message = new MessageWritable();
        outMesList = new MessageArrayWritable();
        mos = new MultipleOutputs(job);
    }
    
    @Override
    public void reduce(NodeWritable key, Iterator<MessageWritable> values,
            OutputCollector<NodeWritable, MessageArrayWritable> output, Reporter reporter) throws IOException {
        receivedMsgList.clear();
        outputNode.set(key);
        message.set(values.next());
        if(values.hasNext()) {
            do{
                if(message.getFlag() != Message.NON) {
                    receivedMsgList.add(message);
                }
                if(values.hasNext())
                    message.set(values.next());
                else
                    break;
            }while(true);
            outMesList.set(receivedMsgList.toArray(new MessageWritable[receivedMsgList.size()]));
            output.collect(key, outMesList);
            mos.getCollector("ToUse", reporter).collect(key, outMesList);
        }
        else {
            //TODO multi-output seperate the start node, middle node and unrelated node
            if(message.getFlag() == Message.NON) {
                if(message.getBubbleMark() == false) {
                    mos.getCollector("NotToUse", reporter).collect(key, nullWritable);
                }
                else
                    mos.getCollector("ToUse", reporter).collect(key, outMesList);
            }
            else {
                mos.getCollector("ToUse", reporter).collect(key, outMesList);
            }
        }
    }
}


