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
package edu.uci.ics.genomix.hadoop.tp.graphclean.qbasic;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.io.GraphCleanNodeOperation;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable;

@SuppressWarnings("deprecation")
public class QCleanMapper extends MapReduceBase implements
        Mapper<NodeWritable, NullWritable, NodeWritable, MessageWritable> {

    private NodeWritable desKeyNode;
    private MessageWritable message;
    private GraphCleanNodeOperation gcCleanner;
    private int threshold;

    public void configure(JobConf job) {
        threshold = job.getInt("threshold", 0);
        desKeyNode = new NodeWritable();
        message = new MessageWritable();
    }

    @Override
    public void map(NodeWritable key, NullWritable value, OutputCollector<NodeWritable, MessageWritable> output,
            Reporter reporter) throws IOException {
        desKeyNode.reset();
        message.reset();
        if (VertexUtil.isIncomingTipVertex(key)) {
            if (key.getKmer().getKmerLetterLength() <= threshold) {
                gcCleanner.sendSettledMsgToPreviousNode(key, message, desKeyNode);// Previsous Node ? inproper name
                output.collect(desKeyNode, message);
            }
            output.collect(key, message);
        } else if (VertexUtil.isOutgoingTipVertex(key)) {
            if (key.getKmer().getKmerLetterLength() <= threshold) {
                gcCleanner.sendSettledMsgToNextNode(key, message, desKeyNode); // Next Node ? inproper name
                output.collect(desKeyNode, message);
            }
            output.collect(key, message);
        } else if (VertexUtil.isSingleVertex(key)) {
            if (key.getKmer().getKmerLetterLength() > threshold)
                output.collect(key, message);
        } else
            output.collect(key, message);
    }
}
