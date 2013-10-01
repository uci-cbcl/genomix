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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.hadoop.tp.graphclean.io.GraphCleanNodeOperation;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageAbasic3Writable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageArrayWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.io.MessageWritable;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.Message;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.MessageFlag;
import edu.uci.ics.genomix.hadoop.tp.graphclean.type.VertexUtil;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

@SuppressWarnings("deprecation")
public class ACleanMapper3 extends MapReduceBase implements
        Mapper<NodeWritable, MessageArrayWritable, NodeWritable, MessageWritable> {

    private NodeWritable desKeyNode;
    private MessageWritable message;
    private GraphCleanNodeOperation gcCleanner;
    protected Iterator<MessageWritable> mesIterator;
    private Map<VKmerBytesWritable, ArrayList<MessageWritable>> receivedMsgMap = new HashMap<VKmerBytesWritable, ArrayList<MessageWritable>>();
    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
    private Set<MessageWritable> unchangedSet = new HashSet<MessageWritable>();
    private Set<MessageWritable> deletedSet = new HashSet<MessageWritable>();
    private int dissimilarThreshold;
    private MessageAbasic3Writable gg = new MessageAbasic3Writable();
    
    public void configure(JobConf job) {
        desKeyNode = new NodeWritable();
        message = new MessageWritable();
        dissimilarThreshold = job.getInt("dissimilarThreshold", 0);
    }

    @SuppressWarnings("unchecked")
    public void aggregateBubbleNodesByMajorNode(MessageWritable msg) {
        if (!receivedMsgMap.containsKey(msg.getStartVertexId())) {
            receivedMsgList.clear();
            receivedMsgList.add(msg);
            receivedMsgMap.put(msg.getStartVertexId(), (ArrayList<MessageWritable>) receivedMsgList.clone());
        } else {
            receivedMsgList.clear();
            receivedMsgList.addAll(receivedMsgMap.get(msg.getStartVertexId()));
            receivedMsgList.add(msg);
            receivedMsgMap.put(msg.getStartVertexId(), (ArrayList<MessageWritable>) receivedMsgList.clone());
        }
    }

    public void processSimilarSetToUnchangeSetAndDeletedSet() {
        unchangedSet.clear();
        deletedSet.clear();
        MessageWritable topCoverageMessage = new MessageWritable();
        MessageWritable tmpMessage = new MessageWritable();
        Iterator<MessageWritable> it;
        while (!receivedMsgList.isEmpty()) {
            it = receivedMsgList.iterator();
            topCoverageMessage.set(it.next());
            it.remove(); //delete topCoverage node
            while (it.hasNext()) {
                tmpMessage.set(it.next());
                //compute the similarity  
                float fracDissimilar = topCoverageMessage.getSourceVertexId().fracDissimilar(
                        tmpMessage.getSourceVertexId());
                if (fracDissimilar < dissimilarThreshold) { //If similar with top node, delete this node and put it in deletedSet 
                    //add coverage to top node
                    topCoverageMessage.mergeCoverage(tmpMessage);
                    deletedSet.add(tmpMessage);
                    it.remove();
                }
            }
            unchangedSet.add(topCoverageMessage);
        }
    }

    public void processUnchangedSet(NodeWritable outKey, MessageWritable outMes,
            OutputCollector<NodeWritable, MessageWritable> output) throws IOException {
        byte outFlag = 0x00;
        outMes.reset();
        for (MessageWritable msg : unchangedSet) {
            outFlag = MessageFlag.UNCHANGE;
            outMes.setFlag(outFlag);
            outMes.setAverageCoverage(msg.getAverageCoverage());
            outKey.setKmer(msg.getSourceVertexId());
            output.collect(outKey, outMes);
        }
    }

    public void processDeletedSet(NodeWritable outKey, MessageWritable outMes, MessageAbasic3Writable aOutMes,
            OutputCollector<NodeWritable, MessageWritable> output) throws IOException {
        byte outFlag = 0x00;
        for (MessageWritable msg : deletedSet) {
            outFlag = MessageFlag.KILL;
            outMes.setFlag(outFlag);
            outKey.setKmer(msg.getSourceVertexId());
            output.collect(outKey, outMes);
            outKey.setKmer(msg.getStartVertexId());
            aOutMes.setToDeletedVkmer(msg.getSourceVertexId());
            output.collect(outKey, aOutMes);
        }
    }

    @Override
    public void map(NodeWritable key, MessageArrayWritable value,
            OutputCollector<NodeWritable, MessageWritable> output, Reporter reporter) throws IOException {
        desKeyNode.reset();
        message.reset();
        gg.reset();
        mesIterator = value.iterator();
        while (mesIterator.hasNext()) {
            aggregateBubbleNodesByMajorNode(mesIterator.next());
        }
        receivedMsgMap.clear();
        receivedMsgList.clear();
        for (VKmerBytesWritable prevId : receivedMsgMap.keySet()) {
            receivedMsgList = receivedMsgMap.get(prevId);
            if (receivedMsgList.size() > 1) { // filter bubble
                /** for each startVertex, sort the node by decreasing order of coverage **/

                Collections.sort(receivedMsgList, new MessageWritable.SortByCoverage());

                /** process similarSet, keep the unchanged set and deleted set & add coverage to unchange node **/
                processSimilarSetToUnchangeSetAndDeletedSet();

                /** send message to the unchanged set for updating coverage & send kill message to the deleted set **/
                processUnchangedSet(desKeyNode, message, output);
                processDeletedSet(desKeyNode, message, gg, output);
//                gcCleanner.responseToDeadVertex();
            }
        }
    }
}
