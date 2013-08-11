package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.util.ArrayList;
import java.util.Iterator;

import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.VKmerBytesWritable;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: MessageWritable
 * 
 * DNA:
 * A: 00
 * C: 01
 * G: 10
 * T: 11
 * 
 * succeed node
 *  A 00000001 1
 *  G 00000010 2
 *  C 00000100 4
 *  T 00001000 8
 * precursor node
 *  A 00010000 16
 *  G 00100000 32
 *  C 01000000 64
 *  T 10000000 128
 *  
 * For example, ONE LINE in input file: 00,01,10    0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 * Naive Algorithm for path merge graph
 */
public class BridgeRemoveVertex extends
    BasicGraphCleanVertex {
    public static final String LENGTH = "BridgeRemoveVertex.length";
    private int length = -1;

    private ArrayList<MessageWritable> receivedMsgList = new ArrayList<MessageWritable>();
   
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if(length == -1)
            length = getContext().getConfiguration().getInt(LENGTH, kmerSize);
        if(incomingMsg == null)
            incomingMsg = new MessageWritable(kmerSize);
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable(kmerSize);
        else
            outgoingMsg.reset(kmerSize);
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable(kmerSize);
        receivedMsgList.clear();
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            if(VertexUtil.isUpBridgeVertex(getVertexValue())){
                sendSettledMsgToAllNextNodes(getVertexValue());
            }
            else if(VertexUtil.isDownBridgeVertex(getVertexValue())){
                sendSettledMsgToAllPreviousNodes(getVertexValue());
            }
        }
        else if (getSuperstep() == 2){
            int i = 0;
            while (msgIterator.hasNext()) {
                if(i == 3)
                    break;
                receivedMsgList.add(msgIterator.next());
                i++;
            }
            if(receivedMsgList.size() == 2){
                if(getVertexValue().getKmerLength() <= length 
                        && getVertexValue().getDegree() == 2){
                    broadcaseKillself();
                }
            }
        }
        else if(getSuperstep() == 3){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                responseToDeadVertex();
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BridgeRemoveVertex.class.getSimpleName());
        job.setVertexClass(BridgeRemoveVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
