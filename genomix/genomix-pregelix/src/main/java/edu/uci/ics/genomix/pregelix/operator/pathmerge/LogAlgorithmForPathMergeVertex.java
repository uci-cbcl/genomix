package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.LogAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;
import edu.uci.ics.genomix.type.PositionWritable;
/*
 * vertexId: BytesWritable
 * vertexValue: ValueStateWritable
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
 * For example, ONE LINE in input file: 00,01,10	0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
public class LogAlgorithmForPathMergeVertex extends
        Vertex<PositionWritable, ValueStateWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "LogAlgorithmForPathMergeVertex.kmerSize";
    public static final String ITERATIONS = "LogAlgorithmForPathMergeVertex.iteration";
    public static int kmerSize = -1;
    private int maxIteration = -1;

    private MessageWritable incomingMsg = new MessageWritable();
    private MessageWritable outgoingMsg = new MessageWritable();

    private KmerBytesWritableFactory kmerFactory = new KmerBytesWritableFactory(1);
    private KmerBytesWritable lastKmer = new KmerBytesWritable(1);

    private PositionWritable destVertexId = new PositionWritable();
    private Iterator<PositionWritable> posIterator;
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        outgoingMsg.reset();
    }

    /**
     * get destination vertex
     */
    public PositionWritable getNextDestVertexId(ValueStateWritable value) {
        if(value.getFFList().getCountOfPosition() > 0) // #FFList() > 0
            posIterator = value.getFFList().iterator();
        else // #FRList() > 0
            posIterator = value.getFRList().iterator();
        return posIterator.next();
    }

    public PositionWritable getPreDestVertexId(ValueStateWritable value) {
        if(value.getRFList().getCountOfPosition() > 0) // #RFList() > 0
            posIterator = value.getRFList().iterator();
        else // #RRList() > 0
            posIterator = value.getRRList().iterator();
        return posIterator.next();
    }

    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes(ValueStateWritable value) {
        posIterator = value.getFFList().iterator(); // FFList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getFRList().iterator(); // FRList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    /**
     * head send message to all previous nodes
     */
    public void sendMsgToAllPreviousNodes(ValueStateWritable value) {
        posIterator = value.getRFList().iterator(); // RFList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
        posIterator = value.getRRList().iterator(); // RRList
        while(posIterator.hasNext()){
            destVertexId.set(posIterator.next());
            sendMsg(destVertexId, outgoingMsg);
        }
    }

    /**
     * start sending message
     */
    public void startSendMsg() {
        if (VertexUtil.isHeadVertex(getVertexValue())) {
            outgoingMsg.setMessage(Message.START);
            sendMsgToAllNextNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isRearVertex(getVertexValue())) {
            outgoingMsg.setMessage(Message.END);
            sendMsgToAllPreviousNodes(getVertexValue());
            voteToHalt();
        }
        if (VertexUtil.isHeadWithoutIndegree(getVertexValue())){
            outgoingMsg.setMessage(Message.START);
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
        if (VertexUtil.isRearWithoutOutdegree(getVertexValue())){
            outgoingMsg.setMessage(Message.END);
            sendMsg(getVertexId(), outgoingMsg); //send to itself
            voteToHalt();
        }
    }

    /**
     * initiate head, rear and path node
     */
    public void initState(Iterator<MessageWritable> msgIterator) {
        while (msgIterator.hasNext()) {
            if (!VertexUtil.isPathVertex(getVertexValue())
                    && !VertexUtil.isHeadWithoutIndegree(getVertexValue())
                    && !VertexUtil.isRearWithoutOutdegree(getVertexValue())) {
                msgIterator.next();
                voteToHalt();
            } else {
                incomingMsg = msgIterator.next();
                setState();
            }
        }
    }

    /**
     * set vertex state
     */
    public void setState() {
        if (incomingMsg.getMessage() == Message.START) {
            getVertexValue().setState(State.START_VERTEX);
            //getVertexValue().setMergeChain(null);
        } else if (incomingMsg.getMessage() == Message.END && getVertexValue().getState() != State.START_VERTEX) {
            getVertexValue().setState(State.END_VERTEX);
            getVertexValue().setMergeChain(getVertexValue().getMergeChain());
            voteToHalt();
        } else
            voteToHalt();
    }

    /**
     * head send message to path
     */
    public void sendOutMsg() {
        if (getVertexValue().getState() == State.START_VERTEX) {
            outgoingMsg.setMessage(Message.START);
            outgoingMsg.setSourceVertexId(getVertexId());
            sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
        } else if (getVertexValue().getState() != State.END_VERTEX) {
            outgoingMsg.setMessage(Message.NON);
            outgoingMsg.setSourceVertexId(getVertexId());
            sendMsg(getNextDestVertexId(getVertexValue()), outgoingMsg);
        }
    }

    /**
     * head send message to path
     */
    public void sendMsgToPathVertex(Iterator<MessageWritable> msgIterator) {
        if (getSuperstep() == 3) {
            sendOutMsg();
        } else {
            if (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if (mergeChainVertex()) {
                    if (incomingMsg.getMessage() == Message.END) {
                        if (getVertexValue().getState() == State.START_VERTEX) {
                            getVertexValue().setState(State.FINAL_VERTEX);
                            //String source = getVertexValue().getMergeChain().toString();
                            //System.out.println();
                        } else
                            getVertexValue().setState(State.END_VERTEX);
                    } else
                        sendOutMsg();
                }
            }
        }
    }

    /**
     * path response message to head
     */
    public void responseMsgToHeadVertex(Iterator<MessageWritable> msgIterator) {
        if (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            outgoingMsg.setChainVertexId(getVertexValue().getMergeChain());
            outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
            if (getVertexValue().getState() == State.END_VERTEX)
                outgoingMsg.setMessage(Message.END);
            sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);

            if (incomingMsg.getMessage() == Message.START)
                deleteVertex(getVertexId());
        } else {
            if (getVertexValue().getState() != State.START_VERTEX && getVertexValue().getState() != State.END_VERTEX)
                deleteVertex(getVertexId());//killSelf because it doesn't receive any message
        }
    }

    /**
     * merge chainVertex and store in vertexVal.chainVertexId
     */
    public boolean mergeChainVertex() {
        //merge chain
        lastKmer.set(kmerFactory.getLastKmerFromChain(incomingMsg.getLengthOfChain() - kmerSize + 1,
                incomingMsg.getChainVertexId()));
        KmerBytesWritable chainVertexId = kmerFactory.mergeTwoKmer(getVertexValue().getMergeChain(), lastKmer);
        getVertexValue().setMergeChain(chainVertexId);
        getVertexValue().setOutgoingList(incomingMsg.getNeighberNode());
        if (VertexUtil.isCycle(kmerFactory.getFirstKmerFromChain(kmerSize, getVertexValue().getMergeChain()),
                chainVertexId, kmerSize)) {
            getVertexValue().setState(State.CYCLE);
            return false;
        } 
        return true;
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 2 == 1 && getSuperstep() <= maxIteration) {
            sendMsgToPathVertex(msgIterator);
            voteToHalt();
        } else if (getSuperstep() % 2 == 0 && getSuperstep() <= maxIteration) {
            responseMsgToHeadVertex(msgIterator);
            voteToHalt();
        } else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(LogAlgorithmForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(LogAlgorithmForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput~/
         */
        job.setVertexInputFormatClass(LogAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(LogAlgorithmForPathMergeOutputFormat.class);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        job.setDynamicVertexValueSize(true);
        Client.run(args, job);
    }
}
