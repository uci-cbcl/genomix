package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

//import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

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
 * For example, ONE LINE in input file: 00,01,10	0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 * Naive Algorithm for path merge graph
 */
public class P1ForPathMergeVertex extends
        Vertex<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "P1ForPathMergeVertex.kmerSize";
    public static final String ITERATIONS = "P1ForPathMergeVertex.iteration";
    public static int kmerSize = -1;
    private int maxIteration = -1;

    private MessageWritable incomingMsg = new MessageWritable();
    private MessageWritable outgoingMsg = new MessageWritable();
  
    private KmerBytesWritableFactory kmerFactory = new KmerBytesWritableFactory(1);
    private VKmerBytesWritable lastKmer = new VKmerBytesWritable();
    
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
    public PositionWritable getNextDestVertexId(VertexValueWritable value) {
        if(value.getFFList().getCountOfPosition() > 0) // #FFList() > 0
            posIterator = value.getFFList().iterator();
        else // #FRList() > 0
            posIterator = value.getFRList().iterator();
        return posIterator.next();
    }

    public PositionWritable getPreDestVertexId(VertexValueWritable value) {
        if(value.getRFList().getCountOfPosition() > 0) // #RFList() > 0
            posIterator = value.getRFList().iterator();
        else // #RRList() > 0
            posIterator = value.getRRList().iterator();
        return posIterator.next();
    }

    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes(VertexValueWritable value) {
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
    public void sendMsgToAllPreviousNodes(VertexValueWritable value) {
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
        if (VertexUtil.isHeadVertexWithIndegree(getVertexValue())) {
            outgoingMsg.setFlag(Message.START);
            sendMsgToAllNextNodes(getVertexValue());
        }
        if (VertexUtil.isRearVertexWithOutdegree(getVertexValue())) {
            outgoingMsg.setFlag(Message.END);
            sendMsgToAllPreviousNodes(getVertexValue());
        }
        if (VertexUtil.isHeadWithoutIndegree(getVertexValue())){
            outgoingMsg.setFlag(Message.START);
            sendMsg(getVertexId(), outgoingMsg); //send to itself
        }
        if (VertexUtil.isRearWithoutOutdegree(getVertexValue())){
            outgoingMsg.setFlag(Message.END);
            sendMsg(getVertexId(), outgoingMsg); //send to itself
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
        if (incomingMsg.getFlag() == Message.START) {
            getVertexValue().setState(State.IS_HEAD);
        } else if (incomingMsg.getFlag() == Message.END && getVertexValue().getState() != State.IS_HEAD) {
            getVertexValue().setState(State.IS_HEAD);//is tail
            voteToHalt();
        } else
            voteToHalt();
    }
    
    /**
     * merge chainVertex and store in vertexVal.chainVertexId
     */
    public void mergeChainVertex() {
        //merge chain
        lastKmer.setAsCopy(kmerFactory.getLastKmerFromChain(incomingMsg.getLengthOfChain() - kmerSize + 1,
                incomingMsg.getActualKmer()));
        getVertexValue().setKmer(kmerFactory.mergeTwoKmer(getVertexValue().getKmer(), lastKmer));
        getVertexValue().setOutgoingList(incomingMsg.getNeighberNode());
    }

    /**
     * head node sends message to path node
     */
    public void sendMsgToPathVertex(Iterator<MessageWritable> msgIterator) {
        if (getSuperstep() == 3) {
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(getNextDestVertexId(getVertexValue()));
            sendMsg(destVertexId, outgoingMsg);
        } else {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if (incomingMsg.getFlag() != Message.STOP) {
                    mergeChainVertex();
                    outgoingMsg.setSourceVertexId(getVertexId());
                    destVertexId.set(getNextDestVertexId(getVertexValue()));
                    sendMsg(destVertexId, outgoingMsg);
                } else {
                    mergeChainVertex();
                    getVertexValue().setState(State.IS_FINAL);
                    //String source = getVertexValue().getKmer().toString();
                    //System.out.println();
                }
            }
        }
    }

    /**
     * path node sends message back to head node
     */
    public void responseMsgToHeadVertex() {
        deleteVertex(getVertexId());
        outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
        outgoingMsg.setAcutalKmer(getVertexValue().getKmer());
        if (getVertexValue().getState() == State.IS_HEAD)//is_tail
            outgoingMsg.setFlag(Message.STOP);
        destVertexId.setAsCopy(incomingMsg.getSourceVertexId());
        sendMsg(destVertexId, outgoingMsg);
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1) {
            startSendMsg();
            voteToHalt();
        } else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() % 2 == 1 && getSuperstep() <= maxIteration) {
            sendMsgToPathVertex(msgIterator);
            voteToHalt();
        } else if (getSuperstep() % 2 == 0 && getSuperstep() > 2 && getSuperstep() <= maxIteration) {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                responseMsgToHeadVertex();
            }
            voteToHalt();
        } else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(P1ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P1ForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
