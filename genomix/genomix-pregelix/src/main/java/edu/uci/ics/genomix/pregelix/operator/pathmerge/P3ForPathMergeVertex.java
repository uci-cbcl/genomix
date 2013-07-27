package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.KmerBytesWritableFactory;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.State2;
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
public class P3ForPathMergeVertex extends
        Vertex<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "P3ForPathMergeVertex.kmerSize";
    public static final String ITERATIONS = "P3ForPathMergeVertex.iteration";
    public static final String PSEUDORATE = "P3ForPathMergeVertex.pseudoRate";
    public static final String MAXROUND = "P3ForPathMergeVertex.maxRound";
    public static int kmerSize = -1;
    private int maxIteration = -1;
    public static float pseudoRate = -1;
    public static int maxRound = -1;

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
        if(pseudoRate < 0)
            pseudoRate = getContext().getConfiguration().getFloat(PSEUDORATE, 0.2f);
        if (maxRound < 0)
            maxRound = getContext().getConfiguration().getInt(MAXROUND, 2);
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
        if (msgIterator.hasNext()) {
            do {
                if (!VertexUtil.isPathVertex(getVertexValue())
                        && !VertexUtil.isHeadWithoutIndegree(getVertexValue())
                        && !VertexUtil.isRearWithoutOutdegree(getVertexValue())) {
                    msgIterator.next();
                    voteToHalt();
                } else {
                    incomingMsg = msgIterator.next();
                    setState();
                }
            } while (msgIterator.hasNext());
        } else {
            float random = (float) Math.random();
            if (random < pseudoRate)
                markPseudoHead();
            else{
                getVertexValue().setState(State2.NON_VERTEX);
                voteToHalt();
            }
            /*if (getVertexId().toString().equals("CCTCA") || getVertexId().toString().equals("CTCAG")) //AGTAC CCTCA CTCAG CGCCC ACGCC
                markPseudoHead();
            else
                voteToHalt();*/
        }
    }
    
    /**
     * set vertex state
     */
    public void setState() {
        if (incomingMsg.getFlag() == Message.START) {
            getVertexValue().setState(State2.START_VERTEX);
        } else if (incomingMsg.getFlag() == Message.END && getVertexValue().getState() != State2.START_VERTEX) {
            getVertexValue().setState(State2.END_VERTEX);
            voteToHalt();
        } else
            voteToHalt();
    }
   
    /**
     * mark the pseudoHead
     */
    public void markPseudoHead() {
        getVertexValue().setState(State2.PSEUDOHEAD);
        outgoingMsg.setFlag(Message.FROMPSEUDOHEAD);
        destVertexId
                .set(getPreDestVertexId(getVertexValue()));
        sendMsg(destVertexId, outgoingMsg);
    }

    /**
     * mark the pseudoRear
     */
    public void markPseudoRear() {
        if (incomingMsg.getFlag() == Message.FROMPSEUDOHEAD 
                && getVertexValue().getState() != State2.START_VERTEX) {
            getVertexValue().setState(State2.PSEUDOREAR);
            voteToHalt();
        }
        else if(incomingMsg.getFlag() == Message.FROMPSEUDOHEAD 
                && getVertexValue().getState() == State2.START_VERTEX){
            getVertexValue().setState(State2.START_HALT);
        }
    }
 
    /**
     * merge chain vertex
     */
    public void mergeChainVertex(){
        lastKmer.set(kmerFactory.getLastKmerFromChain(incomingMsg.getLengthOfChain() - kmerSize + 1,
                incomingMsg.getActualKmer()));
        getVertexValue().setKmer(
                kmerFactory.mergeTwoKmer(getVertexValue().getKmer(), 
                        lastKmer));
        getVertexValue().setOutgoingList(incomingMsg.getNeighberNode());
    }
    
    /**
     * head node sends message to path node
     */
    public void sendMsgToPathVertexMergePhase(Iterator<MessageWritable> msgIterator) {
        if (getSuperstep() == 3 + 2 * maxRound + 2) {
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(getNextDestVertexId(getVertexValue()));
            sendMsg(destVertexId, outgoingMsg);
        } else {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if (incomingMsg.getFlag() != Message.STOP) {
                    mergeChainVertex();
                    outgoingMsg.setSourceVertexId(getVertexId());
                    destVertexId
                            .set(getNextDestVertexId(getVertexValue()));
                    sendMsg(destVertexId, outgoingMsg);
                } else {
                    mergeChainVertex();
                    getVertexValue().setState(State2.FINAL_VERTEX);
                    //String source = getVertexValue().getKmer().toString();
                    //System.out.println();
                }
            }
        }
    }

    /**
     * path node sends message back to head node
     */
    public void responseMsgToHeadVertexMergePhase() {
        deleteVertex(getVertexId());
        outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList());
        outgoingMsg.setAcutalKmer(getVertexValue().getKmer());
        if (getVertexValue().getState() == State2.END_VERTEX)
            outgoingMsg.setFlag(Message.STOP);
        sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
    }
    
    /**
     * head node sends message to path node in partition phase
     */
    public void sendMsgToPathVertexPartitionPhase(Iterator<MessageWritable> msgIterator) {
        if (getSuperstep() == 4) {
            if(getVertexValue().getState() != State2.START_HALT){
                outgoingMsg.setSourceVertexId(getVertexId());
                destVertexId.set(getNextDestVertexId(getVertexValue()));
                sendMsg(destVertexId, outgoingMsg);
                voteToHalt();
            }
        } else {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                //if from pseudoHead, voteToHalt(), otherwise ...
                if (incomingMsg.getFlag() != Message.FROMPSEUDOHEAD){
                    mergeChainVertex();
                    if (incomingMsg.getFlag() != Message.STOP 
                            && incomingMsg.getFlag() != Message.FROMPSEUDOREAR) {
                        outgoingMsg.setSourceVertexId(getVertexId());
                        destVertexId.set(getNextDestVertexId(getVertexValue()));
                        sendMsg(destVertexId, outgoingMsg);
                        voteToHalt();
                    } else {
                        //check head or pseudoHead
                        if (getVertexValue().getState() == State2.START_VERTEX
                                && incomingMsg.getFlag() == Message.STOP) {
                            getVertexValue().setState(State2.FINAL_VERTEX);
                            //String source = getVertexValue().getKmer().toString();
                            //System.out.println();
                        } else if(getVertexValue().getState() == State2.PSEUDOHEAD
                                && incomingMsg.getFlag() == Message.STOP)
                            getVertexValue().setState(State2.END_VERTEX);
                    }
                }
            }
        }
    }

    /**
     * path node sends message back to head node in partition phase
     */
    public void responseMsgToHeadVertexPartitionPhase() {
        if (getVertexValue().getState() == State2.PSEUDOHEAD)
            outgoingMsg.setFlag(Message.FROMPSEUDOHEAD);
        else {
            deleteVertex(getVertexId());
            outgoingMsg.setNeighberNode(getVertexValue().getOutgoingList()); //incomingMsg.getNeighberNode()
            outgoingMsg.setAcutalKmer(getVertexValue().getKmer());
            if (getVertexValue().getState() == State2.PSEUDOREAR)
                outgoingMsg.setFlag(Message.FROMPSEUDOREAR);
            else if (getVertexValue().getState() == State2.END_VERTEX)
                outgoingMsg.setFlag(Message.STOP);
        }
        sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
        voteToHalt();
    }
    
    /**
     * final process the result of partition phase
     */
    public void finalProcessPartitionPhase(Iterator<MessageWritable> msgIterator){
        while (msgIterator.hasNext()) {
            incomingMsg = msgIterator.next();
            mergeChainVertex();
            getVertexValue().setOutgoingList(incomingMsg.getNeighberNode());
            //check head or pseudoHead
            if (getVertexValue().getState() == State2.START_VERTEX
                    && incomingMsg.getFlag() == Message.STOP) {
                getVertexValue().setState(State2.FINAL_VERTEX);
                //String source = getVertexValue().getKmer().toString();
                //System.out.println();
            } else if(getVertexValue().getState() == State2.PSEUDOHEAD
                    && incomingMsg.getFlag() == Message.STOP)
                getVertexValue().setState(State2.END_VERTEX);
        }
    }
    /**
     * After partition phase, reset state: ex. psudoHead and psudoRear -> null
     */
    public void resetState() {
        if (getVertexValue().getState() == State2.PSEUDOHEAD || getVertexValue().getState() == State2.PSEUDOREAR) {
            getVertexValue().setState(State2.NON_VERTEX);
        }
        if(getVertexValue().getState() == State2.START_HALT)
            getVertexValue().setState(State2.START_VERTEX);
    }

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if (getSuperstep() == 1)
            startSendMsg();
        else if (getSuperstep() == 2)
            initState(msgIterator);
        else if (getSuperstep() == 3) {
            if (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                markPseudoRear();
            }
        } else if (getSuperstep() % 2 == 0 && getSuperstep() <= 3 + 2 * maxRound && getSuperstep() <= maxIteration) {
            sendMsgToPathVertexPartitionPhase(msgIterator);
        } else if (getSuperstep() % 2 == 1 && getSuperstep() <= 3 + 2 * maxRound && getSuperstep() <= maxIteration) {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                responseMsgToHeadVertexPartitionPhase();
            }
        } else if(getSuperstep() == 3 + 2 * maxRound + 1 && getSuperstep() <= maxIteration){
            finalProcessPartitionPhase(msgIterator);
        } else if (getSuperstep() % 2 == 1 && getSuperstep() <= maxIteration) {
            resetState();
            if(getVertexValue().getState() == State2.START_VERTEX)
                sendMsgToPathVertexMergePhase(msgIterator);
            voteToHalt();
        } else if (getSuperstep() % 2 == 0 && getSuperstep() <= maxIteration) {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                responseMsgToHeadVertexMergePhase();
            }
            voteToHalt();
        } else
            voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(P3ForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(P3ForPathMergeVertex.class);
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
