package edu.uci.ics.genomix.pregelix.operator;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeInputFormat;
import edu.uci.ics.genomix.pregelix.format.NaiveAlgorithmForPathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.NaiveAlgorithmMessageWritable;
import edu.uci.ics.genomix.pregelix.io.ValueStateWritable;
import edu.uci.ics.genomix.pregelix.type.Message;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: NaiveAlgorithmMessageWritable
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
public class NaiveAlgorithmForPathMergeVertex extends
        Vertex<KmerBytesWritable, ValueStateWritable, NullWritable, NaiveAlgorithmMessageWritable> {
    public static final String KMER_SIZE = "NaiveAlgorithmForPathMergeVertex.kmerSize";
    public static final String ITERATIONS = "NaiveAlgorithmForPathMergeVertex.iteration";
    public static int kmerSize = -1;
    private int maxIteration = -1;

    private NaiveAlgorithmMessageWritable incomingMsg = new NaiveAlgorithmMessageWritable();
    private NaiveAlgorithmMessageWritable outgoingMsg = new NaiveAlgorithmMessageWritable();

    private VKmerBytesWritableFactory kmerFactory = new VKmerBytesWritableFactory(1);
    private VKmerBytesWritable destVertexId = new VKmerBytesWritable(1);

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
    public VKmerBytesWritable getDestVertexId(KmerBytesWritable vertexId, byte geneCode) {
        return kmerFactory.shiftKmerWithNextCode(vertexId, geneCode);
    }

    public VKmerBytesWritable getPreDestVertexId(KmerBytesWritable vertexId, byte geneCode) {
        return kmerFactory.shiftKmerWithPreCode(vertexId, geneCode);
    }

    public VKmerBytesWritable getDestVertexIdFromChain(VKmerBytesWritable chainVertexId, byte adjMap) {
        VKmerBytesWritable lastKmer = kmerFactory.getLastKmerFromChain(kmerSize, chainVertexId);
        return getDestVertexId(lastKmer, GeneCode.getGeneCodeFromBitMap((byte) (adjMap & 0x0F)));
    }

    /**
     * head send message to all next nodes
     */
    public void sendMsgToAllNextNodes(KmerBytesWritable vertexId, byte adjMap) {
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            if ((adjMap & (1 << x)) != 0) {
                destVertexId.set(getDestVertexId(vertexId, x));
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }

    /**
     * head send message to all previous nodes
     */
    public void sendMsgToAllPreviousNodes(KmerBytesWritable vertexId, byte adjMap) {
        for (byte x = GeneCode.A; x <= GeneCode.T; x++) {
            if (((adjMap >> 4) & (1 << x)) != 0) {
                destVertexId.set(getPreDestVertexId(vertexId, x));
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }

    /**
     * start sending message
     */
    public void startSendMsg() {
        if (VertexUtil.isHeadVertex(getVertexValue().getAdjMap())) {
            outgoingMsg.setMessage(Message.START);
            sendMsgToAllNextNodes(getVertexId(), getVertexValue().getAdjMap());
        }
        if (VertexUtil.isRearVertex(getVertexValue().getAdjMap())) {
            outgoingMsg.setMessage(Message.END);
            sendMsgToAllPreviousNodes(getVertexId(), getVertexValue().getAdjMap());
        }
    }

    /**
     * initiate head, rear and path node
     */
    public void initState(Iterator<NaiveAlgorithmMessageWritable> msgIterator) {
        while (msgIterator.hasNext()) {
            if (!VertexUtil.isPathVertex(getVertexValue().getAdjMap())) {
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
        } else if (incomingMsg.getMessage() == Message.END && getVertexValue().getState() != State.START_VERTEX) {
            getVertexValue().setState(State.END_VERTEX);
            voteToHalt();
        } else
            voteToHalt();
    }

    /**
     * head node sends message to path node
     */
    public void sendMsgToPathVertex(Iterator<NaiveAlgorithmMessageWritable> msgIterator) {
        if (getSuperstep() == 3) {
            getVertexValue().setMergeChain(getVertexId());
            outgoingMsg.setSourceVertexId(getVertexId());
            destVertexId.set(getDestVertexIdFromChain(getVertexValue().getMergeChain(), getVertexValue().getAdjMap()));
            sendMsg(destVertexId, outgoingMsg);
        } else {
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if (incomingMsg.getMessage() != Message.STOP) {
                    getVertexValue().setMergeChain(
                            kmerFactory.mergeKmerWithNextCode(getVertexValue().getMergeChain(),
                                    incomingMsg.getLastGeneCode()));
                    outgoingMsg.setSourceVertexId(getVertexId());
                    destVertexId
                            .set(getDestVertexIdFromChain(getVertexValue().getMergeChain(), incomingMsg.getAdjMap()));
                    sendMsg(destVertexId, outgoingMsg);
                } else {
                    getVertexValue().setMergeChain(
                            kmerFactory.mergeKmerWithNextCode(getVertexValue().getMergeChain(),
                                    incomingMsg.getLastGeneCode()));
                    byte adjMap = VertexUtil.updateRightNeighber(getVertexValue().getAdjMap(), incomingMsg.getAdjMap());
                    getVertexValue().setAdjMap(adjMap);
                    getVertexValue().setState(State.FINAL_VERTEX);
                    //String source = getVertexValue().getMergeChain().toString();
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
        outgoingMsg.setAdjMap(getVertexValue().getAdjMap());
        outgoingMsg.setLastGeneCode(getVertexId().getGeneCodeAtPosition(kmerSize - 1));
        if (getVertexValue().getState() == State.END_VERTEX)
            outgoingMsg.setMessage(Message.STOP);
        sendMsg(incomingMsg.getSourceVertexId(), outgoingMsg);
    }

    @Override
    public void compute(Iterator<NaiveAlgorithmMessageWritable> msgIterator) {
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
        PregelixJob job = new PregelixJob(NaiveAlgorithmForPathMergeVertex.class.getSimpleName());
        job.setVertexClass(NaiveAlgorithmForPathMergeVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(NaiveAlgorithmForPathMergeInputFormat.class);
        job.setVertexOutputFormatClass(NaiveAlgorithmForPathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(ValueStateWritable.class);
        Client.run(args, job);
    }
}
