package edu.uci.ics.genomix.pregelix.operator.tipremove;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.PositionWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.DataCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.DataCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.type.AdjMessage;
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
 * For example, ONE LINE in input file: 00,01,10    0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 *  Remove tip or single node when l > constant
 */
public class TipRemoveVertex extends
        Vertex<PositionWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "TipRemoveVertex.kmerSize";
    public static final String LENGTH = "TipRemoveVertex.length";
    public static int kmerSize = -1;
    private int length = -1;

    private MessageWritable incomingMsg = new MessageWritable();
    private MessageWritable outgoingMsg = new MessageWritable();
    
    private Iterator<PositionWritable> iterator;
    private PositionWritable pos = new PositionWritable();
    private PositionWritable destVertexId = new PositionWritable();
    private Iterator<PositionWritable> posIterator;
    
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if(length == -1)
            length = getContext().getConfiguration().getInt(LENGTH, kmerSize); //kmerSize + 5
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

    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex(); 
        if(getSuperstep() == 1){
            if(VertexUtil.isIncomingTipVertex(getVertexValue())){
            	if(getVertexValue().getLengthOfMergeChain() <= length){
            		if(getVertexValue().getFFList().getCountOfPosition() > 0)
            			outgoingMsg.setFlag(AdjMessage.FROMFF);
            		else if(getVertexValue().getFRList().getCountOfPosition() > 0)
            			outgoingMsg.setFlag(AdjMessage.FROMFR);
            		outgoingMsg.setSourceVertexId(getVertexId());
            		destVertexId.set(getNextDestVertexId(getVertexValue()));
            		sendMsg(destVertexId, outgoingMsg);
            		deleteVertex(getVertexId());
            	}
            }
            else if(VertexUtil.isOutgoingTipVertex(getVertexValue())){
                if(getVertexValue().getLengthOfMergeChain() <= length){
                    if(getVertexValue().getRFList().getCountOfPosition() > 0)
                        outgoingMsg.setFlag(AdjMessage.FROMRF);
                    else if(getVertexValue().getRRList().getCountOfPosition() > 0)
                        outgoingMsg.setFlag(AdjMessage.FROMRR);
                    outgoingMsg.setSourceVertexId(getVertexId());
                    destVertexId.set(getPreDestVertexId(getVertexValue()));
                    sendMsg(destVertexId, outgoingMsg);
                    deleteVertex(getVertexId());
                }
            }
            else if(VertexUtil.isSingleVertex(getVertexValue())){
                if(getVertexValue().getLengthOfMergeChain() > length)
                    deleteVertex(getVertexId());
            }
        }
        else if(getSuperstep() == 2){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(incomingMsg.getFlag() == AdjMessage.FROMFF){
                    //remove incomingMsg.getSourceId from RR positionList
                    iterator = getVertexValue().getRRList().iterator();
                    while(iterator.hasNext()){
                        pos = iterator.next();
                        if(pos.equals(incomingMsg.getSourceVertexId())){
                            iterator.remove();
                            break;
                        }
                    }
                } else if(incomingMsg.getFlag() == AdjMessage.FROMFR){
                    //remove incomingMsg.getSourceId from RF positionList
                    iterator = getVertexValue().getRFList().iterator();
                    while(iterator.hasNext()){
                        pos = iterator.next();
                        if(pos.equals(incomingMsg.getSourceVertexId())){
                            iterator.remove();
                            break;
                        }
                    }
                } else if(incomingMsg.getFlag() == AdjMessage.FROMRF){
                    //remove incomingMsg.getSourceId from FR positionList
                    iterator = getVertexValue().getFRList().iterator();
                    while(iterator.hasNext()){
                        pos = iterator.next();
                        if(pos.equals(incomingMsg.getSourceVertexId())){
                            iterator.remove();
                            break;
                        }
                    }
                } else{ //incomingMsg.getFlag() == AdjMessage.FROMRR
                    //remove incomingMsg.getSourceId from FF positionList
                    iterator = getVertexValue().getFFList().iterator();
                    while(iterator.hasNext()){
                        pos = iterator.next();
                        if(pos.equals(incomingMsg.getSourceVertexId())){
                            iterator.remove();
                            break;
                        }
                    }
                }
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(TipRemoveVertex.class.getSimpleName());
        job.setVertexClass(TipRemoveVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(DataCleanInputFormat.class);
        job.setVertexOutputFormatClass(DataCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(PositionWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}
