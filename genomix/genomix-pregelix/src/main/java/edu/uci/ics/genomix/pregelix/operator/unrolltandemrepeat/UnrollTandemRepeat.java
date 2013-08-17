package edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.format.InitialGraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.tipremove.TipRemoveVertex;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class UnrollTandemRepeat extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable>{
    public static final String KMER_SIZE = "BasicGraphCleanVertex.kmerSize";
    public static int kmerSize = -1;
    
    private VKmerBytesWritable curKmer = new VKmerBytesWritable();
    private byte repeatDir = 0;
    
    private VertexValueWritable tmpValue = new VertexValueWritable();
    private EdgeWritable tmpEdge = new EdgeWritable();
    private MessageWritable incomingMsg = new MessageWritable();
    private MessageWritable outgoingMsg = new MessageWritable();
    
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        outgoingMsg.reset();
    }
    
    /**
     * check if it is a tandem repeat
     */
    public boolean isTandemRepeat(){
        for(byte d : DirectionFlag.values){
            Iterator<VKmerBytesWritable> it = getVertexValue().getEdgeList(d).getKeys();
            while(it.hasNext()){
                curKmer.setAsCopy(it.next());
                if(curKmer.equals(getVertexId())){
                    repeatDir = d;
                    return true;
                }
            }
        }
        return false;
    }
    
    public byte mirrorDirection(byte dir) {
        switch (dir) {
            case MessageFlag.DIR_FF:
                return MessageFlag.DIR_RR;
            case MessageFlag.DIR_FR:
                return MessageFlag.DIR_FR;
            case MessageFlag.DIR_RF:
                return MessageFlag.DIR_RF;
            case MessageFlag.DIR_RR:
                return MessageFlag.DIR_FF;
            default:
                throw new RuntimeException("Unrecognized direction in flipDirection: " + dir);
        }
    }
    
    public byte flipDir(byte dir){
        switch(dir){
            case DirectionFlag.DIR_FF:
                return DirectionFlag.DIR_RF;
            case DirectionFlag.DIR_FR:
                return DirectionFlag.DIR_RR;
            case DirectionFlag.DIR_RF:
                return DirectionFlag.DIR_FF;
            case DirectionFlag.DIR_RR:
                return DirectionFlag.DIR_FR;
            default:
                throw new RuntimeException("Unrecognized direction in flipDirection: " + dir);
        }
    }
    
    /**
     * check if this tandem repeat can be solved
     */
    public boolean repeatCanBeMerged(){
        tmpValue.setAsCopy(getVertexValue());
        tmpValue.getEdgeList(repeatDir).remove(curKmer);
        boolean hasFlip = false;
        /** pick one edge and flip **/
        for(byte d : DirectionFlag.values){
            for(EdgeWritable edge : tmpValue.getEdgeList(d)){
                byte flipDir = flipDir(d);
                tmpValue.getEdgeList(flipDir).add(edge);
                tmpValue.getEdgeList(d).remove(edge);
                /** setup hasFlip to go out of the loop **/
                hasFlip = true;
                break;
            }
            if(hasFlip)
                break;
        }
        
        if(VertexUtil.isPathVertex(tmpValue) || VertexUtil.isTipVertex(tmpValue)
                || VertexUtil.isSingleVertex(tmpValue))
            return true;
        else
            return false;
    }
    
    /**
     * merge tandem repeat
     */
    public void mergeTandemRepeat(){
        getVertexValue().getInternalKmer().mergeWithKmerInDir(repeatDir, kmerSize, getVertexId());
        getVertexValue().getEdgeList(repeatDir).remove(getVertexId());
        boolean hasFlip = false;
        /** pick one edge and flip **/
        for(byte d : DirectionFlag.values){
            for(EdgeWritable edge : getVertexValue().getEdgeList(d)){
                byte flipDir = flipDir(d);
                getVertexValue().getEdgeList(flipDir).add(edge);
                getVertexValue().getEdgeList(d).remove(edge);
                /** send flip message to node for updating edgeDir **/
                outgoingMsg.setFlag(flipDir);
                outgoingMsg.setSourceVertexId(getVertexId());
                sendMsg(edge.getKey(), outgoingMsg);
                /** setup hasFlip to go out of the loop **/
                hasFlip = true;
                break;
            }
            if(hasFlip)
                break;
        }
    }
    
    /**
     * update edges
     */
    public void updateEdges(){
        byte flipDir = flipDir(incomingMsg.getFlag());
        byte prevNeighborToMe = mirrorDirection(flipDir);
        byte curNeighborToMe = mirrorDirection(incomingMsg.getFlag());
        tmpEdge.setAsCopy(getVertexValue().getEdgeList(prevNeighborToMe).getEdge(incomingMsg.getSourceVertexId()));
        getVertexValue().getEdgeList(prevNeighborToMe).remove(incomingMsg.getSourceVertexId());
        getVertexValue().getEdgeList(curNeighborToMe).add(tmpEdge);
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) throws Exception {
        initVertex();
        if(getSuperstep() == 1){
            if(isTandemRepeat() && repeatCanBeMerged()){
                mergeTandemRepeat();
            }
            voteToHalt();
        } else if(getSuperstep() == 2){
            while(msgIterator.hasNext()){
                incomingMsg = msgIterator.next();
                /** update edge **/
                updateEdges();
            }
            voteToHalt();
        }
    }
    
    public static void main(String[] args) throws Exception {
        Client.run(args, getConfiguredJob(null));
    }
    
    public static PregelixJob getConfiguredJob(GenomixJobConf conf) throws IOException {
        PregelixJob job;
        if (conf == null)
            job = new PregelixJob(UnrollTandemRepeat.class.getSimpleName());
        else
            job = new PregelixJob(conf, UnrollTandemRepeat.class.getSimpleName());
        job.setVertexClass(UnrollTandemRepeat.class);
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        job.setDynamicVertexValueSize(true);
        return job;
    }
}
