package edu.uci.ics.genomix.pregelix.operator.unrolltandemrepeat;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.genomix.config.GenomixJobConf;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.operator.BasicGraphCleanVertex;
import edu.uci.ics.genomix.pregelix.util.VertexUtil;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;
import edu.uci.ics.pregelix.api.job.PregelixJob;

public class UnrollTandemRepeat extends
    BasicGraphCleanVertex<MessageWritable>{
    private EdgeWritable tmpEdge = new EdgeWritable();
    
    /**
     * initiate kmerSize, length
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = Integer.parseInt(getContext().getConfiguration().get(GenomixJobConf.KMER_LENGTH));
        if(incomingMsg == null)
            incomingMsg = new MessageWritable();
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable();
        else
            outgoingMsg.reset();
        if(repeatKmer == null)
            repeatKmer = new VKmerBytesWritable();
    }
    
    /**
     * check if this tandem repeat can be solved
     */
    public boolean repeatCanBeMerged(){
        tmpValue.setAsCopy(getVertexValue());
        tmpValue.getEdgeList(repeatDir).remove(repeatKmer);
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
