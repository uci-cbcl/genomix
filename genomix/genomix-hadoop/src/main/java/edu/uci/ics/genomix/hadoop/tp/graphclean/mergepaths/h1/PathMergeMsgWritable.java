package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.EnumSet;

import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.MsgWritable;
import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;;

public class PathMergeMsgWritable extends MsgWritable{
    
    private NodeWritable node;
//    private boolean isFlip; // use for path merge
//    private boolean updateMsg; // use for distinguish updateMsg or mergeMsg

    public PathMergeMsgWritable(){
        super();
        node = new NodeWritable();
//        isFlip = false;
//        updateMsg = false;
    }
    
    public void reset(){
        super.reset();
        node.reset();
//        isFlip = false;
//        updateMsg = false;
    }   
    
    public void setAsCopy(PathMergeMsgWritable other) {
        node.setAsCopy(other.getNode());
        super.setAsCopy(other);
    }
    public VKmerBytesWritable getInternalKmer() {
        return node.getInternalKmer();
    }

    public void setInternalKmer(VKmerBytesWritable internalKmer) {
        this.node.setInternalKmer(internalKmer);
    }
    
    public EdgeListWritable getEdgeList(EDGETYPE edgeType) {
        return node.getEdgeList(edgeType);
    }
    
    public EdgeWritable getNeighborEdge(){
        for(EDGETYPE e : EnumSet.allOf(EDGETYPE.class)){
            if(!getEdgeList(e).isEmpty())
                return getEdgeList(e).get(0);
        }
        return null;
    }

    public void setEdgeList(EDGETYPE edgeType, EdgeListWritable edgeList) {
        this.node.setEdgeList(edgeType, edgeList);
    }
    
    public PositionListWritable getStartReads() {
        return this.node.getStartReads();
    }

    public void setStartReads(PositionListWritable startReads) {
        this.node.setStartReads(startReads);
    }

    public PositionListWritable getEndReads() {
        return this.node.getEndReads();
    }

    public void setEndReads(PositionListWritable endReads) {
        this.node.setEndReads(endReads);
    }
    
    public void setAvgCoverage(float coverage) {
        this.node.setAvgCoverage(coverage);
    }

//    public float getAvgCoverage() {
//        return this.node.getAvgCoverage();
//    }
//
//    public boolean isFlip() {
//        return isFlip;
//    }
//
//    public void setFlip(boolean isFlip) {
//        this.isFlip = isFlip;
//    }
//
//    public boolean isUpdateMsg() {
//        return updateMsg;
//    }
//
//    public void setUpdateMsg(boolean updateMsg) {
//        this.updateMsg = updateMsg;
//    }
    
    public NodeWritable getNode() {
        return node;
    }

    public void setNode(NodeWritable node) {
        this.node.setAsCopy(node);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        super.readFields(in);
        node.readFields(in);
//        isFlip = in.readBoolean();
//        updateMsg = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        node.write(out);
//        out.writeBoolean(isFlip);
//        out.writeBoolean(updateMsg);
    }
}
