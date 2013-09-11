package edu.uci.ics.genomix.pregelix.io.message;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.type.EdgeListWritable;
import edu.uci.ics.genomix.type.EdgeWritable;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DirectionFlag;

public class PathMergeMessageWritable extends MessageWritable{
    
    private NodeWritable node;
    private boolean isFlip; // use for path merge
    private boolean updateMsg; // use for distinguish updateMsg or mergeMsg

    public PathMergeMessageWritable(){
        super();
        node = new NodeWritable();
        isFlip = false;
        updateMsg = false;
    }
    
    public void setAsCopy(PathMergeMessageWritable other){
        this.node.setAsCopy(other.getNode());
        this.isFlip = other.isFlip();
        this.updateMsg = other.isUpdateMsg();
    }
    
    public void reset(){
        super.reset();
        node.reset();
        isFlip = false;
        updateMsg = false;
    }   
    
    public VKmerBytesWritable getInternalKmer() {
        return node.getInternalKmer();
    }

    public void setInternalKmer(VKmerBytesWritable internalKmer) {
        this.node.setInternalKmer(internalKmer);
    }
    
    public EdgeListWritable getEdgeList(byte dir) {
        return node.getEdgeList((byte) (dir & DirectionFlag.DIR_MASK));
    }
    
    public EdgeWritable getNeighborEdge(){
        for(byte d : DirectionFlag.values){
            if(!getEdgeList(d).isEmpty())
                return getEdgeList(d).get(0);
        }
        return null;
    }

    public void setEdgeList(byte dir, EdgeListWritable edgeList) {
        this.node.setEdgeList((byte) (dir & DirectionFlag.DIR_MASK), edgeList);
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

    public float getAvgCoverage() {
        return this.node.getAvgCoverage();
    }

    public boolean isFlip() {
        return isFlip;
    }

    public void setFlip(boolean isFlip) {
        this.isFlip = isFlip;
    }

    public boolean isUpdateMsg() {
        return updateMsg;
    }

    public void setUpdateMsg(boolean updateMsg) {
        this.updateMsg = updateMsg;
    }
    
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
        isFlip = in.readBoolean();
        updateMsg = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        node.write(out);
        out.writeBoolean(isFlip);
        out.writeBoolean(updateMsg);
    }
    
    @Override
    public String toString(){
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('{');
        sbuilder.append("src:");
        sbuilder.append(getSourceVertexId().toString()).append("\t");
        sbuilder.append("node:");
        sbuilder.append(node.toString()).append("\t");
        sbuilder.append("Flip:").append(isFlip).append("\t");
        sbuilder.append("updateMsg").append(updateMsg);
        sbuilder.append('}');
        return sbuilder.toString();
    }
}
