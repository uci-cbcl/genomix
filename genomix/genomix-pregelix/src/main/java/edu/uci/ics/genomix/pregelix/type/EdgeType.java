package edu.uci.ics.genomix.pregelix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.Node.EDGETYPE;

public class EdgeType implements Writable{
    
    private EDGETYPE edgeType;
    
    public EdgeType(EDGETYPE edgeType){
        this.edgeType = edgeType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(edgeType.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        edgeType = EDGETYPE.fromByte(in.readByte());   
    }

    public byte getEdgeTypeByte(){
        return getEdgeType().get();
    }
    
    public EDGETYPE getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(EDGETYPE edgeType) {
        this.edgeType = edgeType;
    }
}
