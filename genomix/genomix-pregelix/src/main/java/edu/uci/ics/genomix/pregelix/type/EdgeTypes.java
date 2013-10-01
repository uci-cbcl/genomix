package edu.uci.ics.genomix.pregelix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

public class EdgeTypes implements Writable{

    private byte prevToMeEdgeType;
    private byte nextToMeEdgeType;
    
    public EdgeTypes(){
        prevToMeEdgeType = 0;
        nextToMeEdgeType = 0;
    }
    
    public EDGETYPE getPrevToMeEdgeType() {
        return EDGETYPE.fromByte(prevToMeEdgeType);
    }

    public void setPrevToMeEdgeType(EDGETYPE prevToMeEdgeType) {
        this.prevToMeEdgeType = prevToMeEdgeType.get();
    }

    public EDGETYPE getNextToMeEdgeType() {
        return EDGETYPE.fromByte(nextToMeEdgeType);
    }

    public void setNextToMeEdgeType(EDGETYPE nextToMeEdgeType) {
        this.nextToMeEdgeType = nextToMeEdgeType.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(prevToMeEdgeType);
        out.writeByte(nextToMeEdgeType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        prevToMeEdgeType = in.readByte();
        nextToMeEdgeType = in.readByte();
    }

}
