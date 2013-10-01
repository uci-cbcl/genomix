package edu.uci.ics.genomix.pregelix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;

public class EdgeTypes implements Writable{

    private byte meToPrevEdgeType;
    private byte meToNextEdgeType;
    
    public EdgeTypes(){
        meToPrevEdgeType = 0;
        meToNextEdgeType = 0;
    }

    public EDGETYPE getMeToPrevEdgeType() {
		return EDGETYPE.fromByte(meToPrevEdgeType);
	}

	public void setMeToPrevEdgeType(EDGETYPE meToPrevEdgeType) {
		this.meToPrevEdgeType = meToPrevEdgeType.get();
	}

	public EDGETYPE getMeToNextEdgeType() {
		return EDGETYPE.fromByte(meToNextEdgeType);
	}

	public void setMeToNextEdgeType(EDGETYPE meToNextEdgeType) {
		this.meToNextEdgeType = meToNextEdgeType.get();
	}

	@Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(meToPrevEdgeType);
        out.writeByte(meToNextEdgeType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        meToPrevEdgeType = in.readByte();
        meToNextEdgeType = in.readByte();
    }

}
