package edu.uci.ics.genomix.pregelix.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class EdgeTypes implements Writable{

    private byte meToPrevEdgeType;
    private byte meToNextEdgeType;
    
    public EdgeTypes(){
        meToPrevEdgeType = 0;
        meToNextEdgeType = 0;
    }

    public byte getMeToPrevEdgeType() {
		return meToPrevEdgeType;
	}

	public void setMeToPrevEdgeType(byte meToPrevEdgeType) {
		this.meToPrevEdgeType = meToPrevEdgeType;
	}

	public byte getMeToNextEdgeType() {
		return meToNextEdgeType;
	}

	public void setMeToNextEdgeType(byte meToNextEdgeType) {
		this.meToNextEdgeType = meToNextEdgeType;
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
