package edu.uci.ics.genomix.pregelix.operator.seeddetection;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import edu.uci.ics.genomix.data.types.EDGETYPE;
import edu.uci.ics.genomix.pregelix.base.MessageWritable;

public class PruneSeedMessage extends MessageWritable {
	private EDGETYPE toPruneEdgeType = null;
	
	protected class FIELDS extends MESSAGE_FIELDS {
        public static final byte TO_PRUNE_EDGETYPE = 1 << 1;    
	}
	
	protected byte getActiveMessageFields() {
	        byte fields = super.getActiveMessageFields();
	        if (toPruneEdgeType != null) {
	            fields |= FIELDS.TO_PRUNE_EDGETYPE;
	        } 
	        return fields;
	}       
	 
	public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if ((messageFields & FIELDS.TO_PRUNE_EDGETYPE) != 0) {
            toPruneEdgeType = EDGETYPE.fromByte(in.readByte());
        }
	}
	
	public void write(DataOutput out) throws IOException {
        super.write(out);
        if (toPruneEdgeType != null) {
            out.writeByte(toPruneEdgeType.get());
        }
	}
	
	public void setToPruneEdgeType(EDGETYPE toPruneEdgeType) {
        this.toPruneEdgeType = toPruneEdgeType;
    }
	
	public EDGETYPE getToPruneEdgeType() {
        return this.toPruneEdgeType;
    }
	
	
}
