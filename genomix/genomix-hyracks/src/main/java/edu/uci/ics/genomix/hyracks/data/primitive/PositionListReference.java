package edu.uci.ics.genomix.hyracks.data.primitive;

import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.hyracks.data.std.api.IValueReference;

public class PositionListReference extends PositionListWritable implements  IValueReference {

	public PositionListReference(int countByDataLength, byte[] byteArray, int startOffset) {
	    super(countByDataLength, byteArray, startOffset);
    }

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
