package edu.uci.ics.genomix.pregelix.io.common;

import edu.uci.ics.genomix.type.EDGETYPE;
/**
 * ArrayListWritable of EDGETYPE
 * @author anbangx
 */
public class EdgeTypeList extends ByteArrayListWritable<EDGETYPE> {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    byte getByte(EDGETYPE val) {
        return val.get();
    }

    @Override
    EDGETYPE get(byte val) {
        return EDGETYPE.fromByte(val);
    }

}
