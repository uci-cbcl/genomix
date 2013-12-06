package edu.uci.ics.genomix.pregelix.operator.complexbubblemerge;

import edu.uci.ics.genomix.data.types.EDGETYPE;
/**
 * ArrayListWritable of EDGETYPE
 * @author anbangx
 */
public class EdgeTypeList extends MashToByte<EDGETYPE> {
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
