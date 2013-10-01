package edu.uci.ics.genomix.hadoop.tp.graphclean.mergepaths.h1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.EnumSet;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


import edu.uci.ics.genomix.hadoop.tp.graphclean.refactortype.GraphCleanNode;
import edu.uci.ics.genomix.type.NodeWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.NodeWritable.DIR;
import edu.uci.ics.genomix.type.NodeWritable.EDGETYPE;;

public class PathMergeNode extends GraphCleanNode {
    
    /**
     * 
     */
    public static class P4State {
        // 2 bits for DirectionFlag, then 2 bits for set of DIR's
        // each merge has an edge-type direction (e.g., FF)
        public static final byte NO_MERGE = 0b0 << 4;
        public static final byte MERGE = 0b1 << 4;
        public static final byte MERGE_CLEAR = 0b1101100; // clear the MERGE/NO_MERGE and the MERGE_DIRECTION
        public static final byte MERGE_MASK = 0b0010011;
    }
    
    private static final long serialVersionUID = 1L;
    protected byte state;
    
    
    public PathMergeNode() {
        this.state = 0b00 << 0;
    }
    
    public void setState(byte state) {
        this.state = state;
    }
    
    public byte getState() {
        return this.state;
    }
    
    public void setMerge(byte mergeState){
        short state = getState();
        state &= P4State.MERGE_CLEAR;
        state |= (mergeState & P4State.MERGE_MASK);
    }
    
}
