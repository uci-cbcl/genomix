package edu.uci.ics.genomix.pregelix.type;

import edu.uci.ics.genomix.type.NodeWritable.MergeDirFlag;

public class MessageFlag extends MergeDirFlag {
    
    //public static final byte FLIP = 1 << 6;

    
    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        return "ERROR_BAD_MESSAGE";
    }
}
