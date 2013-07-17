package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag extends DirectionFlag {
    
    //public static final byte FLIP = 1 << 6;
    public static final byte HEAD_SHOULD_MERGEWITHPREV = 0b101 << 0;
    public static final byte HEAD_SHOULD_MERGEWITHNEXT = 0b111 << 0;

    
    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        return "ERROR_BAD_MESSAGE";
    }
}


