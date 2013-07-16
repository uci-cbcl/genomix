package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag extends DirectionFlag {
    
    //public static final byte FLIP = 1 << 6;

    
    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        return "ERROR_BAD_MESSAGE";
    }
}


