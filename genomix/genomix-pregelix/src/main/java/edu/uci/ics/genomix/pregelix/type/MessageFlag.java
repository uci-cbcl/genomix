package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag extends DirFlag {
    
    public static final byte STOP = 0b1 << 2;
    public static final byte STOP_MASK = 0b1 << 2;
    
    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        return "ERROR_BAD_MESSAGE";
    }
}


