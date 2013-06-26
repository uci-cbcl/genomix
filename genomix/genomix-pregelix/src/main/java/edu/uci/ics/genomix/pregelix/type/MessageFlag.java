package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag {
    public static final byte FLIP = 0;
    public static final byte IS_HEAD = 1 << 1;
    public static final byte IS_TAIL = 1 << 2;
    public static final byte SHOULD_MERGEWITHNEXT = 1 << 3;
    public static final byte SHOULD_MERGEWITHPREV = 1 << 4;
    public static final byte FROM_SUCCESSOR = 1 << 5;
    public static final byte FROM_PREDECESSOR = 1 << 6;
    
    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        switch (code) {
            case IS_HEAD:
                return "IS_HEAD";
            case IS_TAIL:
                return "IS_TAIL";
            case SHOULD_MERGEWITHNEXT:
                return "SHOULD_MERGEWITHNEXT";
            case SHOULD_MERGEWITHPREV:
                return "SHOULD_MERGEWITHPREV";
            case FLIP:
                return "FLIP";
            case FROM_SUCCESSOR:
                return "FROM_SUCCESSOR";
            case FROM_PREDECESSOR:
                return "FROM_PREDECESSOR";
        }
        return "ERROR_BAD_MESSAGE";
    }
}
