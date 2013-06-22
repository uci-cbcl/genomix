package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag {
    public static final byte EMPTY_MESSAGE = 0;
    public static final byte FROM_SELF = 1;
    public static final byte FROM_SUCCESSOR = 1 << 1;
    public static final byte FROM_PREDECESSOR = 1 << 2;
    public static final byte IS_HEAD = 1 << 3;
    public static final byte IS_TAIL = 1 << 4;

    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        switch (code) {
            case EMPTY_MESSAGE:
                return "EMPTY_MESSAGE";
            case FROM_SELF:
                return "FROM_SELF";
            case FROM_SUCCESSOR:
                return "FROM_SUCCESSOR";
            case FROM_PREDECESSOR:
                return "FROM_PREDECESSOR";
            case IS_HEAD:
                return "IS_HEAD";
            case IS_TAIL:
                return "IS_TAIL";
        }
        return "ERROR_BAD_MESSAGE";
    }
}
