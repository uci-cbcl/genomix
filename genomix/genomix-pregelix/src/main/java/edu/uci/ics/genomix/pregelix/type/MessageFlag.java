package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag {
    public static final byte FROM_SELF = 0;
    public static final byte IS_HEAD = 1 << 1;
    public static final byte IS_TAIL = 1 << 2;
    public static final byte FROM_DEADVERTEX = 1 << 3;
    //public static final byte FROM_FORWARDLIST = 1 << 4;
    public static final byte FROM_REWARDLIST = 1 << 4;
    public static final byte FROM_SUCCESSOR = 1 << 5;
    public static final byte FROM_PREDECESSOR = 1 << 6;
    
    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        switch (code) {
            case FROM_SELF:
                return "FROM_SELF";
            case IS_HEAD:
                return "IS_HEAD";
            case IS_TAIL:
                return "IS_TAIL";
            case FROM_DEADVERTEX:
                return "FROM_DEADVERTEX";
            case FROM_REWARDLIST:
                return "FROM_REWARDLIST";
            case FROM_SUCCESSOR:
                return "FROM_SUCCESSOR";
            case FROM_PREDECESSOR:
                return "FROM_PREDECESSOR";
        }
        return "ERROR_BAD_MESSAGE";
    }
}
