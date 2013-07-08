package edu.uci.ics.genomix.pregelix.type;

public class MessageFromHead {
    public static final byte BothMsgsFromHead = 0b0000 << 1;
    public static final byte BothMsgsFromNonHead = 0b0001 << 1;
    public static final byte BothMsgsFromOldHead = 0b0010 << 1;
    public static final byte OneMsgFromHead = 0b0011 << 1;
    public static final byte OneMsgFromNonHead = 0b0100 << 1;
    public static final byte OneMsgFromHeadAndOneFromNonHead = 0b0101 << 1;
    public static final byte OneMsgFromHeadToHead = 0b0110 << 1;
    public static final byte OneMsgFromOldHeadToNonHead = 0b0111 << 1;
    public static final byte OneMsgFromOldHeadToHead = 0b1000 << 1;
    public static final byte OneMsgFromOldHeadAndOneFromHead = 0b1001 << 1;
    public static final byte NO_MSG = 0b1010 << 1;
    
    public static final byte NO_INFO = 0 << 0;
}
