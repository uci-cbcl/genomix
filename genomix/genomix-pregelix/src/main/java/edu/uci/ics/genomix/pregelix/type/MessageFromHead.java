package edu.uci.ics.genomix.pregelix.type;

public class MessageFromHead {
    public static final byte BothMsgsFromHead = 1 << 0;
    public static final byte BothMsgsFromNonHead = 1 << 1;
    public static final byte BothMsgsFromOldHead = 1 << 2;
    public static final byte OneMsgFromHead = 1 << 3;
    public static final byte OneMsgFromNonHead = 1 << 4;
    public static final byte OneMsgFromHeadToNonHead = 1 << 5;
    public static final byte OneMsgFromHeadToHead = 1 << 6;
    
    public static final byte NO_INFO = 0 << 0;
}
