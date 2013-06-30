package edu.uci.ics.genomix.pregelix.type;

public class MessageFromHead {
    public static final byte BothMsgsFromHead = 1 << 0;
    public static final byte BothMsgsFromNonHead = 2 << 0;
    public static final byte BothMsgsFromOldHead = 3 << 0;
    public static final byte OneMsgFromHead = 4 << 1;
    public static final byte OneMsgFromNonHead = 5 << 1;
    
    public static final byte NO_INFO = 0 << 0;
}
