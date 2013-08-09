package edu.uci.ics.genomix.pregelix.type;

public class CheckMessage {

    public static final byte SOURCE = 1 << 0;
    public static final byte INTERNALKMER = 1 << 1;
    public static final byte NEIGHBER = 1 << 2;
    public static final byte PATHLIST = 1 << 3;
    public static final byte NODEIDLIST = 1 << 4;
    public static final byte ADJMSG = 1 << 5;
    public static final byte START = 1 << 6;

    public final static class CheckMessage_CONTENT {

        public static String getContentFromCode(byte code) {
            String r = "";
            switch (code) {
                case SOURCE:
                    r = "SOURCE";
                    break;
                case INTERNALKMER:
                    r = "INTERNALKMER";
                    break;
                case NEIGHBER:
                    r = "NEIGHBER";
                    break;
                case PATHLIST:
                    r = "PATHLIST";
                    break;
                case NODEIDLIST:
                    r = "READID";
                    break;
                case ADJMSG:
                    r = "ADJMSG";
                    break;
                case START:
                    r = "START";
                    break;
            }
            return r;
        }
    }
}
