package edu.uci.ics.genomix.pregelix.type;

public class AdjMessage {
    public static final byte FROMFF = 0;
    public static final byte FROMFR = 1;
    public static final byte FROMRF = 2;
    public static final byte FROMRR = 3;
    public static final byte NON = 4;
    public static final byte UNCHANGE = 5;
    public static final byte MERGE = 6;
    public static final byte KILL = 7;
    
    public final static class ADJMESSAGE_CONTENT {
        public static String getContentFromCode(byte code) {
            String r = "";
            switch (code) {
                case FROMFF:
                    r = "FROMFF";
                    break;
                case FROMFR:
                    r = "FROMFR";
                    break;
                case FROMRF:
                    r = "FROMRF";
                    break;
                case FROMRR:
                    r = "FROMRR";
                    break;
                case NON:
                    r = "NON";
                    break;
                case UNCHANGE:
                    r = "UNCHANGE";
                    break;
                case MERGE:
                    r = "MERGE";
                    break;
                case KILL:
                    r = "KILL";
                    break;
            }
            return r;
        }
    }
}
