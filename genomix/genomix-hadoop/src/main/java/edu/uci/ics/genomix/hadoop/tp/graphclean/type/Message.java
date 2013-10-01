package edu.uci.ics.genomix.hadoop.tp.graphclean.type;

public class Message {

    public static final byte NON = 0;
    public static final byte START = 1;
    public static final byte END = 2;
    public static final byte STOP = 3;
    public static final byte FROMPSEUDOHEAD = 4;
    public static final byte FROMPSEUDOREAR = 5;
    public static final byte IN = 6;
    public static final byte OUT = 7;

    public final static class MESSAGE_CONTENT {

        public static String getContentFromCode(byte code) {
            String r = "";
            switch (code) {
                case NON:
                    r = "NON";
                    break;
                case START:
                    r = "START";
                    break;
                case END:
                    r = "END";
                    break;
                case STOP:
                    r = "STOP";
                    break;
                case FROMPSEUDOHEAD:
                    r = "FROMPSEUDOHEAD";
                    break;
                case FROMPSEUDOREAR:
                    r = "FROMPSEUDOREAR";
                    break;
                case IN:
                    r = "IN";
                    break;
                case OUT:
                    r = "OUT";
                    break;
            }
            return r;
        }
    }
}
