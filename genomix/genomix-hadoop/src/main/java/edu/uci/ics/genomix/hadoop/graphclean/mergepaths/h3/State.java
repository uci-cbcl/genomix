package edu.uci.ics.genomix.hadoop.graphclean.mergepaths.h3;

public class State {

    public static final byte EMPTY_MESSAGE = 0;
    public static final byte FROM_SELF = 1;
    public static final byte FROM_SUCCESSOR = 2;

    public final static class STATE_CONTENT {

        public static String getContentFromCode(byte code) {
            String r = "ERROR_BAD_MESSAGE";
            switch (code) {
                case EMPTY_MESSAGE:
                    r = "EMPTY_MESSAGE";
                    break;
                case FROM_SELF:
                    r = "FROM_SELF";
                    break;
                case FROM_SUCCESSOR:
                    r = "FROM_SUCCESSOR";
                    break;
            }
            return r;
        }
    }
}
