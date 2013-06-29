package edu.uci.ics.genomix.pregelix.type;

import edu.uci.ics.genomix.type.NodeWritable.MergeDirFlag;

public class MessageFlag extends MergeDirFlag{
    
    public static final byte FLIP = 1 << 3;
    public static final byte IS_HEAD = 1 << 4;
    public static final byte IS_OLDHEAD = 1 << 5;
    public static final byte IS_TAIL = 1 << 5;
    
    public static String getFlagAsString(byte code) {
        // TODO: allow multiple flags to be set
        switch (code) {
            case IS_HEAD:
                return "IS_HEAD";
            case IS_OLDHEAD:
                return "IS_OLDHEAD";
            case FLIP:
                return "FLIP";
        }
        return "ERROR_BAD_MESSAGE";
    }
}
