package edu.uci.ics.genomix.hadoop.tp.graphclean.type;


public class MessageFlag extends VertexStateFlag {
    
    public static final byte DIR_FF = 0b00 << 0;
    public static final byte DIR_FR = 0b01 << 0;
    public static final byte DIR_RF = 0b10 << 0;
    public static final byte DIR_RR = 0b11 << 0;
    public static final byte DIR_MASK = 0b11 << 0;
    public static final byte DIR_CLEAR = 0b1111100 << 0;
    public static final short REPLACE_DIR_MASK = 0b11 << 0;
    public static final short REPLACE_DIR_CLEAR = 0b11111111111100 << 0;
    
    public static final byte[] values = { DIR_FF, DIR_FR, DIR_RF, DIR_RR };
    
    public static final short UNCHANGE = 0b0 << 7;
    public static final short UPDATE = 0b01 << 7; //reuse 0b0 << 6, becasue UNCHANGE and UPDATE use for different patterns
    public static final short MSG_TYPE_MASK = 0b1 << 7;
    public static final short MSG_TYPE_CLEAR = 0b111111110111111;
    
//    public static final short UNCHANGE = 0b0 << 8;
//    public static final short UPDATE = 0b01 << 7; //reuse 0b0 << 6, becasue UNCHANGE and UPDATE use for different patterns
    public static final short KILL = 0b1 << 8;
    public static final short KILL_MASK = 0b1 << 8;
    
    public static final short UPDATE_MASK = 0b11 << 7;
    public static final short DIR_FROM_DEADVERTEX = 0b1 << 7;
    public static final short DEAD_MASK = 0b1 << 7;
    
    public static final short MERGE_DIR_FF = 0b00 << 9;
    public static final short MERGE_DIR_FR = 0b01 << 9;
    public static final short MERGE_DIR_RF = 0b10 << 9;
    public static final short MERGE_DIR_RR = 0b11 << 9;
    public static final short MERGE_DIR_MASK = 0b11 << 9;
    public static final short MERGE_DIR_CLEAR = 0b11100111111111;
    
    public static final short DELETE_DIR_FF = 0b00 << 11;
    public static final short DELETE_DIR_FR = 0b01 << 11;
    public static final short DELETE_DIR_RF = 0b10 << 11;
    public static final short DELETE_DIR_RR = 0b11 << 11;
    public static final short DELETE_DIR_MASK = 0b11 << 11;
    public static final short DELETE_DIR_CLEAR = 0b10011111111111;
    
    public static String getFlagAsString(byte code) {
        return "ERROR_BAD_MESSAGE";
    }
}


