package edu.uci.ics.genomix.pregelix.type;

import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.VertexStateFlag;

public class MessageFlag extends VertexStateFlag {
    
    public static final byte DIR_FF = 0b00 << 0;
    public static final byte DIR_FR = 0b01 << 0;
    public static final byte DIR_RF = 0b10 << 0;
    public static final byte DIR_RR = 0b11 << 0;
    public static final byte DIR_MASK = 0b11 << 0;
    public static final byte DIR_CLEAR = 0b1111100 << 0;
    
    public static final byte[] values = { DIR_FF, DIR_FR, DIR_RF, DIR_RR };
    
    public static final byte UNCHANGE = 0b0 << 6;
    public static final byte UPDATE = 0b0 << 6; //reuse 0b0 << 6, becasue UNCHANGE and UPDATE use for different patterns
    public static final byte KILL = 0b1 << 6;
    public static final byte KILL_MASK = 0b1 << 6;
    
    public static final byte DIR_FROM_DEADVERTEX = 0b1 << 5;
    public static final byte DEAD_MASK = 0b1 << 5;
    
    public static String getFlagAsString(byte code) {
        return "ERROR_BAD_MESSAGE";
    }
}


