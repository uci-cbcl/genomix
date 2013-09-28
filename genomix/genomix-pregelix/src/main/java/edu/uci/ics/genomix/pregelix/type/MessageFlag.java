package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag {
    // 2 bits(0-1) for EDGETYPE, then 2 bits(2-3) for set of DIR's
    // each merge has an edge-type direction (e.g., FF)
    // 2 bits(4-6) for MESSAGETYPE
    public enum MESSAGETYPE{
        
        UPDATE ((byte)(0b001 << 4)),
        TO_NEIGHBOR ((byte)(0b010 << 4)),
        REPLACE_NODE ((byte)(0b011 << 4)),
        KILL_SELF ((byte)(0b100 << 4));
        
        public static final byte MASK = (byte)(0b111 << 4); 
        public static final byte CLEAR = (byte)(0b0001111); 
        
        private final byte val;
        private MESSAGETYPE(byte val) {
            this.val = val;
        }
        public final byte get() {
            return val;
        }
    }
    
    public static final byte TO_UPDATE = 0b01 << 5;
    public static final byte TO_NEIGHBOR = 0b11 << 5;
    
    public static final byte MSG_MASK = 0b11 << 5; 
    public static final byte MSG_CLEAR = (byte)0011111;
    
    public static final short REPLACE_NODE = 0b01 << 7;
    public static final short KILL_SELF = 0b011 << 7;
    public static final short UPDATE = 0b010 << 7; //reuse 0b0 << 6, becasue UNCHANGE and UPDATE use for different patterns

    public static final short MSG_TYPE_MASK = 0b11 << 7;
    public static final short MSG_TYPE_CLEAR = 0b111111110111111;
    
//    public static final short UNCHANGE = 0b0 << 8;
//    public static final short UPDATE = 0b01 << 7; //reuse 0b0 << 6, becasue UNCHANGE and UPDATE use for different patterns
    public static final short KILL = 0b11 << 8;
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
    
    public static String getFlagAsString(byte code) {
        return "ERROR_BAD_MESSAGE";
    }
}


