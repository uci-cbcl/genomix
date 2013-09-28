package edu.uci.ics.genomix.pregelix.type;

public class MessageFlag {
    // 2 bits(0-1) for EDGETYPE, then 2 bits(2-3) for set of DIR's
    // each merge has an edge-type direction (e.g., FF)
    // 2 bits(4-6) for MESSAGETYPE
    public enum MESSAGETYPE{
        
        UPDATE ((byte)(0b001 << 4)),
        TO_NEIGHBOR ((byte)(0b010 << 4)),
        REPLACE_NODE ((byte)(0b011 << 4)),
        KILL_SELF ((byte)(0b100 << 4)),
        FROM_DEAD ((byte)(0b101 << 4));
        
        public static final byte MASK = (byte)(0b111 << 4); 
        public static final byte CLEAR = (byte)(0b0001111); 
        
        private final byte val;
        private MESSAGETYPE(byte val) {
            this.val = val;
        }
        public final byte get() {
            return val;
        }
        
        public static MESSAGETYPE fromByte(short b) {
            b &= MASK;
            if(b == UPDATE.val)
                return UPDATE;
            if(b == TO_NEIGHBOR.val)
                return TO_NEIGHBOR;
            if(b == REPLACE_NODE.val)
                return REPLACE_NODE;
            if(b == KILL_SELF.val)
                return KILL_SELF;
            if(b == FROM_DEAD.val)
                return FROM_DEAD;
            return null;

        }
    }
    
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


