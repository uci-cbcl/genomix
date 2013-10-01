package edu.uci.ics.genomix.hadoop.tp.graphclean.type;


public  class VertexStateFlag {
    public static final byte TO_UPDATE = 0b01 << 5;
    public static final byte TO_OTHER = 0b10 << 5;
    public static final byte TO_NEIGHBOR = 0b11 << 5;
    public static final byte MSG_MASK = 0b11 << 5; 
    public static final byte MSG_CLEAR = (byte)0011111;
    
    //TODO clean up code
    public static final byte IS_NON = 0b000 << 4;
    public static final byte IS_HEAD = 0b001 << 4;
    public static final byte IS_FINAL = 0b010 << 4;
    
    public static final byte IS_OLDHEAD = 0b011 << 4;
    
    public static final byte IS_HALT = 0b100 << 4;
    public static final byte IS_DEAD = 0b101 << 4;
    public static final byte IS_ERROR = 0b110 << 4;
    
    public static final byte VERTEX_MASK = 0b111 << 4; 
    public static final byte VERTEX_CLEAR = (byte)0001111;
    
    public static String getContent(short stateFlag){
        switch(stateFlag & VERTEX_MASK){
            case IS_NON:
                return "IS_NON";
            case IS_HEAD:
                return "IS_HEAD";
            case IS_FINAL:
                return "IS_FINAL";
            case IS_OLDHEAD:
                return "IS_OLDHEAD";
            case IS_HALT:
                return "IS_HALT";
            case IS_DEAD:
                return "IS_DEAD";
            case IS_ERROR:
                return "IS_ERROR";
                
        }
        return null;
    }
}
