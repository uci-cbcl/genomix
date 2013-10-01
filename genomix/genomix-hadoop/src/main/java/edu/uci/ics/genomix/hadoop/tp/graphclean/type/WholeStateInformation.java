package edu.uci.ics.genomix.hadoop.tp.graphclean.type;


public class WholeStateInformation {
    public static class State extends VertexStateFlag{            
        public static final byte NO_MERGE = 0b00 << 3;
        public static final byte SHOULD_MERGEWITHNEXT = 0b01 << 3;
        public static final byte SHOULD_MERGEWITHPREV = 0b10 << 3;
        public static final byte SHOULD_MERGE_MASK = 0b11 << 3;
        public static final byte SHOULD_MERGE_CLEAR = 0b1100111;
        
        public static final byte UNCHANGE = 0b0 << 3;
        public static final byte KILL = 0b1 << 3;
        public static final byte KILL_MASK = 0b1 << 3;
        
        public static final byte DIR_FROM_DEADVERTEX = 0b10 << 3;
    }
    
    public static class VertexStateFlag extends FakeFlag {
        public static final byte IS_NON = 0b00 << 5;
        public static final byte IS_RANDOMTAIL = 0b00 << 5;
        public static final byte IS_HEAD = 0b01 << 5;
        public static final byte IS_FINAL = 0b10 << 5;
        public static final byte IS_RANDOMHEAD = 0b11 << 5;
        public static final byte IS_OLDHEAD = 0b11 << 5;

        public static final byte VERTEX_MASK = 0b11 << 5;
        public static final byte VERTEX_CLEAR = (byte) 11001111;
    }
    
    public static class FakeFlag{
        public static final byte IS_NONFAKE = 0 << 0;
        public static final byte IS_FAKE = 1 << 0;
        
        public static final byte FAKEFLAG_MASK = (byte) 00000001;
    }
}
