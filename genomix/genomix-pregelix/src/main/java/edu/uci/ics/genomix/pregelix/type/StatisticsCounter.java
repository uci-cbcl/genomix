package edu.uci.ics.genomix.pregelix.type;

public class StatisticsCounter {
    public static final byte Num_MergedNodes = 0b00 << 0;
    public static final byte Num_MergedPaths = 0b01 << 0;
    public static final byte Num_TandemRepeats = 0b10 << 0;
    public static final byte Num_RemovedTips = 0b11 << 0;
    
    public final static class COUNTER_CONTENT{
        public static String getContent(byte code){
            String r = "";
            switch(code){
                case Num_MergedNodes:
                    r = "num of merged nodes";
                    break;
                case Num_MergedPaths:
                    r = "num of merge paths";
                    break;
                case Num_TandemRepeats:
                    r = "num of tandem repeats";
                    break;
                case Num_RemovedTips:
                    r = "num of removed tips";
                    break;
            }
            return r;
        }
    }
}
