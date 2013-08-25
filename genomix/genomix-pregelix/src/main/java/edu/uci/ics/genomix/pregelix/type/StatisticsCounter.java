package edu.uci.ics.genomix.pregelix.type;

public class StatisticsCounter {
    public static final byte Num_MergedNodes = 0b000 << 0;
    public static final byte Num_MergedPaths = 0b001 << 0;
    public static final byte Num_TandemRepeats = 0b010 << 0;
    public static final byte Num_RemovedTips = 0b011 << 0;
    public static final byte Num_RemovedLowCoverageNodes = 0b100 << 0;
    public static final byte Num_RemovedBubbles = 0b101 << 0;
    public static final byte Num_RemovedBridges = 0b110 << 0;
    
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
                case Num_RemovedLowCoverageNodes:
                    r = "num of removed low coverage nodes";
                    break;
                case Num_RemovedBubbles:
                    r = "num of removed bubbles";
                    break;
                case Num_RemovedBridges:
                    r = "num of removed bridges";
                    break;
            }
            return r;
        }
    }
}
