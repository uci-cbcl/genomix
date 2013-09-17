package edu.uci.ics.genomix.pregelix.type;

public class StatisticsCounter {
    public static final byte Num_MergedNodes = 0b0000 << 0;
    public static final byte Num_MergedPaths = 0b0001 << 0;
    public static final byte Num_TandemRepeats = 0b0010 << 0;
    public static final byte Num_RemovedTips = 0b0011 << 0;
    public static final byte Num_RemovedLowCoverageNodes = 0b0100 << 0;
    public static final byte Num_RemovedBubbles = 0b0101 << 0;
    public static final byte Num_RemovedBridges = 0b0110 << 0;
    public static final byte Num_SplitRepeats = 0b0111 << 0;
    public static final byte Num_Scaffodings = 0b1000 << 0;
    public static final byte Num_Cycles = 0b1001 << 0;
    
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
                case Num_SplitRepeats:
                    r = "num of split repeats";
                    break;
                case Num_Scaffodings:
                    r = "num of scaffoldings";
                    break;
                case Num_Cycles:
                    r = "num of cycles";
                    break;
            }
            return r;
        }
    }
}
