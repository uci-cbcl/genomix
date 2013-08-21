package edu.uci.ics.genomix.pregelix.JobRun;

import java.io.File;

import junit.framework.Test;

public class PathMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="PathMerge";
//        String testSet[] = {"2", "3", "4", "5", "6", "7", "8", "9", "head_6", "head_7",
//                "LeftAdj", "RightAdj",
//                "FR", "RF", "head_FR", "head_RF", "twohead_FR", "twohead_RF",
//                "SelfTandemRepeat", "TandemRepeatWithMergeEdge", 
//                "TandemRepeatWithUnmergeEdge", "ComplexTandemRepeat",
//                "SimplePath", "ThreeDuplicate",
//                "SimpleBridgePath", "BridgePathWithTandemRepeat",
//                "RingPath", "CyclePath",
//                "SimpleTreePath", "ComplexTreePath"
//                };
        String PreFix = "BubbleMerge_Input"; 
        String testSet[] = { PreFix + File.separator
            + "BubbleWithTip"};
//        , PreFix + File.separator
//            + "LtoL", PreFix + File.separator
//            + "LtoR", PreFix + File.separator
//            + "RtoL", PreFix + File.separator
//            + "RtoR"};//, PreFix + File.separator
//            + "InvalidBubble", PreFix + File.separator
//            + "BubbleWithTip", PreFix + File.separator
//            + "SideBubble", PreFix + File.separator
//            + "OverlapBubble", PreFix + File.separator
//            + "Rectangle", PreFix + File.separator
//            + "Grid"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
