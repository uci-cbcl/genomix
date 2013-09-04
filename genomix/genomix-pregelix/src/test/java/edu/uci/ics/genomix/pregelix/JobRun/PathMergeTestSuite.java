package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class PathMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="PathMerge";
        String testSet[] = {"P2_5"};
//                "2", "3", "4", "5", "6", "7", "8", "9", "head_6", "head_7",
//                "LeftAdj", "RightAdj",
//                "FR", "RF", "head_FR", "head_RF", "twohead_FR", "twohead_RF",
//                "SelfTandemRepeat", "TandemRepeatWithMergeEdge", 
//                "TandemRepeatWithUnmergeEdge", "ComplexTandemRepeat",
//            {      "SimplePath", "ThreeDuplicate",
//                "SimpleBridgePath", "BridgePathWithTandemRepeat",
//                "RingPath", "CyclePath",
//                "SimpleTreePath", "ComplexTreePath",
//                "Triangle", "Rectangle", 
//                "synthetic",
//                "SmallGenome"
//        };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
