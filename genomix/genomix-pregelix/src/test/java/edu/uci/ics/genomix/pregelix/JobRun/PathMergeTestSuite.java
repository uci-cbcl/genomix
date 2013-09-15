package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class PathMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="PathMerge"; 
        String testSet[] = {"P2_8",
//                "2", "3", "4", "5", "6", "7", "8", "9", "head_6", "head_7",
//                "P2_3", "P2_4", "P2_5", "P2_6", "P2_7", "P2_8",
//                "LeftAdj", "RightAdj",
//                "FR", "RF", "head_FR", "head_RF", "twohead_FR", "twohead_RF",
                "SelfTandemRepeat", "TandemRepeatWithMergeEdge", 
                "TandemRepeatWithUnmergeEdge", "ComplexTandemRepeat",
                "SimplePath", "ThreeDuplicate",
                "SimpleBridgePath", "BridgePathWithTandemRepeat",
                "RingPath", //"CyclePath",
//                "SimpleTreePath", "ComplexTreePath",
//                "Triangle", "Rectangle", 
//                "synthetic",
//                "MultiTandemRepeat", "MultiTandemRepeat2", "MultiTandemRepeat3",
//                "TandemRepeatWithSmallCycle", "TandemRepeatAndCycle"
//                "sameWithEdge"
//                "SmallGenome_5",

        };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
