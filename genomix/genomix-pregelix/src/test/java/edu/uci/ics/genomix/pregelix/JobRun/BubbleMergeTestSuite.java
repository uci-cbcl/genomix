package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BubbleMerge";
        String testSet[] = { "SimpleBubble", "LtoL", "LtoR", "RtoL", "RtoR", "FRandRF", "InvalidBubble",
                "BubbleWithTip", "SideBubble", "OverlapBubble", "SimpleRectangle", "MediumRectangle",
                "ComplexRectangle", "ThreeSquares", "Grid", "TwoSimilarInTwo", "TwoSimilarInThree", "TwoSimilarInFour",
                "ThreeSimilarInFour", "FourSimilarInFour" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
