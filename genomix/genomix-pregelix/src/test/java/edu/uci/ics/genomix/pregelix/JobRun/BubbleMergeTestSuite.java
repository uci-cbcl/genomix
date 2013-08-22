package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BubbleMerge";
        String testSet[] = {"OverlapBubble"}; 
//            {"SimpleBubble", "LtoL", "LtoR", "RtoL", "RtoR",
//                "InvalidBubble", "BubbleWithTip",
//                "SimpleRectangle", "MediumRectangle", "ComplexRectangle"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
