package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BubbleMerge";
        String testSet[] = {"LtoL"};//{"SimpleBubble", "LtoL", "LtoR", "RtoL", "RtoR"};//, "MediumRectangle", "ComplexRectangle"};//, "LtoL", "LtoR", "RtoL", "RtoR"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
