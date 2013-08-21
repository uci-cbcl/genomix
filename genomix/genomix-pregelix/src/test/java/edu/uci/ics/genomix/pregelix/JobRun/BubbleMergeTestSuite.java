package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BubbleMerge";
        String testSet[] = {"BubbleWithTip"};//, "LtoL", "LtoR", "RtoL", "RtoR"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
