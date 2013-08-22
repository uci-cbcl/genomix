package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BubbleMergeTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="BubbleMerge";
        String testSet[] = {"SimpleRectangle", "MediumRectangle", "ComplexRectangle"};//{"SimpleRectangle", "MediumRectangle", "ComplexRectangle"};//, };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
