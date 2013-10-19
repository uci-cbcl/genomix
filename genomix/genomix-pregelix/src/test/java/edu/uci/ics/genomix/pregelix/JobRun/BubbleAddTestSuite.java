package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class BubbleAddTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "BubbleAdd";
        String testSet[] = { "AdjFR" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
