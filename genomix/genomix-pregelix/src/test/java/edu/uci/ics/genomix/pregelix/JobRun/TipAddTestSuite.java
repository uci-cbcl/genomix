package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class TipAddTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="TipAdd";
        String testSet[] = {"5"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
