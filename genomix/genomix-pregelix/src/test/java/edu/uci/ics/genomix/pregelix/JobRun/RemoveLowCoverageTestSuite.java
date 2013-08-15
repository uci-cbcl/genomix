package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class RemoveLowCoverageTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="RemoveLowCoverage";
        String testSet[] = {"MaxCoverage_6"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
