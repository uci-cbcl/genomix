package edu.uci.ics.genomix.pregelix.JobRun.main;

import edu.uci.ics.genomix.pregelix.JobRun.BasicGraphCleanTestSuite;
import junit.framework.Test;

public class RemoveLowCoverageTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "RemoveLowCoverage";
        String testSet[] = { "MaxCoverage_2" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
