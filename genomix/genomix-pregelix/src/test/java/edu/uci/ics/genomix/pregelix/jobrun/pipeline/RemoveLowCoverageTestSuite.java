package edu.uci.ics.genomix.pregelix.jobrun.pipeline;

import edu.uci.ics.genomix.pregelix.jobrun.BasicGraphCleanTestSuite;
import junit.framework.Test;

public class RemoveLowCoverageTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "LOW_COVERAGE";
        String testSet[] = { "MaxCoverage_6" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
