package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class SymmetryCheckerTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "SymmetryChecker";
        String testSet[] = { "2" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
