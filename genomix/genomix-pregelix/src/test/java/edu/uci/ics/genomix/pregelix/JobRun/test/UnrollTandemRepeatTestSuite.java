package edu.uci.ics.genomix.pregelix.JobRun.test;

import junit.framework.Test;
import edu.uci.ics.genomix.pregelix.JobRun.BasicGraphCleanTestSuite;

public class UnrollTandemRepeatTestSuite extends BasicGraphCleanTestSuite {

    public static Test suite() throws Exception {
        String pattern = "UnrollTandemRepeat";
        String testSet[] = { "SelfTandemRepeat" };
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
