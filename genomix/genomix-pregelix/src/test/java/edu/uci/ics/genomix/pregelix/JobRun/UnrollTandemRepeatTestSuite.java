package edu.uci.ics.genomix.pregelix.JobRun;

import junit.framework.Test;

public class UnrollTandemRepeatTestSuite extends BasicGraphCleanTestSuite{

    public static Test suite() throws Exception {
        String pattern ="UnrollTandemRepeat";
        String testSet[] = {"TandemRepeat", "BridgePathWithTandemRepeat"};
        init(pattern, testSet);
        BasicGraphCleanTestSuite testSuite = new BasicGraphCleanTestSuite();
        return makeTestSuite(testSuite);
    }
}
